"""Persistent multi-scope kill switch (circuit breaker) for Global Action Outbox."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from tortoise.expressions import Q
from tortoise.transactions import in_transaction

from app.rsi.guardrails.time_window import to_utc
from app.rsi.models import RsiKillSwitch


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _db(queryset, connection):
    return queryset.using_db(connection) if connection is not None else queryset


async def set_kill_switch(
    tenant_id: str,
    scope_type: str,
    target_id: str,
    paused: bool,
    *,
    reason: Optional[str] = None,
    paused_until: Optional[datetime] = None,
) -> RsiKillSwitch:
    """Persistently pause or resume global, tenant, channel, or policy dispatch."""
    if scope_type not in ("global", "tenant", "channel", "policy"):
        raise ValueError(f"Unsupported kill switch scope_type: {scope_type}")
    norm_tenant = "default" if scope_type == "global" else (tenant_id.strip() or "default")
    norm_target = "*" if scope_type == "global" else (target_id.strip() or norm_tenant)
    until_utc = to_utc(paused_until) if (paused and paused_until is not None) else None

    async with in_transaction() as conn:
        existing = await (
            RsiKillSwitch.filter(
                tenant_id=norm_tenant,
                scope_type=scope_type,
                target_id=norm_target,
            )
            .using_db(conn)
            .select_for_update()
            .first()
        )
        if existing is not None:
            existing.paused = bool(paused)
            existing.reason = reason if paused else None
            existing.paused_until = until_utc
            await existing.save(using_db=conn)
            return existing

        return await RsiKillSwitch.create(
            using_db=conn,
            tenant_id=norm_tenant,
            scope_type=scope_type,
            target_id=norm_target,
            paused=bool(paused),
            reason=reason if paused else None,
            paused_until=until_utc,
        )


async def is_killed(
    tenant_id: str,
    *,
    channel: Optional[str] = None,
    policy_id: Optional[str] = None,
    now: Optional[datetime] = None,
    connection=None,
) -> Tuple[bool, Optional[str]]:
    """Evaluate active global/tenant/channel/policy kill switches."""
    check_now = to_utc(now) if now is not None else _utc_now()
    cond = Q(tenant_id="default", scope_type="global", target_id="*") | Q(
        tenant_id=tenant_id, scope_type="tenant", target_id=tenant_id
    )
    if channel:
        cond = cond | Q(tenant_id=tenant_id, scope_type="channel", target_id=channel)
    if policy_id:
        cond = cond | Q(tenant_id=tenant_id, scope_type="policy", target_id=policy_id)

    switches = await _db(RsiKillSwitch.filter(cond, paused=True), connection).all()
    if not switches:
        return False, None

    priority_order = {"global": 0, "tenant": 1, "channel": 2, "policy": 3}
    for sw in sorted(switches, key=lambda s: priority_order.get(s.scope_type, 99)):
        if sw.paused_until is None or to_utc(sw.paused_until) > check_now:
            return True, f"kill_switch:{sw.scope_type}:{sw.target_id}"
    return False, None


async def list_kill_switches(tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """List current kill-switch states for admin inspection."""
    qs = RsiKillSwitch.filter(paused=True)
    if tenant_id:
        qs = qs.filter(Q(tenant_id=tenant_id) | Q(scope_type="global"))
    rows = await qs.order_by("scope_type", "target_id").all()
    now = _utc_now()
    active_rows = [
        r for r in rows if r.paused_until is None or to_utc(r.paused_until) > now
    ]
    return [
        {
            "tenant_id": r.tenant_id,
            "scope_type": r.scope_type,
            "target_id": r.target_id,
            "paused": r.paused,
            "reason": r.reason,
            "paused_until": r.paused_until.isoformat() if r.paused_until else None,
        }
        for r in active_rows
    ]
