"""Global Action Outbox hard compliance guardrail pipeline (sole outbound side-effect gate)."""
from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from tortoise.transactions import in_transaction

from app.rsi.adapters import default_adapter_registry
from app.rsi.adapters.base import AdapterRegistry, record_action_result
from app.rsi.guardrails.kill_switch import is_killed
from app.rsi.guardrails.time_window import (
    is_within_working_window,
    next_working_window_opening,
    resolve_working_window,
    snap_to_working_window,
    to_utc,
)
from app.rsi.identity import resolve_canonical_entity
from app.rsi.models import (
    ActionOutbox,
    EntityMemoryState,
    FollowupEntity,
    FollowupPolicy,
)
from app.rsi.schemas import (
    ActionProposal,
    GuardrailVerdict,
    OutboxSubmitResponse,
    RSIEventEnvelope,
)

logger = logging.getLogger(__name__)

DEFAULT_OUTBOUND_FORBIDDEN_WORDS: Tuple[str, ...] = (
    "保证放款",
    "百分百通过",
    "免征信",
    "内部关系",
    "绝对低息",
    "稳过",
    "骂人",
    "骗子",
)

_PERM_RANK: Dict[str, int] = {"L0": 0, "L1": 1, "L2": 2, "L3": 3}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _db(queryset, connection):
    return queryset.using_db(connection) if connection is not None else queryset


def _parse_iso_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return to_utc(value)
    if isinstance(value, str) and value.strip():
        try:
            return to_utc(datetime.fromisoformat(value.strip()))
        except ValueError:
            return None
    return None


def scan_outbound_forbidden_word(
    payload: Dict[str, Any],
    extra_words: Optional[List[str]] = None,
) -> Optional[str]:
    """Scan outbound message/script text fields against compliance blacklist."""
    chunks: List[str] = []
    for key in ("text", "message", "opening_script", "sms_text", "content", "reply_text"):
        val = payload.get(key)
        if isinstance(val, str) and val.strip():
            chunks.append(val.strip())
    if not chunks:
        return None
    combined = "\n".join(chunks).lower()

    words: List[str] = list(DEFAULT_OUTBOUND_FORBIDDEN_WORDS)
    if extra_words:
        for w in extra_words:
            if isinstance(w, str) and w.strip() and w.strip() not in words:
                words.append(w.strip())

    for word in words:
        if word.lower() in combined:
            return word
    return None


def resolve_permission_level(
    proposal: ActionProposal,
    policy_config: Optional[Dict[str, Any]] = None,
) -> str:
    """Resolve effective permission tier (policy overrides upward; agent cannot downgrade)."""
    cfg = policy_config if isinstance(policy_config, dict) else {}
    perms_map = cfg.get("action_permissions") if isinstance(cfg.get("action_permissions"), dict) else {}
    policy_perm = perms_map.get(proposal.action_type)
    if policy_perm is None and proposal.action_type in (cfg.get("approval_required_actions") or []):
        policy_perm = "L2"

    prop_perm = proposal.permission_level or "L0"
    if policy_perm in _PERM_RANK and _PERM_RANK[policy_perm] > _PERM_RANK.get(prop_perm, 0):
        return str(policy_perm)
    return str(prop_perm)


def resolve_adapter_name(
    proposal: ActionProposal,
    policy_config: Optional[Dict[str, Any]] = None,
) -> str:
    if proposal.adapter_name and proposal.adapter_name.strip():
        return proposal.adapter_name.strip()
    cfg = policy_config if isinstance(policy_config, dict) else {}
    channel_adapters = cfg.get("channel_adapters") if isinstance(cfg.get("channel_adapters"), dict) else {}
    if proposal.channel in channel_adapters:
        return str(channel_adapters[proposal.channel])
    if proposal.channel == "voice":
        return "voice"
    if proposal.channel == "webhook":
        return "webhook"
    return "crm_passive"


async def evaluate_outbox_guardrails(
    proposal: ActionProposal,
    entity: FollowupEntity,
    state: Optional[EntityMemoryState],
    policy_config: Optional[Dict[str, Any]] = None,
    *,
    now: Optional[datetime] = None,
    bypass_approval: bool = False,
    exclude_outbox_id: Optional[int] = None,
    connection=None,
) -> GuardrailVerdict:
    """Evaluate 6 deterministic guardrail stages before allowing any outbound action."""
    check_now = to_utc(now) if now is not None else _utc_now()
    cfg = dict(policy_config or {})
    hb = dict(state.hard_boundaries or {}) if state and isinstance(state.hard_boundaries, dict) else {}
    lc = dict(state.lifecycle or {}) if state and isinstance(state.lifecycle, dict) else {}
    effective_policy_id = proposal.policy_id or entity.policy_id

    # Stage 1: Kill Switch / Circuit Breaker
    killed, kill_rule = await is_killed(
        entity.tenant_id,
        channel=proposal.channel,
        policy_id=effective_policy_id,
        now=check_now,
        connection=connection,
    )
    if killed:
        return GuardrailVerdict(
            decision="deferred",
            rule_id=kill_rule or "kill_switch",
            reason="Dispatch paused by persistent kill switch",
            deferred_until=check_now + timedelta(minutes=5),
        )

    # Stage 2: Hard Entity Boundaries
    if entity.status in ("blacklisted", "closed", "merged") or hb.get("blacklisted") or hb.get("erasure_requested"):
        return GuardrailVerdict(
            decision="blocked",
            rule_id="boundary:blacklisted_or_closed",
            reason=str(hb.get("blacklist_reason") or entity.status),
        )

    if hb.get("dnd"):
        return GuardrailVerdict(
            decision="blocked",
            rule_id="boundary:dnd",
            reason="Entity requested Do-Not-Disturb",
        )

    if (entity.status == "converted" or lc.get("converted") is True) and not bool(
        cfg.get("allow_post_conversion_followup", False)
    ):
        return GuardrailVerdict(
            decision="blocked",
            rule_id="boundary:converted",
            reason="Entity already converted",
        )

    takeover_until = _parse_iso_dt(hb.get("human_takeover_until"))
    if takeover_until is not None and takeover_until > check_now:
        return GuardrailVerdict(
            decision="deferred",
            rule_id="boundary:human_takeover_active",
            reason="Deferred while human rep takeover lock is active",
            deferred_until=takeover_until,
        )

    # Stage 3: Outbound Forbidden Words
    hit_word = scan_outbound_forbidden_word(
        proposal.payload or {},
        extra_words=cfg.get("outbound_forbidden_words"),
    )
    if hit_word:
        return GuardrailVerdict(
            decision="blocked",
            rule_id=f"outbound_forbidden_word:{hit_word}",
            reason=f"Outbound payload matched forbidden word '{hit_word}'",
        )

    # Stage 4: Permission Tier (L0-L3)
    perm_level = resolve_permission_level(proposal, cfg)
    if perm_level == "L3":
        return GuardrailVerdict(
            decision="blocked",
            rule_id="permission:L3_forbidden",
            reason=f"Action '{proposal.action_type}' is forbidden by L3 policy",
        )
    if perm_level == "L2" and not bypass_approval:
        return GuardrailVerdict(
            decision="awaiting_approval",
            rule_id="permission:L2_approval_required",
            reason=f"Action '{proposal.action_type}' requires human approval",
        )

    # Stage 5: Working Window / Quiet Hours
    work_start, work_end, tz = resolve_working_window(cfg)
    enforced_channels = cfg.get("window_enforced_channels", ["voice", "sms"])
    enforce_all = bool(cfg.get("enforce_window_all_channels", False))
    if enforce_all or proposal.channel in enforced_channels:
        target_dt = to_utc(proposal.scheduled_at) if proposal.scheduled_at else check_now
        if not is_within_working_window(target_dt, work_start, work_end, tz):
            buffer_mins = int(cfg.get("morning_buffer_minutes", 30))
            snapped = snap_to_working_window(
                target_dt, work_start, work_end, tz, morning_buffer_minutes=buffer_mins
            )
            return GuardrailVerdict(
                decision="deferred",
                rule_id="working_window:outside_hours",
                reason=f"Snapped outside working window ({work_start}-{work_end})",
                deferred_until=snapped,
            )

    # Stage 6: Frequency Caps, Cooldown & In-Flight Dedup
    qs = _db(
        ActionOutbox.filter(tenant_id=entity.tenant_id, entity_id=entity.entity_id),
        connection,
    )
    if exclude_outbox_id is not None:
        qs = qs.exclude(id=exclude_outbox_id)
    recent_actions = await qs.filter(
        status__in=["scheduled", "dispatching", "succeeded"]
    ).order_by("-id").limit(200).all()

    dedup_mins = int(cfg.get("dedup_window_minutes", 5))
    if dedup_mins > 0:
        dedup_low = check_now - timedelta(minutes=dedup_mins)
        dedup_high = check_now + timedelta(minutes=dedup_mins)
        for row in recent_actions:
            if (
                row.status in ("scheduled", "dispatching")
                and row.channel == proposal.channel
                and row.action_type == proposal.action_type
                and row.scheduled_at is not None
                and dedup_low <= to_utc(row.scheduled_at) <= dedup_high
            ):
                return GuardrailVerdict(
                    decision="blocked",
                    rule_id="dedup:inflight_duplicate",
                    reason="Duplicate in-flight action within dedup window",
                )

    succeeded_rows = [
        r for r in recent_actions if r.status == "succeeded" and r.dispatched_at is not None
    ]

    max_touches_7d = int(cfg.get("max_touches_7d", 15))
    if max_touches_7d > 0:
        cutoff_7d = check_now - timedelta(days=7)
        touches_7d = sum(1 for r in succeeded_rows if to_utc(r.dispatched_at) >= cutoff_7d)
        if touches_7d >= max_touches_7d:
            return GuardrailVerdict(
                decision="blocked",
                rule_id="freq_cap:cross_channel_7d",
                reason=f"7d touches {touches_7d} reached cap {max_touches_7d}",
            )

    cutoff_24h = check_now - timedelta(hours=24)
    touches_24h = [r for r in succeeded_rows if to_utc(r.dispatched_at) >= cutoff_24h]
    max_touches_24h = int(cfg.get("max_touches_24h", 5))
    if max_touches_24h > 0 and len(touches_24h) >= max_touches_24h:
        next_open = next_working_window_opening(
            check_now, work_start, work_end, tz, force_future=True
        )
        return GuardrailVerdict(
            decision="deferred",
            rule_id="freq_cap:cross_channel_24h",
            reason=f"24h cross-channel touches {len(touches_24h)} reached cap {max_touches_24h}",
            deferred_until=next_open,
        )

    channel_caps = cfg.get("channel_caps_24h") if isinstance(cfg.get("channel_caps_24h"), dict) else {}
    ch_cap = channel_caps.get(proposal.channel)
    if isinstance(ch_cap, int) and ch_cap > 0:
        ch_count = sum(1 for r in touches_24h if r.channel == proposal.channel)
        if ch_count >= ch_cap:
            next_open = next_working_window_opening(
                check_now, work_start, work_end, tz, force_future=True
            )
            return GuardrailVerdict(
                decision="deferred",
                rule_id=f"freq_cap:channel_24h:{proposal.channel}",
                reason=f"24h channel '{proposal.channel}' touches {ch_count} reached cap {ch_cap}",
                deferred_until=next_open,
            )

    min_cooldown_mins = int(cfg.get("min_cooldown_minutes", 0))
    if min_cooldown_mins > 0 and succeeded_rows:
        latest_disp = max(to_utc(r.dispatched_at) for r in succeeded_rows)
        cooldown_until = latest_disp + timedelta(minutes=min_cooldown_mins)
        if cooldown_until > check_now:
            return GuardrailVerdict(
                decision="deferred",
                rule_id="freq_cap:cooldown",
                reason="Cooling down between outbound touches",
                deferred_until=cooldown_until,
            )

    return GuardrailVerdict(decision="allowed", rule_id="guardrail:passed")


async def _load_entity_bundle(
    tenant_id: str,
    entity_id: str,
    policy_id_override: Optional[str] = None,
    *,
    connection=None,
) -> Tuple[FollowupEntity, Optional[EntityMemoryState], Dict[str, Any], str]:
    entity = await resolve_canonical_entity(tenant_id, entity_id, connection=connection)
    if entity is None:
        raise ValueError(f"Entity '{entity_id}' not found in tenant '{tenant_id}'")

    state = await _db(
        EntityMemoryState.filter(tenant_id=tenant_id, entity_id=entity.entity_id),
        connection,
    ).first()
    eff_policy_id = policy_id_override or entity.policy_id or "default"
    policy = await _db(
        FollowupPolicy.filter(tenant_id=tenant_id, policy_id=eff_policy_id, is_active=True),
        connection,
    ).first()
    policy_cfg = dict(policy.config or {}) if policy and isinstance(policy.config, dict) else {}
    policy_ver = policy.version if policy else "1.0"
    return entity, state, policy_cfg, policy_ver


async def execute_outbox_record(
    outbox: ActionOutbox,
    entity: FollowupEntity,
    policy_config: Dict[str, Any],
    *,
    verdict_rule: str,
    adapter_registry: Optional[AdapterRegistry] = None,
    now: Optional[datetime] = None,
    emit_receipt: bool = True,
) -> OutboxSubmitResponse:
    """Dispatch an approved ActionOutbox row via edge ChannelAdapter and persist receipt."""
    exec_now = to_utc(now) if now is not None else _utc_now()
    registry = adapter_registry or default_adapter_registry
    adapter = registry.resolve(outbox.adapter_name, outbox.channel)

    outbox.status = "dispatching"
    await outbox.save(update_fields=["status", "update_time"])

    entity_snapshot = {
        "tenant_id": entity.tenant_id,
        "entity_id": entity.entity_id,
        "status": entity.status,
        "policy_id": entity.policy_id,
        "policy_config": policy_config,
    }
    res = await adapter.execute(outbox, entity_snapshot)

    if res.status in ("succeeded", "pending_crm"):
        outbox.status = "succeeded"
        outbox.dispatched_at = exec_now
        outbox.finished_at = exec_now
        outbox.last_error = None
    elif res.status == "retry" and outbox.retry_count < outbox.max_retries:
        outbox.retry_count += 1
        outbox.status = "deferred"
        outbox.scheduled_at = exec_now + timedelta(seconds=min(300, 2 ** outbox.retry_count))
        outbox.last_error = res.error
    else:
        outbox.status = "failed"
        outbox.finished_at = exec_now
        outbox.last_error = res.error or f"adapter_{res.status}"
    await outbox.save()

    result_row = await record_action_result(
        action=outbox,
        verdict="allowed",
        verdict_rule=verdict_rule,
        execution_status=res.status,
        provider_ref=res.provider_ref,
        adapter_response=res.response,
        latency_ms=res.latency_ms,
    )

    if emit_receipt:
        try:
            from app.rsi.memory_reducer import ingest_event

            await ingest_event(
                RSIEventEnvelope(
                    event_id=f"rcpt_outbox_{outbox.id}_{result_row.id}",
                    tenant_id=outbox.tenant_id,
                    event_type=f"adapter.{res.status}",
                    category="action_receipt",
                    entity_id=entity.entity_id,
                    occurred_at=exec_now,
                    source="rsi_outbox",
                    actor_type="system",
                    correlation_id=outbox.correlation_id,
                    causation_id=f"outbox:{outbox.id}",
                    facts={
                        "outbox_id": outbox.id,
                        "result_id": result_row.id,
                        "channel": outbox.channel,
                        "adapter_name": outbox.adapter_name,
                        "action_type": outbox.action_type,
                        "execution_status": res.status,
                        "provider_ref": res.provider_ref,
                    },
                )
            )
        except Exception as err:
            logger.warning("[RSI-Outbox] Receipt ingest skipped outbox_id=%s: %s", outbox.id, err)

    return OutboxSubmitResponse(
        outbox_id=outbox.id,
        tenant_id=outbox.tenant_id,
        entity_id=outbox.entity_id,
        idempotency_key=outbox.idempotency_key,
        duplicate=False,
        status=outbox.status,
        verdict="allowed",
        verdict_rule=outbox.verdict_rule,
        verdict_reason=outbox.verdict_reason,
        scheduled_at=outbox.scheduled_at,
        execution_result_id=result_row.id,
        execution_status=result_row.execution_status,
        provider_ref=result_row.provider_ref,
    )


async def submit_action_proposal(
    proposal: ActionProposal,
    *,
    adapter_registry: Optional[AdapterRegistry] = None,
    now: Optional[datetime] = None,
    dispatch_immediately: bool = True,
    emit_receipt: bool = True,
) -> OutboxSubmitResponse:
    """Submit an outbound proposal through Global Outbox guardrails and edge adapters."""
    check_now = to_utc(now) if now is not None else _utc_now()
    entity, state, policy_cfg, policy_ver = await _load_entity_bundle(
        proposal.tenant_id,
        proposal.entity_id,
        proposal.policy_id,
    )

    idem_key = (
        proposal.idempotency_key.strip()
        if proposal.idempotency_key and proposal.idempotency_key.strip()
        else f"auto_{hashlib.sha256(f'{entity.tenant_id}:{entity.entity_id}:{proposal.action_type}:{uuid.uuid4().hex}'.encode()).hexdigest()[:32]}"
    )

    existing = await ActionOutbox.filter(
        tenant_id=entity.tenant_id, idempotency_key=idem_key
    ).first()
    if existing is not None:
        return OutboxSubmitResponse(
            outbox_id=existing.id,
            tenant_id=existing.tenant_id,
            entity_id=existing.entity_id,
            idempotency_key=existing.idempotency_key,
            duplicate=True,
            status=existing.status,
            verdict="allowed" if existing.status in ("scheduled", "dispatching", "succeeded") else existing.status,
            verdict_rule=existing.verdict_rule,
            verdict_reason=existing.verdict_reason,
            scheduled_at=existing.scheduled_at,
        )

    verdict = await evaluate_outbox_guardrails(
        proposal, entity, state, policy_cfg, now=check_now
    )
    adapter_name = resolve_adapter_name(proposal, policy_cfg)
    perm_level = resolve_permission_level(proposal, policy_cfg)
    requested_at = to_utc(proposal.scheduled_at) if proposal.scheduled_at else check_now

    if verdict.decision == "allowed":
        outbox_status = "scheduled"
        final_scheduled = requested_at
        finished_at = None
        approval_status = None
    elif verdict.decision == "awaiting_approval":
        outbox_status = "awaiting_approval"
        final_scheduled = requested_at
        finished_at = None
        approval_status = "pending"
    elif verdict.decision == "deferred":
        outbox_status = "deferred"
        final_scheduled = verdict.deferred_until or (check_now + timedelta(minutes=5))
        finished_at = None
        approval_status = None
    else:
        outbox_status = "guardrail_blocked"
        final_scheduled = requested_at
        finished_at = check_now
        approval_status = None

    outbox = await ActionOutbox.create(
        tenant_id=entity.tenant_id,
        entity_id=entity.entity_id,
        idempotency_key=idem_key,
        action_type=proposal.action_type,
        channel=proposal.channel,
        adapter_name=adapter_name,
        permission_level=perm_level,
        status=outbox_status,
        priority=proposal.priority,
        scheduled_at=final_scheduled,
        payload=dict(proposal.payload or {}),
        verdict_rule=verdict.rule_id,
        verdict_reason=verdict.reason,
        correlation_id=proposal.correlation_id,
        causation_id=proposal.causation_id,
        policy_id=proposal.policy_id or entity.policy_id or "default",
        policy_version=policy_ver,
        approval_status=approval_status,
        finished_at=finished_at,
    )

    if verdict.decision != "allowed":
        audit_row = await record_action_result(
            action=outbox,
            verdict=verdict.decision,
            verdict_rule=verdict.rule_id,
            execution_status="skipped",
            adapter_response={
                "verdict": verdict.decision,
                "reason": verdict.reason,
                "deferred_until": verdict.deferred_until.isoformat() if verdict.deferred_until else None,
            },
        )
        return OutboxSubmitResponse(
            outbox_id=outbox.id,
            tenant_id=outbox.tenant_id,
            entity_id=outbox.entity_id,
            idempotency_key=outbox.idempotency_key,
            duplicate=False,
            status=outbox.status,
            verdict=verdict.decision,
            verdict_rule=verdict.rule_id,
            verdict_reason=verdict.reason,
            scheduled_at=outbox.scheduled_at,
            execution_result_id=audit_row.id,
            execution_status=audit_row.execution_status,
        )

    if dispatch_immediately and to_utc(outbox.scheduled_at) <= check_now:
        return await execute_outbox_record(
            outbox,
            entity,
            policy_cfg,
            verdict_rule=verdict.rule_id,
            adapter_registry=adapter_registry,
            now=check_now,
            emit_receipt=emit_receipt,
        )

    return OutboxSubmitResponse(
        outbox_id=outbox.id,
        tenant_id=outbox.tenant_id,
        entity_id=outbox.entity_id,
        idempotency_key=outbox.idempotency_key,
        duplicate=False,
        status=outbox.status,
        verdict="allowed",
        verdict_rule=verdict.rule_id,
        verdict_reason=verdict.reason,
        scheduled_at=outbox.scheduled_at,
    )


async def dispatch_due_outbox_actions(
    *,
    adapter_registry: Optional[AdapterRegistry] = None,
    now: Optional[datetime] = None,
    limit: int = 50,
    emit_receipt: bool = True,
) -> int:
    """Re-verify guardrails and execute scheduled/deferred outbox actions that are now due."""
    check_now = to_utc(now) if now is not None else _utc_now()
    due_rows = (
        await ActionOutbox.filter(
            status__in=["scheduled", "deferred"],
            scheduled_at__lte=check_now,
        )
        .order_by("-priority", "scheduled_at", "id")
        .limit(limit)
        .all()
    )

    dispatched = 0
    for row in due_rows:
        try:
            entity, state, policy_cfg, _ = await _load_entity_bundle(
                row.tenant_id, row.entity_id, row.policy_id
            )
        except ValueError:
            row.status = "cancelled"
            row.verdict_reason = "entity_missing"
            row.finished_at = check_now
            await row.save()
            continue

        proposal = ActionProposal(
            entity_id=entity.entity_id,
            tenant_id=row.tenant_id,
            action_type=row.action_type,
            channel=row.channel,
            adapter_name=row.adapter_name,
            permission_level=row.permission_level,  # type: ignore[arg-type]
            priority=row.priority,
            scheduled_at=to_utc(row.scheduled_at),
            idempotency_key=row.idempotency_key,
            correlation_id=row.correlation_id,
            causation_id=row.causation_id,
            policy_id=row.policy_id,
            payload=dict(row.payload or {}),
        )
        verdict = await evaluate_outbox_guardrails(
            proposal,
            entity,
            state,
            policy_cfg,
            now=check_now,
            bypass_approval=(row.approval_status == "approved"),
            exclude_outbox_id=row.id,
        )
        if verdict.decision == "allowed":
            await execute_outbox_record(
                row,
                entity,
                policy_cfg,
                verdict_rule=verdict.rule_id,
                adapter_registry=adapter_registry,
                now=check_now,
                emit_receipt=emit_receipt,
            )
            dispatched += 1
        elif verdict.decision == "deferred":
            row.status = "deferred"
            row.scheduled_at = verdict.deferred_until or (check_now + timedelta(minutes=5))
            row.verdict_rule = verdict.rule_id
            row.verdict_reason = verdict.reason
            await row.save()
        elif verdict.decision == "awaiting_approval":
            row.status = "awaiting_approval"
            row.approval_status = "pending"
            row.verdict_rule = verdict.rule_id
            row.verdict_reason = verdict.reason
            await row.save()
        else:
            row.status = "guardrail_blocked"
            row.verdict_rule = verdict.rule_id
            row.verdict_reason = verdict.reason
            row.finished_at = check_now
            await row.save()
            await record_action_result(
                action=row,
                verdict="blocked",
                verdict_rule=verdict.rule_id,
                execution_status="skipped",
                adapter_response={"reason": verdict.reason},
            )
    return dispatched


async def cancel_entity_outbox(
    tenant_id: str,
    entity_id: str,
    *,
    reason: str = "cancelled",
    connection=None,
) -> int:
    """Cancel all pending/deferred/scheduled/awaiting_approval outbox items for an entity."""
    now = _utc_now()
    async def _do(conn) -> int:
        count = await ActionOutbox.filter(
            tenant_id=tenant_id,
            entity_id=entity_id,
            status__in=["proposed", "awaiting_approval", "deferred", "scheduled"],
        ).using_db(conn).update(
            status="cancelled",
            verdict_reason=str(reason)[:500],
            finished_at=now,
        )
        return int(count)

    if connection is not None:
        return await _do(connection)
    async with in_transaction() as conn:
        return await _do(conn)
