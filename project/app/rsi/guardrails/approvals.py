"""Unified L2 Deferred Approval management & Outbox resumption."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from tortoise.transactions import in_transaction

from app.rsi.adapters.base import AdapterRegistry, record_action_result
from app.rsi.guardrails.outbox_guardrail import (
    _load_entity_bundle,
    evaluate_outbox_guardrails,
    execute_outbox_record,
)
from app.rsi.guardrails.time_window import to_utc
from app.rsi.models import ActionOutbox
from app.rsi.schemas import ActionProposal, ApprovalResolveResponse, RSIEventEnvelope

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def list_pending_outbox_approvals(
    *,
    tenant_id: Optional[str] = None,
    entity_id: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Query pending L2 outbox actions awaiting human operator approval."""
    qs = ActionOutbox.filter(status="awaiting_approval")
    if tenant_id:
        qs = qs.filter(tenant_id=tenant_id)
    if entity_id:
        qs = qs.filter(entity_id=entity_id)
    rows = await qs.order_by("id").limit(limit).all()
    return [
        {
            "outbox_id": r.id,
            "tenant_id": r.tenant_id,
            "entity_id": r.entity_id,
            "action_type": r.action_type,
            "channel": r.channel,
            "adapter_name": r.adapter_name,
            "permission_level": r.permission_level,
            "priority": r.priority,
            "payload": r.payload or {},
            "verdict_reason": r.verdict_reason,
            "create_time": r.create_time.isoformat() if r.create_time else None,
        }
        for r in rows
    ]


async def resolve_outbox_approval(
    outbox_id: int,
    *,
    approved: bool,
    reviewer: str = "operator",
    comment: str = "",
    adapter_registry: Optional[AdapterRegistry] = None,
    dispatch_immediately: bool = True,
    now: Optional[datetime] = None,
    emit_receipt: bool = True,
) -> ApprovalResolveResponse:
    """Resolve an awaiting_approval ActionOutbox entry, re-check boundaries, and dispatch if allowed."""
    resolve_now = to_utc(now) if now is not None else _utc_now()

    async with in_transaction() as conn:
        outbox = (
            await ActionOutbox.filter(id=outbox_id)
            .using_db(conn)
            .select_for_update()
            .first()
        )
        if outbox is None:
            raise ValueError(f"ActionOutbox {outbox_id} not found")

        if outbox.status != "awaiting_approval":
            return ApprovalResolveResponse(
                outbox_id=outbox.id,
                tenant_id=outbox.tenant_id,
                entity_id=outbox.entity_id,
                already_resolved=True,
                approved=(outbox.approval_status == "approved"),
                status=outbox.status,
                verdict_rule=outbox.verdict_rule,
            )

        outbox.approval_reviewer = reviewer
        outbox.approval_comment = comment
        outbox.approved_at = resolve_now

        if not approved:
            outbox.status = "cancelled"
            outbox.approval_status = "rejected"
            outbox.verdict_rule = "approval:rejected"
            outbox.verdict_reason = comment or f"Rejected by {reviewer}"
            outbox.finished_at = resolve_now
            await outbox.save(using_db=conn)
            await record_action_result(
                action=outbox,
                verdict="blocked",
                verdict_rule="approval:rejected",
                execution_status="skipped",
                adapter_response={"approved": False, "reviewer": reviewer, "comment": comment},
                connection=conn,
            )

    if not approved:
        await _emit_approval_event(outbox, approved=False, reviewer=reviewer, comment=comment, now=resolve_now)
        return ApprovalResolveResponse(
            outbox_id=outbox.id,
            tenant_id=outbox.tenant_id,
            entity_id=outbox.entity_id,
            already_resolved=False,
            approved=False,
            status="cancelled",
            verdict="blocked",
            verdict_rule="approval:rejected",
            execution_status="skipped",
        )

    # Approved path: re-evaluate remaining guardrails with bypass_approval=True
    entity, state, policy_cfg, _ = await _load_entity_bundle(
        outbox.tenant_id, outbox.entity_id, outbox.policy_id
    )
    proposal = ActionProposal(
        entity_id=entity.entity_id,
        tenant_id=outbox.tenant_id,
        action_type=outbox.action_type,
        channel=outbox.channel,
        adapter_name=outbox.adapter_name,
        permission_level=outbox.permission_level,  # type: ignore[arg-type]
        priority=outbox.priority,
        scheduled_at=resolve_now,
        idempotency_key=outbox.idempotency_key,
        correlation_id=outbox.correlation_id,
        causation_id=outbox.causation_id,
        policy_id=outbox.policy_id,
        payload=dict(outbox.payload or {}),
    )
    verdict = await evaluate_outbox_guardrails(
        proposal,
        entity,
        state,
        policy_cfg,
        now=resolve_now,
        bypass_approval=True,
        exclude_outbox_id=outbox.id,
    )

    outbox.approval_status = "approved"
    if verdict.decision != "allowed":
        outbox.status = "deferred" if verdict.decision == "deferred" else "guardrail_blocked"
        outbox.verdict_rule = verdict.rule_id
        outbox.verdict_reason = verdict.reason
        if verdict.deferred_until:
            outbox.scheduled_at = verdict.deferred_until
        if outbox.status == "guardrail_blocked":
            outbox.finished_at = resolve_now
        await outbox.save()
        await record_action_result(
            action=outbox,
            verdict=verdict.decision,
            verdict_rule=verdict.rule_id,
            execution_status="skipped",
            adapter_response={"approved": True, "guardrail_block": verdict.reason},
        )
        await _emit_approval_event(outbox, approved=True, reviewer=reviewer, comment=comment, now=resolve_now)
        return ApprovalResolveResponse(
            outbox_id=outbox.id,
            tenant_id=outbox.tenant_id,
            entity_id=outbox.entity_id,
            already_resolved=False,
            approved=True,
            status=outbox.status,
            verdict=verdict.decision,
            verdict_rule=verdict.rule_id,
            execution_status="skipped",
        )

    outbox.status = "scheduled"
    outbox.verdict_rule = "permission:L2_approved"
    outbox.verdict_reason = f"Approved by {reviewer}"
    await outbox.save()

    exec_status: Optional[str] = None
    provider_ref: Optional[str] = None
    if dispatch_immediately:
        exec_res = await execute_outbox_record(
            outbox,
            entity,
            policy_cfg,
            verdict_rule="permission:L2_approved",
            adapter_registry=adapter_registry,
            now=resolve_now,
            emit_receipt=emit_receipt,
        )
        exec_status = exec_res.execution_status
        provider_ref = exec_res.provider_ref
        outbox.status = exec_res.status

    await _emit_approval_event(outbox, approved=True, reviewer=reviewer, comment=comment, now=resolve_now)
    return ApprovalResolveResponse(
        outbox_id=outbox.id,
        tenant_id=outbox.tenant_id,
        entity_id=outbox.entity_id,
        already_resolved=False,
        approved=True,
        status=outbox.status,
        verdict="allowed",
        verdict_rule="permission:L2_approved",
        execution_status=exec_status,
        provider_ref=provider_ref,
    )


async def _emit_approval_event(
    outbox: ActionOutbox,
    *,
    approved: bool,
    reviewer: str,
    comment: str,
    now: datetime,
) -> None:
    try:
        from app.rsi.memory_reducer import ingest_event

        await ingest_event(
            RSIEventEnvelope(
                event_id=f"appr_res_{outbox.id}_{'ok' if approved else 'rej'}",
                tenant_id=outbox.tenant_id,
                event_type="approval.resolved",
                category="approval",
                entity_id=outbox.entity_id,
                occurred_at=now,
                source="rsi_approvals",
                actor_type="human_rep",
                actor_id=reviewer,
                facts={
                    "approval_id": f"outbox_{outbox.id}",
                    "outbox_id": outbox.id,
                    "approved": approved,
                    "status": "approved" if approved else "rejected",
                    "reviewer": reviewer,
                    "comment": comment,
                    "tool_name": outbox.action_type,
                },
            )
        )
    except Exception as err:
        logger.warning("[RSI-Approvals] Failed to emit approval.resolved event outbox_id=%s: %s", outbox.id, err)
