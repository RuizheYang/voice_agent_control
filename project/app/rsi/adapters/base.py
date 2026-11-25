"""Base edge ChannelAdapter protocol, AdapterRegistry, and ActionResultStore persistence."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.rsi.models import ActionExecutionResult, ActionOutbox
from app.rsi.schemas import (
    ActionResultItem,
    ActionResultListResponse,
    AdapterExecutionResult,
)


def _db(queryset, connection):
    return queryset.using_db(connection) if connection is not None else queryset


class BaseChannelAdapter(ABC):
    """Edge execution adapter interface decoupled from RSI kernel domain rules."""

    name: str = "base"

    @abstractmethod
    async def execute(
        self,
        action: ActionOutbox,
        entity_snapshot: Dict[str, Any],
    ) -> AdapterExecutionResult:
        """Execute or record an approved outbox action and return a normalized receipt."""


class AdapterRegistry:
    """Registry mapping adapter_name / channel to concrete edge ChannelAdapter instances."""

    def __init__(self) -> None:
        self._adapters: Dict[str, BaseChannelAdapter] = {}

    def register(self, adapter: BaseChannelAdapter) -> None:
        self._adapters[adapter.name] = adapter

    def get(self, name: str) -> Optional[BaseChannelAdapter]:
        return self._adapters.get(name)

    def resolve(
        self,
        adapter_name: Optional[str],
        channel: str,
    ) -> BaseChannelAdapter:
        if adapter_name and adapter_name in self._adapters:
            return self._adapters[adapter_name]
        if channel in self._adapters:
            return self._adapters[channel]
        passive = self._adapters.get("crm_passive")
        if passive is not None:
            return passive
        raise KeyError(
            f"No adapter registered for adapter_name={adapter_name!r}, channel={channel!r}"
        )


async def record_action_result(
    *,
    action: ActionOutbox,
    verdict: str,
    verdict_rule: Optional[str],
    execution_status: str,
    provider_ref: Optional[str] = None,
    adapter_response: Optional[Dict[str, Any]] = None,
    latency_ms: int = 0,
    connection=None,
) -> ActionExecutionResult:
    """Persist guardrail verdict & execution receipt into ActionResultStore."""
    snapshot = {
        "action_id": action.id,
        "idempotency_key": action.idempotency_key,
        "action_type": action.action_type,
        "channel": action.channel,
        "priority": action.priority,
        "permission_level": action.permission_level,
        "scheduled_at": action.scheduled_at.isoformat() if action.scheduled_at else None,
        "policy_id": action.policy_id,
        "policy_version": action.policy_version,
        "payload": dict(action.payload or {}),
        "verdict_reason": action.verdict_reason,
    }
    kwargs = dict(
        tenant_id=action.tenant_id,
        entity_id=action.entity_id,
        outbox_id=action.id,
        channel=action.channel,
        adapter_name=action.adapter_name,
        action_type=action.action_type,
        verdict=verdict,
        verdict_rule=verdict_rule,
        execution_status=execution_status,
        provider_ref=provider_ref,
        proposal_snapshot=snapshot,
        adapter_response=dict(adapter_response or {}),
        latency_ms=max(0, int(latency_ms)),
    )
    if connection is not None:
        return await ActionExecutionResult.create(using_db=connection, **kwargs)
    return await ActionExecutionResult.create(**kwargs)


async def query_action_results(
    tenant_id: str,
    *,
    entity_id: Optional[str] = None,
    channel: Optional[str] = None,
    verdict: Optional[str] = None,
    execution_status: Optional[str] = None,
    limit: int = 50,
) -> ActionResultListResponse:
    """Query ActionResultStore for upstream CRM consumption and auditability."""
    qs = ActionExecutionResult.filter(tenant_id=tenant_id)
    if entity_id:
        qs = qs.filter(entity_id=entity_id)
    if channel:
        qs = qs.filter(channel=channel)
    if verdict:
        qs = qs.filter(verdict=verdict)
    if execution_status:
        qs = qs.filter(execution_status=execution_status)

    total = await qs.count()
    rows = await qs.order_by("-id").limit(limit).all()
    items = [
        ActionResultItem(
            id=r.id,
            tenant_id=r.tenant_id,
            entity_id=r.entity_id,
            outbox_id=r.outbox_id,
            channel=r.channel,
            adapter_name=r.adapter_name,
            action_type=r.action_type,
            verdict=r.verdict,
            verdict_rule=r.verdict_rule,
            execution_status=r.execution_status,
            provider_ref=r.provider_ref,
            proposal_snapshot=r.proposal_snapshot or {},
            adapter_response=r.adapter_response or {},
            latency_ms=r.latency_ms,
            crm_acked_at=r.crm_acked_at,
            create_time=r.create_time,
        )
        for r in rows
    ]
    return ActionResultListResponse(tenant_id=tenant_id, total=total, items=items)


async def ack_action_result(
    result_id: int,
    *,
    tenant_id: str = "default",
) -> Optional[ActionExecutionResult]:
    """Mark an ActionExecutionResult row as consumed/acknowledged by upstream CRM."""
    row = await ActionExecutionResult.filter(id=result_id, tenant_id=tenant_id).first()
    if row is None:
        return None
    if row.crm_acked_at is None:
        row.crm_acked_at = datetime.now(timezone.utc)
        await row.save(update_fields=["crm_acked_at", "update_time"])
    return row
