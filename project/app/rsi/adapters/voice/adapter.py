"""Edge VoiceChannelAdapter bridging Global Action Outbox to Voice gateways & priority queue."""
from __future__ import annotations

from time import perf_counter
from typing import Any, Dict, Optional

from app.rsi.adapters.base import BaseChannelAdapter
from app.rsi.adapters.voice.gateway import (
    BaseVoiceGateway,
    resolve_city_caller_number,
    resolve_voice_gateway,
)
from app.rsi.adapters.voice.queue import VoicePriorityQueue
from app.rsi.models import ActionOutbox
from app.rsi.schemas import AdapterExecutionResult


class VoiceChannelAdapter(BaseChannelAdapter):
    """Executes approved voice call proposals with city-matched caller line & optional priority index."""

    name = "voice"

    def __init__(
        self,
        *,
        gateway: Optional[BaseVoiceGateway] = None,
        priority_queue: Optional[VoicePriorityQueue] = None,
    ) -> None:
        self.gateway = gateway
        self.priority_queue = priority_queue

    async def execute(
        self,
        action: ActionOutbox,
        entity_snapshot: Dict[str, Any],
    ) -> AdapterExecutionResult:
        t0 = perf_counter()
        payload = dict(action.payload or {})
        policy_config = entity_snapshot.get("policy_config") or {}
        city_caller = resolve_city_caller_number(payload, policy_config)

        if self.priority_queue is not None:
            run_at_ms = int(action.scheduled_at.timestamp() * 1000)
            await self.priority_queue.publish(action.id, run_at_ms, action.priority)

        call_id = str(payload.get("call_id") or f"rsi_call_{action.id}")
        gw = self.gateway or resolve_voice_gateway(payload.get("gateway_mode"))
        gw_result = await gw.submit_call(
            {
                "call_id": call_id,
                "tenant_id": action.tenant_id,
                "entity_id": action.entity_id,
                "payload": payload,
                "city_caller": city_caller,
                "policy_config": policy_config,
            }
        )
        latency_ms = int((perf_counter() - t0) * 1000)

        if gw_result.status == "accepted":
            return AdapterExecutionResult(
                ok=True,
                status="succeeded",
                provider_ref=gw_result.provider_call_id or call_id,
                response={
                    "gateway_mode": gw.mode,
                    "code": gw_result.code,
                    "city_caller": city_caller,
                    "dispatch_payload": gw_result.dispatch_payload,
                },
                latency_ms=latency_ms,
            )

        mapped_status = "retry" if gw_result.status in ("retry", "deferred", "unknown") else "failed"
        return AdapterExecutionResult(
            ok=False,
            status=mapped_status,
            provider_ref=gw_result.provider_call_id,
            response={
                "gateway_mode": gw.mode,
                "code": gw_result.code,
                "city_caller": city_caller,
            },
            error=gw_result.code,
            latency_ms=latency_ms,
        )
