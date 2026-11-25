"""Passive CRM edge adapter recording approved action intents for CRM consumption."""
from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict
from urllib.parse import urlsplit

import httpx

from app.rsi.adapters.base import BaseChannelAdapter
from app.rsi.models import ActionOutbox
from app.rsi.schemas import AdapterExecutionResult

logger = logging.getLogger(__name__)


def _is_safe_url(url: str) -> bool:
    clean = str(url or "").strip()
    if not clean:
        return False
    parsed = urlsplit(clean)
    if parsed.scheme == "https" and parsed.hostname:
        return True
    if parsed.scheme == "http" and parsed.hostname in ("127.0.0.1", "localhost"):
        return True
    if parsed.scheme == "http" and parsed.hostname and parsed.hostname.startswith("internal-"):
        return True
    return False


class CrmPassiveAdapter(BaseChannelAdapter):
    """Stores guardrail-approved proposals for upstream CRM pull/notification without coupling business APIs."""

    name = "crm_passive"

    async def execute(
        self,
        action: ActionOutbox,
        entity_snapshot: Dict[str, Any],
    ) -> AdapterExecutionResult:
        t0 = perf_counter()
        payload = dict(action.payload or {})
        response_data: Dict[str, Any] = {
            "mode": "crm_passive",
            "channel": action.channel,
            "action_type": action.action_type,
            "payload": payload,
            "entity_status": entity_snapshot.get("status"),
        }

        notify_url = payload.get("notify_url")
        if notify_url and _is_safe_url(str(notify_url)):
            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    resp = await client.post(
                        str(notify_url).strip(),
                        json={
                            "outbox_id": action.id,
                            "tenant_id": action.tenant_id,
                            "entity_id": action.entity_id,
                            "channel": action.channel,
                            "action_type": action.action_type,
                            "payload": payload,
                        },
                    )
                    response_data["notify_status_code"] = resp.status_code
            except Exception as err:
                response_data["notify_error"] = type(err).__name__
                logger.warning("[CrmPassiveAdapter] Optional CRM notify failed: %s", err)

        latency_ms = int((perf_counter() - t0) * 1000)
        return AdapterExecutionResult(
            ok=True,
            status="pending_crm",
            provider_ref=f"crm_outbox_{action.id}",
            response=response_data,
            latency_ms=latency_ms,
        )
