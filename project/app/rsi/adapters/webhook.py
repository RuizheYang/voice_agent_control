"""Edge HTTP Webhook ChannelAdapter with SSRF guard and normalized retry classification."""
from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

import httpx

from app.rsi.adapters.base import BaseChannelAdapter
from app.rsi.models import ActionOutbox
from app.rsi.schemas import AdapterExecutionResult

logger = logging.getLogger(__name__)


def _is_safe_webhook_url(url: str) -> bool:
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


class WebhookChannelAdapter(BaseChannelAdapter):
    """Generic HTTP Webhook delivery adapter for external side-effect channels."""

    name = "webhook"

    def __init__(self, *, timeout_seconds: float = 3.0) -> None:
        self.timeout_seconds = timeout_seconds

    async def execute(
        self,
        action: ActionOutbox,
        entity_snapshot: Dict[str, Any],
    ) -> AdapterExecutionResult:
        t0 = perf_counter()
        payload = dict(action.payload or {})
        policy_cfg = entity_snapshot.get("policy_config") or {}
        target_url: Optional[str] = (
            payload.get("callback_url")
            or payload.get("url")
            or policy_cfg.get("webhook_url")
        )
        if not target_url or not str(target_url).strip():
            return AdapterExecutionResult(
                ok=False,
                status="failed",
                error="webhook_url_missing",
                latency_ms=int((perf_counter() - t0) * 1000),
            )
        clean_url = str(target_url).strip()
        if not _is_safe_webhook_url(clean_url):
            return AdapterExecutionResult(
                ok=False,
                status="failed",
                error="unsafe_webhook_url",
                latency_ms=int((perf_counter() - t0) * 1000),
            )

        body = {
            "outbox_id": action.id,
            "tenant_id": action.tenant_id,
            "entity_id": action.entity_id,
            "channel": action.channel,
            "action_type": action.action_type,
            "idempotency_key": action.idempotency_key,
            "payload": payload,
        }
        headers = {
            "Content-Type": "application/json",
            "X-RSI-Outbox-Id": str(action.id),
            "X-RSI-Idempotency-Key": action.idempotency_key,
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resp = await client.post(clean_url, json=body, headers=headers)
            latency_ms = int((perf_counter() - t0) * 1000)
            if 200 <= resp.status_code < 300:
                return AdapterExecutionResult(
                    ok=True,
                    status="succeeded",
                    provider_ref=f"wh_{action.id}_{resp.status_code}",
                    response={"status_code": resp.status_code, "url": clean_url},
                    latency_ms=latency_ms,
                )
            retryable = resp.status_code >= 500 or resp.status_code == 429
            return AdapterExecutionResult(
                ok=False,
                status="retry" if retryable else "failed",
                response={"status_code": resp.status_code, "url": clean_url},
                error=f"http_status_{resp.status_code}",
                latency_ms=latency_ms,
            )
        except Exception as err:
            latency_ms = int((perf_counter() - t0) * 1000)
            logger.warning("[WebhookChannelAdapter] POST failed url=%s err=%s", clean_url, err)
            return AdapterExecutionResult(
                ok=False,
                status="retry",
                error=f"{type(err).__name__}: {err}",
                latency_ms=latency_ms,
            )
