"""Edge Voice Gateways (Mock / HTTP Webhook / LiveKit / Pipecat) + city caller matching."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional
from urllib.parse import urlsplit

import httpx


@dataclass
class VoiceGatewayResult:
    status: Literal["accepted", "deferred", "retry", "failed", "unknown"]
    code: str
    provider_call_id: Optional[str] = None
    dispatch_payload: Dict[str, Any] = field(default_factory=dict)


def _is_safe_endpoint(url: str) -> bool:
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


def resolve_city_caller_number(
    payload: Dict[str, Any],
    policy_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Resolve city-matched local caller number/line_id from policy or action payload."""
    cfg = policy_config if isinstance(policy_config, dict) else {}
    city_map: Dict[str, Any] = {}
    if isinstance(cfg.get("city_caller_map"), dict):
        city_map.update(cfg["city_caller_map"])
    if isinstance(payload.get("city_caller_map"), dict):
        city_map.update(payload["city_caller_map"])

    raw_city = str(
        payload.get("city")
        or payload.get("customer_city")
        or ""
    ).strip()

    matched_entry: Any = None
    matched_city: Optional[str] = None
    if raw_city and city_map:
        candidates = [raw_city]
        if raw_city.endswith("市") and len(raw_city) > 1:
            candidates.append(raw_city[:-1])
        else:
            candidates.append(f"{raw_city}市")
        for cand in candidates:
            if cand in city_map:
                matched_entry = city_map[cand]
                matched_city = raw_city
                break

    caller_number = payload.get("caller_number")
    line_id = payload.get("line_id")
    if isinstance(matched_entry, str):
        caller_number = matched_entry
    elif isinstance(matched_entry, dict):
        caller_number = matched_entry.get("caller_number") or caller_number
        line_id = matched_entry.get("line_id") or line_id

    return {
        "matched_city": matched_city,
        "caller_number": caller_number,
        "line_id": line_id,
        "city_matched": matched_entry is not None,
    }


def build_voice_dispatch_payload(request_ctx: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(request_ctx.get("payload") or {})
    city_caller = dict(request_ctx.get("city_caller") or {})
    return {
        "call_id": request_ctx["call_id"],
        "tenant_id": request_ctx["tenant_id"],
        "entity_id": request_ctx["entity_id"],
        "line_type": payload.get("line_type", "landline"),
        "opening_script": payload.get("opening_script"),
        "phone_number": payload.get("phone_number"),
        "caller_number": city_caller.get("caller_number"),
        "line_id": city_caller.get("line_id"),
        "matched_city": city_caller.get("matched_city"),
        "city_matched": bool(city_caller.get("city_matched")),
        "agent_id": payload.get("agent_id", "default"),
        "extra": dict(payload.get("extra") or {}),
    }


class BaseVoiceGateway:
    """Abstract edge telephony gateway protocol."""

    mode: str = "base"

    async def submit_call(self, request_ctx: Dict[str, Any]) -> VoiceGatewayResult:
        raise NotImplementedError


class MockVoiceGateway(BaseVoiceGateway):
    """In-memory sandbox telephony gateway returning deterministic accepted receipts."""

    mode = "mock"

    async def submit_call(self, request_ctx: Dict[str, Any]) -> VoiceGatewayResult:
        dispatch_payload = build_voice_dispatch_payload(request_ctx)
        call_id = request_ctx["call_id"]
        return VoiceGatewayResult(
            status="accepted",
            code="mock_accepted",
            provider_call_id=f"mock_{call_id}",
            dispatch_payload=dispatch_payload,
        )


class WebhookVoiceGateway(BaseVoiceGateway):
    """Generic HTTP Webhook outbound telephony gateway."""

    mode = "webhook"

    def __init__(self, endpoint_url: Optional[str] = None, timeout_seconds: float = 3.0) -> None:
        self.endpoint_url = (endpoint_url or os.getenv("TELEPHONY_SEND_URL", "")).strip()
        self.timeout_seconds = timeout_seconds

    async def submit_call(self, request_ctx: Dict[str, Any]) -> VoiceGatewayResult:
        url = (
            str((request_ctx.get("payload") or {}).get("gateway_url") or "").strip()
            or self.endpoint_url
        )
        if not url:
            return VoiceGatewayResult("deferred", "telephony_endpoint_missing")
        if not _is_safe_endpoint(url):
            return VoiceGatewayResult("deferred", "invalid_telephony_endpoint")

        dispatch_payload = build_voice_dispatch_payload(request_ctx)
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resp = await client.post(url, json=dispatch_payload)
            if 200 <= resp.status_code < 300:
                body = resp.json() if resp.content else {}
                provider_id = (
                    body.get("provider_call_id") or body.get("callId") or f"wh_{request_ctx['call_id']}"
                ) if isinstance(body, dict) else f"wh_{request_ctx['call_id']}"
                return VoiceGatewayResult(
                    status="accepted",
                    code="webhook_accepted",
                    provider_call_id=str(provider_id),
                    dispatch_payload=dispatch_payload,
                )
            return VoiceGatewayResult(
                status="retry" if resp.status_code >= 500 else "failed",
                code=f"http_{resp.status_code}",
                dispatch_payload=dispatch_payload,
            )
        except Exception:
            return VoiceGatewayResult("retry", "transport_retry", dispatch_payload=dispatch_payload)


class LiveKitVoiceGateway(WebhookVoiceGateway):
    """LiveKit Agents + SIP dispatch gateway carrying room metadata and city caller info."""

    mode = "livekit"

    def __init__(self, endpoint_url: Optional[str] = None, timeout_seconds: float = 3.0) -> None:
        super().__init__(
            endpoint_url=endpoint_url or os.getenv("LIVEKIT_DISPATCH_BRIDGE_URL", ""),
            timeout_seconds=timeout_seconds,
        )

    async def submit_call(self, request_ctx: Dict[str, Any]) -> VoiceGatewayResult:
        enriched = dict(request_ctx)
        base = build_voice_dispatch_payload(request_ctx)
        base["room_name"] = f"rsi-{request_ctx['entity_id']}-{request_ctx['call_id']}"
        base["sip_trunk_id"] = os.getenv("LIVEKIT_SIP_TRUNK_ID", "").strip()
        enriched["payload"] = {**(request_ctx.get("payload") or {}), **base}
        return await super().submit_call(enriched)


class PipecatVoiceGateway(WebhookVoiceGateway):
    """Pipecat outbound bot dispatch gateway."""

    mode = "pipecat"

    def __init__(self, endpoint_url: Optional[str] = None, timeout_seconds: float = 3.0) -> None:
        super().__init__(
            endpoint_url=endpoint_url or os.getenv("PIPECAT_OUTBOUND_URL", ""),
            timeout_seconds=timeout_seconds,
        )


def resolve_voice_gateway(
    mode: Optional[str] = None,
    *,
    endpoint_url: Optional[str] = None,
) -> BaseVoiceGateway:
    active_mode = (mode or os.getenv("TELEPHONY_GATEWAY", "mock")).strip().lower()
    if active_mode == "livekit":
        return LiveKitVoiceGateway(endpoint_url=endpoint_url)
    if active_mode == "pipecat":
        return PipecatVoiceGateway(endpoint_url=endpoint_url)
    if active_mode == "webhook":
        return WebhookVoiceGateway(endpoint_url=endpoint_url)
    return MockVoiceGateway()
