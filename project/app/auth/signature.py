"""Multi-tenant HMAC-SHA256 signature & anti-replay authentication for RSI V3."""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time
from typing import Dict, Optional

from fastapi import HTTPException, Request

from app.redis_client.redis_manager import redis_manager
from app.rsi.models import RsiTenantAuth

logger = logging.getLogger(__name__)

# Maximum allowed clock drift: 5 minutes (300,000 ms)
MAX_TIMESTAMP_DRIFT_MS = 300_000
_LOCAL_NONCE_CACHE: Dict[str, float] = {}
_LOCAL_REQ_ID_CACHE: Dict[str, float] = {}


def clear_local_nonce_cache() -> None:
    """Clear in-process anti-replay nonce & request-id caches (primarily for tests)."""
    _LOCAL_NONCE_CACHE.clear()
    _LOCAL_REQ_ID_CACHE.clear()


def _prune_expired(cache: Dict[str, float], now_s: float) -> None:
    expired = [k for k, exp in cache.items() if exp <= now_s]
    for k in expired:
        cache.pop(k, None)


async def _acquire_once_token(cache_key: str, *, ttl_seconds: int, fallback: Dict[str, float]) -> bool:
    """Record a single-use nonce/idempotency token in Redis if active, else in-process TTL map."""
    if redis_manager._initialized and redis_manager._redis is not None:
        try:
            acquired = await redis_manager._redis.set(
                cache_key, "1", ex=ttl_seconds, nx=True
            )
            return bool(acquired)
        except Exception as err:
            logger.warning("[Auth] Redis unavailable for replay guard, falling back local: %s", err)

    now_s = time.monotonic()
    _prune_expired(fallback, now_s)
    if cache_key in fallback:
        return False
    fallback[cache_key] = now_s + ttl_seconds
    return True


async def check_and_lock_request_id(request_id: str, expire_seconds: int = 86400) -> bool:
    """Lock request_id once to prevent duplicate request replay."""
    if not request_id:
        return True
    ok = await _acquire_once_token(
        f"idempotency:req:{request_id}",
        ttl_seconds=expire_seconds,
        fallback=_LOCAL_REQ_ID_CACHE,
    )
    if not ok:
        raise HTTPException(
            status_code=400,
            detail={"code": 40005, "msg": f"request_id 重复: {request_id}"},
        )
    return True


async def _resolve_tenant_auth(tenant_id: Optional[str], app_id: str) -> Optional[RsiTenantAuth]:
    if tenant_id:
        row = await RsiTenantAuth.filter(
            tenant_id=tenant_id, app_id=app_id, is_active=True
        ).first()
        if row is not None:
            return row
    return await RsiTenantAuth.filter(app_id=app_id, is_active=True).first()


async def verify_rsi_signature(
    request: Request,
    raw_body: str = "",
    *,
    policy_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
    strict: bool = True,
) -> str:
    """Verify HMAC-SHA256 request headers (`X-App-Id`, `X-Timestamp`, `X-Nonce`, `X-Signature`).

    StringToSign = `{app_id}.{timestamp}.{nonce}.{raw_body}`
    """
    skip_auth = os.getenv("SKIP_AUTH", "").lower() in ("true", "1", "yes")
    x_app_id = request.headers.get("X-App-Id", "")
    x_timestamp = request.headers.get("X-Timestamp", "")
    x_nonce = request.headers.get("X-Nonce", "")
    x_signature = request.headers.get("X-Signature", "")

    if not strict and skip_auth and not x_app_id:
        return "dev"

    if not all((x_app_id, x_timestamp, x_nonce, x_signature)) or len(x_app_id) > 128 or len(x_nonce) > 128:
        raise HTTPException(
            status_code=400,
            detail={"code": 40001, "msg": "缺少或不合法的签名请求头"},
        )

    if len(x_signature) != 64 or any(c not in "0123456789abcdef" for c in x_signature.lower()):
        raise HTTPException(
            status_code=401,
            detail={"code": 40002, "msg": "请求签名格式不正确"},
        )

    try:
        req_ts = int(x_timestamp)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail={"code": 40001, "msg": "X-Timestamp 必须为毫秒时间戳"},
        ) from None

    if abs(int(time.time() * 1000) - req_ts) > MAX_TIMESTAMP_DRIFT_MS:
        raise HTTPException(
            status_code=401,
            detail={"code": 40002, "msg": "签名时间戳已过期"},
        )

    header_tenant = tenant_id or request.headers.get("X-Tenant-Id")
    app = await _resolve_tenant_auth(header_tenant, x_app_id)
    if app is None:
        raise HTTPException(
            status_code=401,
            detail={"code": 40002, "msg": "应用未注册或已停用"},
        )

    message = f"{x_app_id}.{x_timestamp}.{x_nonce}.{raw_body}"
    expected = hmac.new(
        app.app_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected.lower(), x_signature.lower()):
        raise HTTPException(
            status_code=401,
            detail={"code": 40002, "msg": "请求签名不正确"},
        )

    allowed = app.allowed_policies or ["*"]
    if not isinstance(allowed, list) or any(not isinstance(v, str) for v in allowed):
        raise HTTPException(
            status_code=503,
            detail={"code": 50000, "msg": "应用授权配置不合法"},
        )

    if policy_id is not None and "*" not in allowed and policy_id not in allowed:
        raise HTTPException(
            status_code=403,
            detail={"code": 40003, "msg": f"应用无权访问策略 {policy_id}"},
        )

    nonce_digest = hashlib.sha256(f"{app.tenant_id}\x00{x_app_id}\x00{x_nonce}".encode("utf-8")).hexdigest()
    acquired = await _acquire_once_token(
        f"auth:rsi:nonce:{nonce_digest}",
        ttl_seconds=601,
        fallback=_LOCAL_NONCE_CACHE,
    )
    if not acquired:
        raise HTTPException(
            status_code=401,
            detail={"code": 40002, "msg": "X-Nonce 已使用，请更新签名后重试"},
        )

    request.state.tenant_id = app.tenant_id
    request.state.allowed_policies = allowed
    return x_app_id


async def verify_inflow_signature(
    request: Request,
    raw_body: str = "",
    workflow_id: Optional[str] = None,
    strict: bool = True,
) -> str:
    """Backward-compatible alias delegating to `verify_rsi_signature`."""
    return await verify_rsi_signature(
        request,
        raw_body,
        policy_id=workflow_id,
        strict=strict,
    )
