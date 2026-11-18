import hmac
import hashlib
import time
import logging
import os
from typing import Optional
from fastapi import Request, HTTPException

from app.redis_client.redis_manager import redis_manager
from app.dbmodels.crud import get_app_auth

logger = logging.getLogger(__name__)

# 允许的最大时间漂移：5分钟 (300,000 毫秒)
MAX_TIMESTAMP_DRIFT_MS = 300_000
# 默认开发测试 secret
DEV_APP_SECRETS = {
    "app_test": "test_secret_key_123456",
    "dev": "dev_secret",
}

async def check_and_lock_request_id(request_id: str, expire_seconds: int = 86400) -> bool:
    """
    检查并锁定 request_id，保证接口幂等性 (code: 40005)
    """
    if not request_id:
        return True
    try:
        client = await redis_manager._get_client()
        lock_key = f"idempotency:req:{request_id}"
        # 使用 nx=True 保证原子性
        acquired = await client.set(lock_key, "1", ex=expire_seconds, nx=True)
        if not acquired:
            logger.warning(f"[Idempotency] 重复的 request_id: {request_id}")
            raise HTTPException(
                status_code=400,
                detail={"code": 40005, "msg": f"request_id 重复: {request_id}"}
            )
        return True
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[Idempotency] Redis 幂等检查异常: {e}")
        # Redis 异常时不直接阻断正常请求
        return True

async def verify_inflow_signature(
    request: Request,
    raw_body: str = "",
    workflow_id: Optional[str] = None,
    strict: bool = False,
) -> str:
    """
    验证线索流入的请求头与 HMAC-SHA256 签名：
    - X-App-Id
    - X-Timestamp
    - X-Nonce
    - X-Signature
    StringToSign = X-App-Id + "." + X-Timestamp + "." + X-Nonce + "." + raw_body
    """
    if strict:
        try:
            app_id = await _verify_registered_app(request, raw_body, workflow_id)
        except HTTPException as error:
            logger.log(logging.ERROR if error.status_code >= 500 else logging.WARNING,
                       "[Auth] 【拒绝】线索流入鉴权失败: app_id=%r, workflow_id=%r, code=%s, reason=%s",
                       request.headers.get("X-App-Id", "")[:128], workflow_id,
                       error.detail.get("code"), error.detail.get("msg"))
            raise
        except Exception as error:
            logger.error("[Auth] 【失败】线索流入鉴权异常: app_id=%r, workflow_id=%r, error_type=%s",
                         request.headers.get("X-App-Id", "")[:128], workflow_id, type(error).__name__)
            raise
        logger.debug("[Auth] 【完成】线索流入鉴权通过: app_id=%r, workflow_id=%r", app_id, workflow_id)
        return app_id

    # 检查环境变量或配置是否跳过鉴权
    skip_auth = os.getenv("SKIP_AUTH", "").lower() in ("true", "1", "yes")

    x_app_id = request.headers.get("X-App-Id")
    x_timestamp = request.headers.get("X-Timestamp")
    x_nonce = request.headers.get("X-Nonce")
    x_signature = request.headers.get("X-Signature")

    # 如果没有传递鉴权头且处于跳过鉴权模式，直接放行
    if not x_app_id and skip_auth:
        logger.debug("[Auth] SKIP_AUTH 生效，已放行无鉴权头请求")
        return "dev"

    if not all([x_app_id, x_timestamp, x_nonce, x_signature]):
        if skip_auth or x_app_id == "dev":
            logger.warning("[Auth] 缺少部分签名头，但处于开发放行模式")
            return x_app_id or "dev"
        raise HTTPException(
            status_code=400,
            detail={
                "code": 40001,
                "msg": "缺少认证请求头: 必须包含 X-App-Id, X-Timestamp, X-Nonce, X-Signature"
            }
        )

    # 1. 验证时间戳漂移 (防重放)
    try:
        req_ts = int(x_timestamp)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail={"code": 40001, "msg": "X-Timestamp 必须为毫秒级整数时间戳"}
        )

    now_ms = int(time.time() * 1000)
    if abs(now_ms - req_ts) > MAX_TIMESTAMP_DRIFT_MS:
        logger.warning(f"[Auth] 时间戳过期: req_ts={req_ts}, now_ms={now_ms}")
        raise HTTPException(
            status_code=400,
            detail={"code": 40002, "msg": "请求时间戳已过期或与服务器时间偏差过大"}
        )

    # 2. 验证 Nonce (防重放)
    try:
        client = await redis_manager._get_client()
        nonce_key = f"auth:nonce:{x_app_id}:{x_nonce}"
        nonce_acquired = await client.set(nonce_key, "1", ex=300, nx=True)
        if not nonce_acquired and not skip_auth:
            logger.warning(f"[Auth] 拦截到重放请求 Nonce 重复: {x_nonce}")
            raise HTTPException(
                status_code=400,
                detail={"code": 40002, "msg": "检测到重放请求 (X-Nonce 重复)"}
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[Auth] Redis Nonce 检查异常: {e}")

    # 3. 获取 App Secret
    app_auth = await get_app_auth(x_app_id)
    if app_auth:
        secret = app_auth.app_secret
        allowed_workflows = app_auth.allowed_workflows or ["*"]
    elif x_app_id in DEV_APP_SECRETS:
        secret = DEV_APP_SECRETS[x_app_id]
        allowed_workflows = ["*"]
    elif skip_auth:
        secret = "dev_secret"
        allowed_workflows = ["*"]
    else:
        logger.warning(f"[Auth] 未知的 X-App-Id: {x_app_id}")
        raise HTTPException(
            status_code=400,
            detail={"code": 40002, "msg": f"未知的 X-App-Id: {x_app_id}"}
        )

    # 4. 校验 HMAC-SHA256 签名
    string_to_sign = f"{x_app_id}.{x_timestamp}.{x_nonce}.{raw_body}"
    computed_sig = hmac.new(
        secret.encode("utf-8"),
        string_to_sign.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(computed_sig, x_signature) and not skip_auth:
        logger.warning(f"[Auth] 签名校验失败: app_id={x_app_id}, expected={computed_sig}, received={x_signature}")
        raise HTTPException(
            status_code=400,
            detail={"code": 40002, "msg": "HMAC-SHA256 签名校验失败"}
        )

    # 5. 校验 X-App-Id 访问 workflow_id 的权限
    if workflow_id and allowed_workflows:
        if "*" not in allowed_workflows and workflow_id not in allowed_workflows:
            logger.warning(f"[Auth] X-App-Id {x_app_id} 无权访问 workflow_id: {workflow_id}")
            raise HTTPException(
                status_code=403,
                detail={"code": 40003, "msg": f"X-App-Id 未授权访问该 workflow_id: {workflow_id}"}
            )

    return x_app_id


async def _verify_registered_app(request: Request, raw_body: str, workflow_id: Optional[str]) -> str:
    """新商业化入口只接受已注册应用；不继承历史 dev/SKIP_AUTH 放行。"""
    app_id = request.headers.get("X-App-Id", "")
    timestamp = request.headers.get("X-Timestamp", "")
    nonce = request.headers.get("X-Nonce", "")
    signature = request.headers.get("X-Signature", "")
    if not all((app_id, timestamp, nonce, signature)) or len(app_id) > 128 or len(nonce) > 128:
        raise HTTPException(400, detail={"code": 40001, "msg": "缺少或不合法的签名请求头"})
    if len(signature) != 64 or any(character not in "0123456789abcdef" for character in signature):
        raise HTTPException(401, detail={"code": 40002, "msg": "请求签名格式不正确"})
    try:
        request_time = int(timestamp)
    except ValueError:
        raise HTTPException(400, detail={"code": 40001, "msg": "X-Timestamp 必须为毫秒时间戳"}) from None
    if abs(int(time.time()*1000)-request_time) > MAX_TIMESTAMP_DRIFT_MS:
        raise HTTPException(401, detail={"code": 40002, "msg": "签名时间戳已过期"})
    app = await get_app_auth(app_id)
    if app is None:
        raise HTTPException(401, detail={"code": 40002, "msg": "应用未注册或已停用"})
    message = f"{app_id}.{timestamp}.{nonce}.{raw_body}"
    expected = hmac.new(app.app_secret.encode(), message.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(401, detail={"code": 40002, "msg": "请求签名不正确"})
    allowed = app.allowed_workflows or []
    if not isinstance(allowed, list) or any(not isinstance(value, str) for value in allowed):
        raise HTTPException(503, detail={"code": 50000, "msg": "应用授权配置不合法"})
    if workflow_id is not None and "*" not in allowed and workflow_id not in allowed:
        raise HTTPException(403, detail={"code": 40003, "msg": "应用无权访问此workflow"})
    try:
        client = await redis_manager._get_client()
        nonce_key = hashlib.sha256((app_id+"\x00"+nonce).encode()).hexdigest()
        acquired = await client.set(f"auth:inflow:nonce:{nonce_key}", "1", ex=601, nx=True)
    except Exception:
        raise HTTPException(503, detail={"code": 50000, "msg": "防重放服务暂不可用"}) from None
    if not acquired:
        raise HTTPException(401, detail={"code": 40002, "msg": "X-Nonce 已使用，请更新签名后重试"})
    request.state.allowed_workflows = allowed
    return app_id
