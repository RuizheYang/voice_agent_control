import logging
from functools import wraps
from typing import Any

from app.utils.exceptions import LockTimeoutError, StateTransitionError

logger = logging.getLogger(__name__)


def idempotent(idempotency_key: str):
    """
    幂等装饰器。
    
    参数:
    - idempotency_key: 用于从参数获取幂等键的属性名。
      目前支持直接在第一个参数 (request 对象) 上取对应属性，或在参数对象的顶层查找对应属性。
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, request_obj: Any, *args, **kwargs):
            from app.dbmodels import crud
            
            # 尝试获取请求id和线索id
            request_id = getattr(request_obj, idempotency_key, None)
            
            if isinstance(request_id, str):
                request_id = request_id.strip()
                
            clue_id = None
            if hasattr(request_obj, "data") and hasattr(request_obj.data, "clueId"):
                clue_id = request_obj.data.clueId
            elif hasattr(request_obj, "clueId"):
                clue_id = request_obj.clueId

            # 若没有请求id，直接执行原函数（不进行幂等控制）
            if not request_id:
                return await func(self, request_obj, *args, **kwargs)

            # Step 1: 尝试获取幂等锁
            idempotency_state = await crud.try_acquire_call_end_idempotency(request_id, clue_id)
            
            # 如果发现是success，直接返回成功就好了。
            if idempotency_state == "existing_success":
                logger.info(f"[Idempotency] 幂等命中成功记录，直接跳过处理: clue_id={clue_id}, request_id={request_id}")
                from app.routers.clue_webhook import WebhookResponse
                return WebhookResponse(code=0, msg="Idempotent: Already processed successfully")
            
            # 如果没有获取到锁，直接报错，等待重试就好了
            if idempotency_state != "acquired":
                logger.warning(f"[Idempotency] 幂等抢锁失败，当前状态={idempotency_state}，等待重试: clue_id={clue_id}, request_id={request_id}")
                raise LockTimeoutError(f"Idempotent request wait for lock, state={idempotency_state}", clue_id=clue_id)

            # Step 2: 尝试执行业务逻辑
            try:
                result = await func(self, request_obj, *args, **kwargs)
                
                # Step 3: 若执行成果，更新幂等为success
                await crud.mark_call_end_idempotency_success(request_id)
                return result
            
            except Exception as e:
                # --- 4. 业务失败，写回失败状态 ---·
                try:
                    await crud.mark_call_end_idempotency_failed(request_id, str(e))
                except Exception as db_err:
                    logger.error(f"[Idempotency] 保存失败状态异常 (request_id={request_id}): {db_err}")
                raise

        return wrapper
    return decorator