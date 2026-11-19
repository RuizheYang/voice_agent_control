from fastapi import APIRouter
from pydantic import BaseModel, Field
from functools import wraps
import traceback
import logging
from tortoise.exceptions import IntegrityError

from app.schemas import (
    CallEndRequest,
    StartCallRequest,
    CancelClueRequest,
    WechatNoticeRequest,
)
from app.clue_handler.factory import ClueHandlerFactory
from app.utils.exceptions import StateTransitionError, LockTimeoutError

logger = logging.getLogger(__name__)


class WebhookResponse(BaseModel):
    code: int = Field(0, description="响应码值: 0-成功, 1-状态冲突, 2-锁超时(需重试), 3-系统异常")
    msg: str = Field("success", description="响应信息")


def handle_webhook_exceptions(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except StateTransitionError as e:
            clue_str = f" [clueId: {e.clue_id}]" if e.clue_id else ""
            logger.warning(f"状态流转异常{clue_str}: {e}")
            return WebhookResponse(code=1, msg=f"状态冲突/废弃{clue_str}: {e}")
        except LockTimeoutError as e:
            clue_str = f" [clueId: {e.clue_id}]" if e.clue_id else ""
            logger.error(f"锁定超时{clue_str}: {e}")
            return WebhookResponse(code=2, msg=f"锁等待超时{clue_str}，请稍后重试")
        except IntegrityError as e:
            logger.info(f"入库冲突，触发幂等保护，返回处理成功: {e}")
            return WebhookResponse(code=0, msg="线索处理成功(幂等拦截)")
        except Exception as e:
            logger.error(f"系统内部异常: {e}\n{traceback.format_exc()}")
            return WebhookResponse(code=3, msg="系统内部错误")
    return wrapper


router = APIRouter(
    prefix="/clue",
    tags=["telephony_callback"],
    responses={404: {"description": "Not found"}},
)


@router.post("/call-end/notify", response_model=WebhookResponse, summary="底层外呼网关通话结束挂机回调")
@handle_webhook_exceptions
async def call_end(request: CallEndRequest, sync: bool = False):
    """
    外呼通话结束回调接口：
    - 默认 (sync=False): Event Inbox 持久化入箱秒级返回 ACK，由后台 InboxWakeupWorker 异步唤醒总结与排期；
    - 兼容 (sync=True): 同步阻塞等待 on_call_end 总结排期完成后返回。
    """
    logger.info(f"[/call-end/notify] 接收到外呼结束通知: clue_id={request.data.clueId}, sync={sync}")
    if sync:
        handler = await ClueHandlerFactory.get_handler_by_clue(request.data.clueId)
        await handler.on_call_end(request)
        return WebhookResponse(code=0, msg="Call end processed successfully")

    from app.agents.inbox_worker import enqueue_call_end_inbox
    inbox_res = await enqueue_call_end_inbox(request)
    msg = "Call end already queued (idempotent)" if inbox_res.get("duplicate") else "Call end accepted for async processing"
    return WebhookResponse(code=0, msg=msg)


@router.post("/call-start/notify", response_model=WebhookResponse, summary="底层外呼网关通话开始通知")
@handle_webhook_exceptions
async def call_start_notify(request: StartCallRequest):
    """外呼开始通知：底层外呼网关振铃/接通时触发"""
    logger.info("[/call-start/notify] 接收到外呼开始通知: clue_id=%r, call_id=%r, provider_call_id=%r, batch_id=%r",
                request.clueId, request.callId, request.providerCallId, request.batchId)
    handler = await ClueHandlerFactory.get_handler_by_clue(request.clueId)
    await handler.on_call_start(
        request.clueId,
        call_id=request.callId,
        provider_call_id=request.providerCallId,
        batch_id=request.batchId,
    )
    return WebhookResponse(code=0, msg="Call start notice received successfully")


@router.post("/channel/notify", response_model=WebhookResponse, summary="外部触达渠道状态回调 (企微/短信)")
@handle_webhook_exceptions
async def channel_notify(request: WechatNoticeRequest, sync: bool = False):
    """
    外部触达渠道状态回调（如企业微信添加结果 resultCode='3' 成功 / '1','4','434' 失败）：
    - 默认 sync=False: Fast-ACK 持久化写入 Unified Event Inbox (`channel_inbox`) 并唤醒 InboxWakeupWorker（支持与 call_end_inbox 事件合并）；
    - sync=True: 即时同步推进状态机 (PENDING -> ACCEPTED) 并清理过期的 agent_wakeup 与 call 排期任务。
    """
    data_obj = getattr(request, "data", None)
    clue_id = (
        getattr(request, "clueId", None)
        or getattr(request, "clue_id", None)
        or (getattr(data_obj, "custId", None) if data_obj else None)
    )
    if not clue_id:
        return WebhookResponse(code=1, msg="缺少 clueId 或 data.custId")

    if not sync:
        from app.agents.inbox_worker import enqueue_channel_inbox
        await enqueue_channel_inbox(clue_id, request.model_dump(mode="json"))
        return WebhookResponse(code=0, msg="Channel notice queued for async processing")

    handler = await ClueHandlerFactory.get_handler_by_clue(clue_id)
    await handler.on_channel_notify(request)
    return WebhookResponse(code=0, msg="Channel notice processed successfully")


@router.post("/cancel", response_model=WebhookResponse, summary="取消单条线索跟进")
@handle_webhook_exceptions
async def cancel_clue(request: CancelClueRequest):
    """取消线索请求"""
    logger.info(f"[/cancel] 接收到取消线索请求: {request.clueId}")
    handler = await ClueHandlerFactory.get_handler_by_clue(request.clueId)
    result = await handler.on_cancel(request.clueId, closeReason=request.closeReason)
    if result.success:
        return WebhookResponse(code=0, msg=result.reason or "已成功取消线索")
    else:
        return WebhookResponse(code=4, msg=result.reason or "取消线索失败")
