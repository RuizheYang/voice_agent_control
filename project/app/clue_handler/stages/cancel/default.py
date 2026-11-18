from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Optional

from app.clue_handler.state import ClueState
from app.clue_handler.contracts.base_cancel import BaseCancelHandler, CancelClueResult
from app.dbmodels import crud
from app.observability.events_bus import event_bus, ClueEndEvent

logger = logging.getLogger(__name__)


class DefaultCancelHandler(BaseCancelHandler):
    """
    取消线索 / 超时兜底阶段默认处理器 (DefaultCancelHandler):
    负责将线索置为终态 REJECTED，保存关闭原因，
    清理关联延迟队列任务，并通过 Webhook 同步 ClueEndEvent 事件给上游业务方。
    """

    def __init__(
        self,
        cancel_call_fn: Optional[Callable[[str], Awaitable[Any]]] = None,
    ):
        self.cancel_call_fn = cancel_call_fn

    async def handle(self, clue_id: str, closeReason: Optional[str] = None) -> CancelClueResult:
        logger.info(f"[DefaultCancelHandler] 【开始】处理取消/超时兜底结束线索: clue_id={clue_id}, closeReason={closeReason}")

        # 1. 幂等校验线索状态，终态跳过
        try:
            state = await crud.get_clue_state(clue_id)
            if state in [ClueState.ACCEPTED.value, ClueState.REJECTED.value, "completed", "cancelled", "failed"]:
                logger.info(f"[DefaultCancelHandler] 线索已处于终态 ({state})，无需重复结束: clue_id={clue_id}")
                return CancelClueResult(success=True, reason="线索已处于终态")
        except Exception as e:
            logger.warning(f"[DefaultCancelHandler] 查询线索状态异常: {e}")

        # 若外部配置了取消外呼通知，安全调用 (不阻断主流程)
        if self.cancel_call_fn is not None:
            try:
                await self.cancel_call_fn(clue_id)
            except Exception as e:
                logger.debug(f"[DefaultCancelHandler] 底层通话通知取消异常 (可忽略): {e}")

        reason = closeReason or "线索已取消跟进"

        # 2. 将线索状态更新为终态 REJECTED，并持久化关闭原因
        try:
            await crud.update_clue_state_or_raise(clue_id, ClueState.REJECTED.value)
            await crud.save_close_reason(clue_id, reason)
            logger.info(f"[DefaultCancelHandler] 【完成】线索状态已置为 REJECTED: clue_id={clue_id}, reason={reason}")
        except Exception as e:
            logger.error(f"[DefaultCancelHandler] 更新线索终态异常: {e}", exc_info=True)
            return CancelClueResult(success=False, reason=str(e))

        # 3. 清理 Redis 延迟队列中的遗留任务
        try:
            from app.redis_client.redis_manager import redis_manager
            delayed_queue = redis_manager.get_delayed_queue()
            await delayed_queue.remove_task(f"calling:{clue_id}")
            await delayed_queue.remove_task(f"wait_for_calling:{clue_id}")
            await delayed_queue.remove_task(f"summary_timeout:{clue_id}")
            await delayed_queue.remove_task(f"pending:{clue_id}")
        except Exception as q_err:
            logger.debug(f"[DefaultCancelHandler] 清理延迟任务异常 (可忽略): {q_err}")

        # 4. 触发标准结案事件回推 (Webhook 同步推送 + EventBus 审计)
        try:
            from app.apis.webhook_dispatcher import dispatch_webhook_event
            from app.dbmodels.models import ClueInfo

            clue = await ClueInfo.filter(clue_id=clue_id).first()
            workflow_id = clue.workflow_id if clue else None

            # 4.1 同步推送外部 Webhook (clue_end)
            await dispatch_webhook_event(
                event="clue_end",
                clue_id=clue_id,
                data={
                    "code": 300,
                    "reason": reason,
                    "clue_id": clue_id,
                    "workflow_id": workflow_id,
                    "close_reason": reason,
                }
            )

            # 4.2 发送 ClueEndEvent 入库 MySQL 审计
            await event_bus.emit(ClueEndEvent(
                clue_id=clue_id,
                workflow_id=workflow_id,
                event_data={
                    "code": 300,
                    "reason": reason,
                }
            ))
            logger.info(f"[DefaultCancelHandler] 【事件推送】已成功推送 clue_end 事件: clue_id={clue_id}")
        except Exception as ev_err:
            logger.warning(f"[DefaultCancelHandler] 推送结案事件异常 (已降级): {ev_err}")

        return CancelClueResult(success=True, reason="已成功结束线索并同步事件")


# 历史兼容别名
CancelCallHandler = DefaultCancelHandler
