import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, Any

logger = logging.getLogger(__name__)

from app.strategy import (
    StrategyEngineFactory,
)
from app.clue_handler.contracts import (
    BaseInflowHandler,
    BasePostCallHandler,
    BaseScheduleHandler,
    BaseCancelHandler,
    BaseCancelCallHandler,
    CancelClueResult,
)
from app.clue_handler.stages import (
    DefaultInflowHandler,
    DefaultPostCallHandler,
    PostCallHandler,
    DefaultScheduleHandler,
    ScheduleHandler,
    DefaultCancelHandler,
    CancelCallHandler,
)
from app.config import config
from app.dbmodels import crud
from app.dbmodels.crud import create_call_end_record, create_clue
from app.redis_client.redis_manager import redis_manager
from app.schemas import CallEndRequest, ScheduledTaskRequest
from app.schemas.channel import WechatNoticeRequest
from app.schemas.clue_create import NormalizedClue
from app.utils.exceptions import LockTimeoutError, StateTransitionError
from app.observability.events_bus import event_bus, StateTransferEvent
from .state import ClueState

delayed_queue = redis_manager.get_delayed_queue()


@asynccontextmanager
async def process_lock(clue_id: str, action_name: str):
    """提取的共用分布式处理锁管理器，处理锁超时并抛出自定义异常。"""
    try:
        async with redis_manager.get_lock(f"process:{clue_id}"):
            yield
    except TimeoutError as e:
        logger.info(f"[Clue-Handler] 【获取处理锁超时】{action_name}: {clue_id}")
        raise LockTimeoutError(f"Process lock timeout {action_name} for clue {clue_id}", clue_id=clue_id) from e


class ClueHandler:
    """
    线索核心调度总线 (Facade):
    聚合 4 大子 Handler (NewClueHandler, PostCallHandler, ScheduleHandler, CancelCallHandler)，
    统一封装分布式锁与各生命周期事件入口。
    """

    def __init__(
        self,
        new_clue_handler: BaseInflowHandler,
        postcall_handler: BasePostCallHandler,
        schedule_handler: Optional[BaseScheduleHandler] = None,
        cancel_call_handler: Optional[BaseCancelHandler] = None,
        workflow_id: Optional[str] = "default",
        # 兼容历史属性名
        scheduled_task_handler: Optional[BaseScheduleHandler] = None,
        cancel_clue_handler: Optional[BaseCancelHandler] = None,
        channel_notification_handler: Optional[Any] = None,
    ):
        self.new_clue_handler = new_clue_handler
        self.postcall_handler = postcall_handler
        self.schedule_handler = schedule_handler or scheduled_task_handler
        self.cancel_call_handler = cancel_call_handler or cancel_clue_handler
        self.workflow_id = workflow_id
        self.channel_notification_handler = channel_notification_handler

        # 兼容历史属性别名访问
        self.scheduled_task_handler = self.schedule_handler
        self.cancel_clue_handler = self.cancel_call_handler

    async def on_call_start(
        self,
        clue_id: str,
        call_id: Optional[str] = None,
        provider_call_id: Optional[str] = None,
        batch_id: Optional[str] = None,
    ):
        """话务振铃/接通事件通知"""
        async with process_lock(clue_id, "on_call_start"):
            logger.info(f"[Clue-Handler] 【开始】处理呼叫中信息: {clue_id}")

            try:
                clue_state = await crud.get_clue_state(clue_id)
            except Exception as e:
                logger.error(f"[Clue-Handler] on_call_start 获取线索状态失败: {clue_id}, 错误: {e}", exc_info=True)
                return

            if clue_state == ClueState.CALLING.value:
                logger.info(f"[Clue-Handler] 线索 {clue_id} 已处于 CALLING 状态，跳过重复通知")
                return

            managed_start = None
            clue_record = await crud.get_clue_info(clue_id)
            from app.dbmodels.models import ClueInfo
            if isinstance(clue_record, ClueInfo):
                from app.call_schedule.callbacks import register_call_start
                managed_start = await register_call_start(clue_id, call_id, provider_call_id, batch_id)
                if managed_start is False:
                    return

            await redis_manager.get_delayed_queue().remove_task(f"wait_for_calling:{clue_id}")
            logger.info(f"[Clue-Handler] 线索 {clue_id} 移除 wait_for_calling 定时任务成功")

            await redis_manager.get_delayed_queue().add_task(f"calling:{clue_id}", 1200)
            if managed_start is None:
                await crud.update_clue_state_or_raise(clue_id, ClueState.CALLING.value)
                await crud.increment_total_call_count(clue_id)
            else:
                await event_bus.emit(StateTransferEvent(
                    clue_id=clue_id,
                    event_data={'from': clue_state, 'to': ClueState.CALLING.value, 'reason': 'call_start_ring_success', 'success': True}
                ))

            # 触发标准事件回推：call_started
            try:
                import time
                from app.apis.webhook_dispatcher import dispatch_webhook_event
                clue_record = await crud.get_clue_info(clue_id)
                agent_id = clue_record.agent_id if clue_record else "default"
                strategy_id = clue_record.strategy_id if clue_record else "default"
                await dispatch_webhook_event(
                    event="call_started",
                    clue_id=clue_id,
                    data={
                        "start_timestamp": int(time.time() * 1000),
                        "agent_id": agent_id,
                        "followup_strategy_id": strategy_id,
                    }
                )
            except Exception as ev_err:
                logger.warning(f"[Clue-Handler] 触发 call_started 事件推送异常 (已降级): {ev_err}")

            logger.info(f"[Clue-Handler] 【完成】已将线索 {clue_id} 状态设置为呼叫中, 并外呼次数加一")

    async def on_cancel(self, clue_id: str, closeReason: Optional[str] = None) -> CancelClueResult:
        """取消线索跟进入口"""
        async with process_lock(clue_id, "on_cancel"):
            logger.info(f"[Clue-Handler] 【开始】处理取消线索请求: {clue_id}")
            result = await self.cancel_call_handler.handle(clue_id, closeReason)
            logger.info(f"[Clue-Handler] 【完成】取消线索处理完毕: {clue_id}, 成功: {result.success}, 原因: {result.reason}")
            return result

    async def on_channel_notify(self, notice: WechatNoticeRequest):
        """渠道事件通知：取消过期的认知唤醒定时器并缓冲合并事件"""
        clue_id = getattr(notice, "clueId", None) or getattr(notice, "clue_id", None)
        if clue_id:
            try:
                from app.dbmodels.models import ExecutionTask
                await ExecutionTask.filter(
                    clue_id=clue_id,
                    task_type="agent_wakeup",
                    status__in=["pending", "retry"],
                ).update(status="cancelled", last_error="cancelled_by_channel_event")
                await self.enqueue_inbox_event(clue_id, {"type": "channel_notify", "payload": str(notice)})
            except Exception as err:
                logger.debug(f"[Clue-Handler] 渠道事件清理唤醒定时器忽略: {err}")
        logger.info(f"[Clue-Handler] 接收到通道通知: clue_id={clue_id}")

    @staticmethod
    async def enqueue_inbox_event_static(clue_id: str, event_payload: dict) -> None:
        """Event Inbox 缓冲写入：避免并发事件撞锁丢失"""
        try:
            import json
            client = await redis_manager._get_client()
            if client is not None:
                await client.rpush(f"clue_inbox:{clue_id}", json.dumps(event_payload, ensure_ascii=False))
        except Exception:
            pass

    async def enqueue_inbox_event(self, clue_id: str, event_payload: dict) -> None:
        """Event Inbox 缓冲写入实例方法"""
        await self.enqueue_inbox_event_static(clue_id, event_payload)

    async def drain_inbox_events(self, clue_id: str) -> list:
        """单 Worker 唤醒时批量取出并合并待处理事件"""
        events = []
        try:
            import json
            client = await redis_manager._get_client()
            if client is not None:
                key = f"clue_inbox:{clue_id}"
                while True:
                    raw = await client.lpop(key)
                    if not raw:
                        break
                    events.append(json.loads(raw))
        except Exception:
            pass
        return events

    async def on_wakeup_timer(self, clue_id: str, check_goal: str = "") -> bool:
        """处理 Always-On 认知型自唤醒定时器到期事件"""
        async with process_lock(clue_id, "on_wakeup_timer"):
            inbox_events = await self.drain_inbox_events(clue_id)
            logger.info(
                f"[Clue-Handler] 【认知唤醒】clue_id={clue_id}, goal={check_goal!r}, "
                f"coalesced_inbox_events={len(inbox_events)}"
            )
            state = await crud.get_clue_state(clue_id)
            if state in (ClueState.ACCEPTED.value, ClueState.REJECTED.value, "completed", "cancelled", "failed"):
                return True
            from app.agents.memory_dreamer import MemoryDreamer
            await MemoryDreamer.curate_and_persist(
                clue_id,
                digest_entry=f"[唤醒自检] {check_goal} (合并事件 {len(inbox_events)} 条)",
                progress="wakeup_checked",
            )
            return True

    async def on_clue_pushed(self, clue: NormalizedClue):
        """先入库，再发布线索消息，避免写库失败留下待消费消息。"""
        logger.info(f"[Clue-Handler] 【开始】准备推送并入库新线索: {clue.clueId}")
        clue_record = await create_clue(clue)
        await redis_manager.rpush_list(config.redis_queue_name, clue.model_dump())
        logger.info(f"[Clue-Handler] 【完成】已将新线索推入Redis并入库: {clue.clueId}")
        return clue_record

    async def on_new_clue(self, clue: NormalizedClue) -> bool:
        """新线索直接流入处理入口"""
        async with process_lock(clue.clueId, "on_new_clue"):
            logger.info(f"[Clue-Handler] 【开始】准备处理新线索: {clue.clueId}")
            await create_clue(clue)
            logger.info(f"[Clue-Handler] 【写库】已将新线索保存到数据库: {clue.clueId}")
            result = await self.new_clue_handler.handle(clue)
            logger.info(f"[Clue-Handler] 【完成】新线索业务逻辑处理完毕: {clue.clueId}")
            return result

    async def on_clue_queue(self, clue: NormalizedClue) -> bool:
        """队列提取新线索处理入口 (已入库线索)"""
        async with process_lock(clue.clueId, "on_clue_queue"):
            logger.info(f"[Clue-Handler] 【开始】准备处理队列中提取的线索: {clue.clueId}")
            result = await self.new_clue_handler.handle(clue)
            logger.info(f"[Clue-Handler] 【完成】队列提取线索业务逻辑处理完毕: {clue.clueId}")
            return result

    async def on_scheduled_task(self, scheduled_task_info: ScheduledTaskRequest) -> bool:
        """排期次呼到期呼叫触发入口"""
        clue_id = scheduled_task_info.clueId if hasattr(scheduled_task_info, "clueId") else str(scheduled_task_info)
        async with process_lock(clue_id, "on_scheduled_task"):
            logger.info(f"[Clue-Handler] 【开始】处理定时排期到期任务: clue_id={clue_id}")
            result = await self.schedule_handler.handle(scheduled_task_info)
            logger.info(f"[Clue-Handler] 【完成】定时排期到期任务处理完毕: clue_id={clue_id}")
            return result

    async def on_call_end(self, end_call_info: CallEndRequest) -> bool:
        """外呼挂机结束回调入口：直接交由 PostCallHandler 完成落库、总结分析与排期次呼"""
        clue_id = end_call_info.data.clueId if hasattr(end_call_info, "data") and end_call_info.data else str(end_call_info)
        async with process_lock(clue_id, "on_call_end"):
            logger.info(f"[Clue-Handler] 【开始】处理外呼结束挂机回调: clue_id={clue_id}")
            result = await self.postcall_handler.handle(end_call_info)
            logger.info(f"[Clue-Handler] 【完成】外呼结束业务逻辑处理完毕: clue_id={clue_id}")
            return result
