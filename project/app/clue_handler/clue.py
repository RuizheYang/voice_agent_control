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
        """渠道事件通知：取消过期的认知唤醒定时器并推进线索终态或触发重试唤醒"""
        data_obj = getattr(notice, "data", None)
        clue_id = (
            getattr(notice, "clueId", None)
            or getattr(notice, "clue_id", None)
            or (getattr(data_obj, "custId", None) if data_obj else None)
        )
        if not clue_id:
            logger.warning("[Clue-Handler] 收到缺少 clueId/custId 的渠道通知，已忽略")
            return {"handled": False, "reason": "missing_clue_id"}

        async with process_lock(clue_id, "on_channel_notify"):
            from app.dbmodels.models import ExecutionTask
            from app.agents.memory_dreamer import MemoryDreamer
            from app.observability.events_bus import ClueEndEvent

            try:
                cancel_types = ["agent_wakeup", "call"] if getattr(notice, "success", None) is True else ["agent_wakeup"]
                await ExecutionTask.filter(
                    clue_id=clue_id,
                    task_type__in=cancel_types,
                    status__in=["pending", "retry"],
                ).update(status="cancelled", last_error="cancelled_by_channel_event")
            except Exception as err:
                logger.debug(f"[Clue-Handler] 渠道事件清理唤醒/排期任务忽略: {err}")

            try:
                await redis_manager.get_delayed_queue().remove_task(f"pending:{clue_id}")
            except Exception:
                pass

            is_success = getattr(notice, "success", None) is True
            is_failed = getattr(notice, "failed", None) is True
            result_code = getattr(data_obj, "resultCode", None) if data_obj else None
            result_msg = getattr(data_obj, "resultMessage", None) if data_obj else None

            cur_state = await crud.get_clue_state(clue_id)
            if cur_state in (ClueState.ACCEPTED.value, ClueState.REJECTED.value, "completed", "cancelled", "failed"):
                logger.info(f"[Clue-Handler] 线索 {clue_id} 已终结 ({cur_state})，跳过渠道回调")
                return {"handled": True, "outcome": "already_terminal", "state": cur_state}

            if is_success:
                try:
                    await crud.update_clue_state_or_raise(clue_id, ClueState.ACCEPTED.value)
                    await crud.save_close_reason(clue_id, "渠道添加成功(企微/通道回执)")
                except Exception as st_err:
                    logger.warning(f"[Clue-Handler] 渠道成功流转 ACCEPTED 异常: {st_err}")

                try:
                    await MemoryDreamer.curate_and_persist(
                        clue_id,
                        digest_entry=f"[渠道回调] 添加成功 (resultCode={result_code})",
                        progress="accepted_via_channel",
                    )
                except Exception:
                    pass

                try:
                    clue_rec = await crud.get_clue_info(clue_id)
                    await event_bus.emit(ClueEndEvent(
                        clue_id=clue_id,
                        workflow_id=clue_rec.workflow_id if clue_rec else self.workflow_id,
                        event_data={"code": 200, "reason": "渠道添加成功", "resultCode": result_code},
                    ))
                except Exception as ev_err:
                    logger.warning(f"[Clue-Handler] 渠道成功发射 ClueEndEvent 异常: {ev_err}")

                logger.info(f"[Clue-Handler] 【渠道收口】线索 {clue_id} 添加成功 -> ACCEPTED")
                return {"handled": True, "outcome": "accepted"}

            if is_failed:
                # 渠道添加失败时记录记忆并推入统一 Inbox 唤醒 Agent 决策是否补发短信或转电话
                try:
                    await MemoryDreamer.curate_and_persist(
                        clue_id,
                        digest_entry=f"[渠道回调] 添加失败 (code={result_code}, msg={result_msg or ''})",
                        progress="channel_failed_pending_decision",
                    )
                except Exception:
                    pass
                try:
                    import hashlib
                    import uuid
                    from datetime import datetime, timezone
                    from app.agents.inbox_worker import notify_inbox_worker

                    await ExecutionTask.create(
                        task_key=hashlib.sha256(f"wakeup:chfail:{clue_id}:{uuid.uuid4().hex}".encode()).hexdigest(),
                        task_type="agent_wakeup",
                        workflow_id=self.workflow_id,
                        clue_id=clue_id,
                        payload={
                            "checkGoal": f"渠道添加失败(code={result_code}): 评估改发短信或安排电话补呼",
                            "channel_result_code": result_code,
                        },
                        status="pending",
                        run_at=datetime.now(timezone.utc),
                    )
                    notify_inbox_worker()
                except Exception as w_err:
                    logger.debug(f"[Clue-Handler] 渠道失败创建唤醒任务忽略: {w_err}")

            logger.info(f"[Clue-Handler] 接收到通道通知: clue_id={clue_id}, code={result_code}")
            return {"handled": True, "outcome": "notified", "resultCode": result_code}

    async def on_wakeup_timer(
        self,
        clue_id: str,
        check_goal: str = "",
        coalesced_events: Optional[list] = None,
    ) -> bool:
        """处理 Always-On 认知型自唤醒定时器到期事件：合并窗口事件并唤醒 SupervisorAgent"""
        async with process_lock(clue_id, "on_wakeup_timer"):
            merged_events = list(coalesced_events or [])
            logger.info(
                f"[Clue-Handler] 【认知唤醒】clue_id={clue_id}, goal={check_goal!r}, "
                f"coalesced_inbox_events={len(merged_events)}"
            )
            state = await crud.get_clue_state(clue_id)
            if state in (ClueState.ACCEPTED.value, ClueState.REJECTED.value, "completed", "cancelled", "failed"):
                return True
            from app.agents.supervisor import clue_supervisor_agent
            await clue_supervisor_agent.evaluate_wakeup(
                clue_id,
                check_goal=check_goal,
                coalesced_events=merged_events,
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
            if result:
                try:
                    from datetime import datetime, timezone
                    from app.dbmodels.models import ExecutionTask
                    await ExecutionTask.filter(
                        clue_id=clue.clueId,
                        task_type="new_clue",
                        status__in=["pending", "retry"],
                    ).update(
                        status="succeeded",
                        result={"entryStatus": "entered"},
                        finished_at=datetime.now(timezone.utc),
                    )
                except Exception:
                    pass
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

    async def on_call_end(
        self,
        end_call_info: CallEndRequest,
        coalesced_events: Optional[list] = None,
    ) -> bool:
        """外呼挂机结束回调入口：直接交由 PostCallHandler 完成落库、总结分析与排期次呼"""
        clue_id = end_call_info.data.clueId if hasattr(end_call_info, "data") and end_call_info.data else str(end_call_info)
        async with process_lock(clue_id, "on_call_end"):
            logger.info(f"[Clue-Handler] 【开始】处理外呼结束挂机回调: clue_id={clue_id}")
            if coalesced_events:
                result = await self.postcall_handler.handle(end_call_info, coalesced_events=coalesced_events)
            else:
                result = await self.postcall_handler.handle(end_call_info)
            logger.info(f"[Clue-Handler] 【完成】外呼结束业务逻辑处理完毕: clue_id={clue_id}")
            return result
