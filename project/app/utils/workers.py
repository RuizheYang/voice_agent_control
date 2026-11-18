from __future__ import annotations

import asyncio
import logging

from app.config import config
from app.schemas.clue_create import NormalizedClue
from app.utils.exceptions import StateTransitionError, LockTimeoutError
from app.observability.events_bus import event_bus, ClueTimeoutWarningEvent
from app.utils.time_utils import get_now
from app.redis_client.redis_manager import redis_manager
from app.dbmodels.crud import get_clue_state
from app.clue_handler.state import ClueState
from app.clue_handler.factory import ClueHandlerFactory

logger = logging.getLogger("clue_workers")


class TaskDispatcher:
    
    def __init__(self):
        self.handlers = {
            "pending": self.handle_pending,
            "calling": self.handle_calling_grace,
            "schedule": self.handle_schedule,
            "summary_timeout": self.handle_summary_timeout,
            "wait_for_calling": self.handle_wait_for_calling
        }
    
    async def handle_brute(self, clue_id: str, clue_state: ClueState, reason: str | None = None):
        """统一超时兜底处理：上报告警事件并调用统一 on_cancel 将线索置为 REJECTED 终态"""
        now_str = get_now().isoformat()
        timeout_reason = reason or f"{clue_state.value}_timeout"

        try:
            await event_bus.emit(ClueTimeoutWarningEvent(
                clue_id=clue_id,
                event_type=f"{clue_state.value}_timeout_warning",
                event_data={"timeout_trigger_time": now_str, "timeout_reason": timeout_reason}
            ))
        except Exception as bus_err:
            logger.warning(f"[workers] 发射超时告警事件异常: {bus_err}")

        logger.error(f"[workers] 告警: 线索 {clue_id} 处于 {clue_state.value} 状态已超时，触发统一 on_cancel 兜底结束。")
        try:
            handler = await ClueHandlerFactory.get_handler_by_clue(clue_id)
            result = await handler.on_cancel(clue_id, closeReason=timeout_reason)
            logger.info(f"[workers] 成功通过 on_cancel 兜底结束超时线索: {clue_id}, reason={timeout_reason}, result={result}")
        except Exception as e:
            logger.error(f"[workers] 兜底结束线索 {clue_id} 发生异常: {e}", exc_info=True)
    
    async def handle_pending(self, clue_id: str):
        """处理 PENDING 超时线索"""
        try:
            state = await get_clue_state(clue_id)
            if state != ClueState.PENDING.value:
                logger.info(f"[workers] 线索 {clue_id} 当前状态为 {state}，已不在 PENDING 状态，跳过超时兜底")
                return
            await self.handle_brute(clue_id, ClueState.PENDING, reason="pending_timeout")
        except Exception as e:
            logger.error(f"[workers] 处理 PENDING 超时线索 {clue_id} 发生异常: {e}", exc_info=True)
        
    async def handle_calling_grace(self, clue_id: str):
        """处理 CALLING 超时线索（通话超过设定时间未收到挂机回调）"""
        try:
            state = await get_clue_state(clue_id)
            if state != ClueState.CALLING.value:
                logger.info(f"[workers] 线索 {clue_id} 当前状态为 {state}，已不在 CALLING 状态，跳过超时兜底")
                return

            logger.info(f"[workers] 处理 CALLING 超时线索: {clue_id}，触发统一超时兜底")
            await self.handle_brute(clue_id, ClueState.CALLING, reason="calling_timeout")
        except Exception as e:
            logger.error(f"[workers] 处理 CALLING 超时线索 {clue_id} 发生异常: {e}", exc_info=True)
            
    async def handle_schedule(self, clue_id: str):
        """处理 SCHEDULED 超时线索"""
        try:
            from app.dbmodels.models import ExecutionTask
            # 新业务任务由独立worker处理到期、有效期与重试；旧watchdog不能误取消它。
            if await ExecutionTask.filter(task_type="call", clue_id=clue_id).exists():
                logger.info("[workers] 跳过独立呼叫调度线索的旧SCHEDULED超时: clue_id=%r", clue_id)
                return
            if await get_clue_state(clue_id) != ClueState.SCHEDULED.value:
                return
            await self.handle_brute(clue_id, ClueState.SCHEDULED, reason="schedule_timeout")
        except Exception as e:
            logger.error(f"[workers] 处理 SCHEDULED 超时线索 {clue_id} 发生异常: {e}", exc_info=True)
            
    async def handle_wait_for_calling(self, clue_id: str):
        """处理 WAIT_FOR_CALLING 超时线索（首呼延迟入池后未成功启动通话）"""
        try:
            state = await get_clue_state(clue_id)
            if state != ClueState.WAIT_FOR_CALLING.value:
                logger.info(f"[workers] 线索 {clue_id} 当前状态为 {state}，跳过 wait_for_calling 超时兜底")
                return
                
            logger.info(f"[workers] 处理 WAIT_FOR_CALLING 超时线索: {clue_id}，触发统一超时兜底")
            await self.handle_brute(clue_id, ClueState.WAIT_FOR_CALLING, reason="wait_for_calling_timeout")
        except Exception as e:
            logger.error(f"[workers] 处理 WAIT_FOR_CALLING 超时线索 {clue_id} 获取状态失败: {e}", exc_info=True)
        
    async def handle_summary_timeout(self, clue_id: str):
        """处理 SUMMARY 超时线索（总结分析超时未完成）"""
        try:
            state = await get_clue_state(clue_id)
            if state != ClueState.SUMMARY.value:
                logger.info(f"[workers] 线索 {clue_id} 当前状态为 {state}，已不在 SUMMARY 状态，跳过超时兜底")
                return

            logger.info(f"[workers] 处理 SUMMARY 超时线索: {clue_id}，触发统一超时兜底")
            await self.handle_brute(clue_id, ClueState.SUMMARY, reason="summary_timeout")
        except Exception as e:
            logger.error(f"[workers] 处理 SUMMARY 超时线索 {clue_id} 发生异常: {e}", exc_info=True)
            
    async def handle_default(self, clue_id):
        logger.error(f"[workers] 未知的延迟任务类型: clue_id: {clue_id}")
        
    async def dispatch(self, task_type: str, payload: str):
        handler = self.handlers.get(task_type, self.handle_default)
        try:
            await handler(payload)
        except Exception as e:
            logger.error(f"[workers] 处理延迟任务 [{task_type}:{payload}] 发生异常: {e}", exc_info=True)
            
dispatcher = TaskDispatcher()


async def is_inflow_paused() -> bool:
    from app.call_schedule.control import is_scope_paused
    return await is_scope_paused("inflow")


async def is_schedule_paused() -> bool:
    from app.call_schedule.control import is_scope_paused
    return await is_scope_paused("schedule")


async def clue_scheduler_loop():
    """
    主循环：高频轮询 Redis 延迟队列
    多台机器同时运行此循环，由于 Lua 脚本的原子性，不会发生重复消费。
    """
    logger.info("[workers] ZSet 延迟队列消费者已启动...")

    # 获取延迟队列实例
    delayed_queue = redis_manager.get_delayed_queue()

    while True:
        try:
            if await is_schedule_paused():
                await asyncio.sleep(1.0)
                continue

            # 1. 原子性弹出已超时的任务（最多 50 个以防单次执行过长）
            expired_tasks = await delayed_queue.pop_expired_tasks(batch_size=50)

            # 2. 如果队列为空，休眠 1 秒防止 CPU 空转
            if not expired_tasks:
                await asyncio.sleep(1.0)
                continue

            # 3. 拆解任务标识 (例如 "calling:1001" -> "calling", "1001" 或 "summary_timeout:1001")
            tasks_to_run = []
            for task_id in expired_tasks:
                try:
                    task_type, payload = task_id.split(":", 1)
                    tasks_to_run.append(dispatcher.dispatch(task_type, payload))
                except ValueError:
                    logger.error(f"[workers] 无效的任务格式: {task_id}，已被直接丢弃")

            # 4. 并发执行这一批拿到的任务
            if tasks_to_run:
                # return_exceptions=True 确保个别协程严重崩溃时，不影响 asyncio.gather 的运行
                results = await asyncio.gather(*tasks_to_run, return_exceptions=True)
                for res in results:
                    if isinstance(res, Exception):
                        logger.error(f"[workers] 任务执行时发生未捕获的致命异常: {res}", exc_info=res)

        except Exception as e:
            logger.error(f"[workers] 延迟队列消费者主循环发生致命异常: {e}", exc_info=True)
            # 当 Redis 连接断开或发生其他致命错误时，强制休眠 5 秒，防止死循环狂刷日志
            await asyncio.sleep(5.0)

async def clue_consumer_loop():
    """
    线索消费主循环：从 Redis 队列中获取新线索并派发给对应的 handler。
    """
    logger.info("[workers] Redis 线索消费循环已启动...")
    while True:
        try:
            if await is_inflow_paused():
                await asyncio.sleep(1.0)
                continue

            clue = await redis_manager.queue_pop(config.redis_queue_name)

            if clue:
                logger.info(
                    f"[Clue-Handler-消费队列] 从Redis队列中获取到线索: "
                    f"{clue.get('clueId')}"
                )
                try:
                    clue = NormalizedClue.model_validate(clue)
                    handler = await ClueHandlerFactory.get_handler_by_workflow(clue.workflowId or "default")
                    await handler.on_clue_queue(clue)

                except StateTransitionError as state_error:
                    logger.warning(
                        f"[Clue-Handler-消费队列] 线索状态流转被拒绝，丢弃该任务不再重试: "
                        f"{state_error}"
                    )
                    # 被拒绝的状态（已终结/被抢占）不能再丢回队列

                except LockTimeoutError as lock_error:
                    logger.warning(
                        f"[Clue-Handler-消费队列] 获取锁超时，将退回队列队尾重试: "
                        f"{lock_error}"
                    )
                    await redis_manager.rpush_list(config.redis_queue_name, clue.model_dump())
                    await asyncio.sleep(1)

                except Exception as process_error:
                    logger.error(
                        f"[Clue-Handler-消费队列] 处理线索失败，将退回队列队尾重试: "
                        f"{process_error}"
                    )
                    await redis_manager.rpush_list(config.redis_queue_name, clue.model_dump())
                    await asyncio.sleep(1)
            else:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"[Clue-Handler-消费队列] 获取Redis线索时发生错误: {e}")
            await asyncio.sleep(1)
