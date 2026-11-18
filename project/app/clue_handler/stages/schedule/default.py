from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Optional, Union

from app.apis.telephony import LineType
from app.call_schedule.contracts import ScheduledCallRequest
from app.dbmodels import crud
from app.dbmodels.models import ClueInfo, ExecutionTask
from app.schemas.clue_create import ScheduledTaskRequest
from app.clue_handler.state import ClueState
from app.clue_handler.contracts.base_schedule import BaseScheduleHandler
from app.redis_client.redis_manager import redis_manager

logger = logging.getLogger(__name__)


class DefaultScheduleHandler(BaseScheduleHandler):
    """
    排期次呼到期阶段默认处理器 (DefaultScheduleHandler):
    负责对所有排期次呼到期后的呼叫进行业务流转与统一送号操作：
    1. 校验线索当前状态，若已进入终态则直接忽略跳过。
    2. 根据策略标记智能选择线路类型 (普通座机 line_type=LineType.LANDLINE 或工作手机 line_type=LineType.MOBILE)。
    3. 调用注入的 send_call(request, line_type=...) 发起底层外呼，并记录送号执行结果。
    由内部调度模块在排期到期后自动触发驱动。
    """

    def __init__(
        self,
        send_call_fn: Optional[Callable[..., Awaitable[Any]]] = None,
        default_line_type: LineType = LineType.LANDLINE,
    ):
        self.send_call_fn = send_call_fn
        self.default_line_type = default_line_type

    async def handle(
        self, scheduled_task_info: Union[ScheduledCallRequest, ScheduledTaskRequest, str]
    ) -> bool:
        clue_id = (
            scheduled_task_info
            if isinstance(scheduled_task_info, str)
            else scheduled_task_info.clueId
        )
        logger.info(f"[DefaultScheduleHandler] 【开始】处理排期到期呼叫任务: clue_id={clue_id}")

        # 1. 检查线索状态，如果已经是终态则不再发起呼叫
        try:
            state = await crud.get_clue_state(clue_id)
            if state in [
                ClueState.ACCEPTED.value,
                ClueState.REJECTED.value,
                "completed",
                "cancelled",
                "failed",
            ]:
                logger.info(
                    f"[DefaultScheduleHandler] 【跳过】线索已处于终态 ({state})，跳过到期外呼: clue_id={clue_id}"
                )
                return True
        except Exception as e:
            logger.warning(
                f"[DefaultScheduleHandler] 获取线索状态异常: clue_id={clue_id}, error={e}"
            )

        # 2. 智能判断线路类型 (工作手机 vs 普通座机)
        line_type = self.default_line_type
        try:
            clue_info = await crud.get_clue_info(clue_id)
            if clue_info and isinstance(clue_info.agentData, dict):
                if clue_info.agentData.get("next_call_use_work_phone"):
                    line_type = LineType.MOBILE
        except Exception as e:
            logger.warning(
                f"[DefaultScheduleHandler] 线路类型判断异常 (沿用默认): clue_id={clue_id}, error={e}"
            )

        # 3. 构造或提取 ScheduledCallRequest
        if isinstance(scheduled_task_info, ScheduledCallRequest):
            call_request = scheduled_task_info
        else:
            task = (
                await ExecutionTask.filter(
                    clue_id=clue_id,
                    task_type="call",
                    status__in=["pending", "retry"],
                )
                .order_by("run_at")
                .first()
            )
            if task:
                call_request = ScheduledCallRequest(
                    clueId=clue_id,
                    taskId=task.id,
                    executionToken="direct_trigger",
                )
            else:
                logger.warning(
                    f"[DefaultScheduleHandler] 未找到有效待执行排期任务: clue_id={clue_id}"
                )
                return False

        # 4. 排期到期发起送号前，正式将状态从 SCHEDULED 流转为 WAIT_FOR_CALLING
        try:
            await crud.update_clue_state_or_raise(clue_id, ClueState.WAIT_FOR_CALLING.value)
            logger.info(f"[DefaultScheduleHandler] 【状态流转】线索已更新为 WAIT_FOR_CALLING: clue_id={clue_id}")
            # 同步更新 task 中的 stateVersion，防止底层调度服务判定为 stale_schedule
            if call_request.taskId:
                clue_cur = await ClueInfo.filter(clue_id=clue_id).first()
                task_cur = await ExecutionTask.filter(id=call_request.taskId).first()
                if clue_cur and task_cur and isinstance(task_cur.payload, dict):
                    new_payload = {**task_cur.payload, "stateVersion": clue_cur.state_version}
                    await ExecutionTask.filter(id=call_request.taskId).update(payload=new_payload)
            await redis_manager.get_delayed_queue().add_task(f"wait_for_calling:{clue_id}", 14400)
        except Exception as e:
            logger.warning(
                f"[DefaultScheduleHandler] 状态流转至 WAIT_FOR_CALLING 异常: clue_id={clue_id}, error={e}"
            )

        # 5. 调用注入的统一送号函数 send_call
        try:
            result = await self.send_call_fn(call_request, line_type=line_type)
            if result is None:
                logger.info(
                    "[DefaultScheduleHandler] 【本轮未送号】调度服务已跳过或延期，请核对任务执行结果: "
                    "clue_id=%r, task_id=%s, line_type=%s",
                    clue_id, call_request.taskId, line_type.value,
                )
            else:
                logger.info(
                    "[DefaultScheduleHandler] 【送号返回】clue_id=%r, task_id=%s, line_type=%s, result=%r",
                    clue_id, call_request.taskId, line_type.value, result,
                )
        except Exception as e:
            logger.error(
                f"[DefaultScheduleHandler] 触发送号异常: clue_id={clue_id}, line_type={line_type.value}, error={e}",
                exc_info=True,
            )
            return False

        return True


# 历史兼容别名
ScheduleHandler = DefaultScheduleHandler
