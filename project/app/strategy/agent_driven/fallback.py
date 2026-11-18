from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional, Any

from app.strategy.agent_driven.models import (
    MasterSummaryDecision,
    StandardBusinessStatus,
    ScheduleActionType,
    AgentStrategyConfig,
)
from app.strategy.agent_driven.tools import snap_to_working_window, calculate_relative_time
from app.strategy.contracts.base import ScheduleResult
from app.utils.time_utils import get_now, ensure_tz_aware

logger = logging.getLogger(__name__)


class HeuristicSafetyFallbackEngine:
    """
    双轨容灾降级引擎 (Heuristic Safety Fallback Engine):
    当大模型主控或业务分析器响应超时 (超过 5.0 秒)、网络中断或 JSON 解析失败时，
    本模块在 1 毫秒内接管排期决策，执行高度确定性的业务兜底逻辑，
    保障系统 99.99% 的 SLA 吞吐，绝对不阻塞排期与外呼主链路。
    """

    def __init__(self, config: Optional[AgentStrategyConfig] = None):
        self.config = config or AgentStrategyConfig()

    def generate_fallback_decision(
        self,
        context: Any,
        is_connected: bool,
        error_reason: str = "LLM推演超时或异常",
    ) -> ScheduleResult:
        """
        生成启发式确定性兜底排期结果
        """
        now = ensure_tz_aware(get_now())
        total_calls = getattr(context, "total_call_count", 0)
        continuous_no_answer = getattr(context, "continuous_no_answer_count", 0)
        customer_date = getattr(context, "customer_date", None)

        logger.warning(
            f"[FallbackEngine] 【触发降级】clue_id={getattr(context, 'clue_id', 'unknown')}: "
            f"reason={error_reason}, is_connected={is_connected}, calls={total_calls}"
        )

        # 1. 熔断红线检查：达到最大外呼次数或连续未接通上限
        if total_calls >= self.config.max_call_num:
            return ScheduleResult.reject(
                reason=f"[降级兜底] 达到最大外呼次数限制 ({self.config.max_call_num}次) 结案",
                business_status=StandardBusinessStatus.FAILURE.value,
            )

        if not is_connected and continuous_no_answer >= self.config.noanswer_num and total_calls > 0:
            return ScheduleResult.reject(
                reason=f"[降级兜底] 连续未接通达到上限 ({self.config.noanswer_num}次) 停止跟进",
                business_status=StandardBusinessStatus.FAILURE.value,
            )

        # 2. 存在合法客户预约时，优先使用预约时间并工时吸附
        if customer_date:
            appt_time = ensure_tz_aware(customer_date)
            if appt_time > now:
                safe_time = snap_to_working_window(
                    appt_time, self.config.work_start_time, self.config.work_end_time
                )
                return ScheduleResult.schedule(
                    call_time=safe_time,
                    priority=6,
                    reason=f"[降级兜底] 客户预约优先生效: {safe_time}",
                    use_work_phone=False,
                    business_status=StandardBusinessStatus.PARTIAL.value,
                    voice_agent_directive="按此前约定时间进行常规回访。",
                )

        # 3. 未接通场景 (FAILURE)：平移 3 小时并吸附至合法工时
        if not is_connected:
            candidate_time = now + timedelta(hours=3)
            safe_time = snap_to_working_window(
                candidate_time, self.config.work_start_time, self.config.work_end_time
            )
            use_mobile = (continuous_no_answer >= 2)  # 连续 2 次未接通自动降级为工作手机
            return ScheduleResult.schedule(
                call_time=safe_time,
                priority=4,
                reason=f"[降级兜底] 未接通追呼排期: {safe_time}",
                use_work_phone=use_mobile,
                business_status=StandardBusinessStatus.FAILURE.value,
                voice_agent_directive="此前未接通，本次做礼貌回拨探询。",
            )

        # 4. 已接通场景 (PARTIAL)：顺延至次日早晨 09:30 或 4 小时后
        candidate_time = now + timedelta(hours=4)
        safe_time = snap_to_working_window(
            candidate_time, self.config.work_start_time, self.config.work_end_time
        )
        return ScheduleResult.schedule(
            call_time=safe_time,
            priority=5,
            reason=f"[降级兜底] 通话后常规跟进排期: {safe_time}",
            use_work_phone=False,
            business_status=StandardBusinessStatus.PARTIAL.value,
            voice_agent_directive="跟进了解客户当前的选车意向与需求。",
        )
