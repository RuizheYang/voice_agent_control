from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from app.strategy.agent_driven.models import (
    MasterSummaryDecision,
    StandardBusinessStatus,
    ScheduleActionType,
    AgentStrategyConfig,
)
from app.strategy.agent_driven.tools import snap_to_working_window, is_within_working_window
from app.strategy.contracts.base import ScheduleResult
from app.utils.time_utils import get_now, ensure_tz_aware

logger = logging.getLogger(__name__)


@dataclass
class CorridorEnvelope:
    """动态可行走廊区间 [T_min, T_max]"""
    min_time: datetime
    max_time: datetime
    reason: str = ""


class PolicyHarness:
    """
    强护栏约束 Harness 体系 (Policy & Bounding Harness):
    1. 动态可行走廊生成 (Pre-Harness Corridor Envelope)
    2. 原话证据链真伪核验 (Evidence-based Verbatim Grounding)
    3. 确定性几何投影与安全吸附 (Safety Snapping 09:00 - 21:00)
    4. 4 态标准驱动的状态流转与调度收敛
    """

    def __init__(self, config: Optional[AgentStrategyConfig] = None):
        self.config = config or AgentStrategyConfig()

    def calculate_corridor(
        self,
        context: Any,
        base_time: Optional[datetime] = None,
    ) -> CorridorEnvelope:
        """
        计算当前排期所处的动态业务可行走廊
        """
        now = ensure_tz_aware(base_time or get_now())
        cooldown = timedelta(minutes=self.config.min_cooldown_minutes)
        min_time = now + cooldown

        raw_intake = getattr(context, "clue_intake_time", None)
        intake_time = ensure_tz_aware(raw_intake) if raw_intake else now
        max_ttl = intake_time + timedelta(hours=self.config.call_max_time)

        # 常规单次追呼最大不宜超过 48 小时
        standard_max = min(now + timedelta(hours=48), max_ttl)
        if standard_max < min_time:
            standard_max = min_time + timedelta(hours=1)

        return CorridorEnvelope(
            min_time=min_time,
            max_time=standard_max,
            reason=f"合规走廊: 最小冷却 {self.config.min_cooldown_minutes}m, 最大生命周期 {self.config.call_max_time}h",
        )

    def verify_verbatim_evidence(
        self,
        decision: MasterSummaryDecision,
        conversation_turns: List[Any],
    ) -> bool:
        """
        事实证据链溯源核验:
        若 Agent 提议预约时间，检查其原话引用是否存在于真实对话转写中，杜绝时间推理幻觉。
        """
        if not decision.appointment or not decision.appointment.has_appointment:
            return False

        quote = decision.appointment.verbatim_quote
        if not quote or not str(quote).strip():
            logger.warning("[PolicyHarness] 预约缺乏原话逐字引用，证据不成立")
            return False

        quote_clean = str(quote).strip().lower()
        full_text = ""
        for turn in conversation_turns:
            if isinstance(turn, dict):
                content = turn.get("content", "")
            else:
                content = getattr(turn, "content", "")
            full_text += str(content).lower() + " "

        # 检查原话或其中关键片段是否真实存在
        if quote_clean in full_text or any(len(part) >= 4 and part in full_text for part in quote_clean.split()):
            logger.info(f"[PolicyHarness] 预约原话证据链核验通过: '{quote}'")
            return True

        logger.warning(f"[PolicyHarness] 预约原话证据链核验失败 (原话不存在于对话中): '{quote}'")
        return False

    def evaluate_and_snap(
        self,
        decision: MasterSummaryDecision,
        context: Any,
        conversation_turns: List[Any],
        base_time: Optional[datetime] = None,
    ) -> ScheduleResult:
        """
        后置强护栏总评估与偏航校准
        """
        now = ensure_tz_aware(base_time or get_now())
        total_calls = getattr(context, "total_call_count", 0)
        continuous_no_answer = getattr(context, "continuous_no_answer_count", 0)

        # ------------------------------------------------------------------
        # 分支 1: SUCCESS (目标达成 / 正向闭环)
        # ------------------------------------------------------------------
        if decision.status == StandardBusinessStatus.SUCCESS:
            logger.info(f"[PolicyHarness] 判定为 SUCCESS: 目标达成结案")
            return ScheduleResult.reject(
                reason=f"外呼目标达成结案: {decision.reason}",
                voice_agent_directive=decision.voice_agent_directive,
                business_status=StandardBusinessStatus.SUCCESS.value,
                master_decision=decision,
            )

        # ------------------------------------------------------------------
        # 分支 2: REJECTION (明确拒绝 / 负向闭环 / DNC)
        # ------------------------------------------------------------------
        if decision.status == StandardBusinessStatus.REJECTION:
            logger.info(f"[PolicyHarness] 判定为 REJECTION: 客户明确拒绝跟进")
            return ScheduleResult.reject(
                reason=f"客户明确拒绝，停止跟进: {decision.reason}",
                voice_agent_directive=decision.voice_agent_directive,
                business_status=StandardBusinessStatus.REJECTION.value,
                master_decision=decision,
            )

        # ------------------------------------------------------------------
        # 分支 3: 熔断红线检查 (无论 failure 还是 partial)
        # ------------------------------------------------------------------
        if total_calls >= self.config.max_call_num:
            return ScheduleResult.reject(
                reason=f"达到最大外呼次数限制 ({self.config.max_call_num}次) 结案",
                business_status=decision.status.value,
                master_decision=decision,
            )

        if decision.status == StandardBusinessStatus.FAILURE and continuous_no_answer >= self.config.noanswer_num:
            return ScheduleResult.reject(
                reason=f"连续未接通次数 ({continuous_no_answer}) 超出上限 ({self.config.noanswer_num}次) 结案",
                business_status=StandardBusinessStatus.FAILURE.value,
                master_decision=decision,
            )

        # ------------------------------------------------------------------
        # 分支 4: PARTIAL 或 FAILURE 排期推演与走廊校准
        # ------------------------------------------------------------------
        corridor = self.calculate_corridor(context, now)
        has_verified_appointment = self.verify_verbatim_evidence(decision, conversation_turns)

        # 确定目标候选时间
        target_time: datetime
        priority = decision.priority or 5
        use_work_phone = decision.use_work_phone

        if has_verified_appointment and decision.appointment and decision.appointment.parsed_time:
            # 强证据预约优先
            target_time = ensure_tz_aware(decision.appointment.parsed_time)
            if target_time < corridor.min_time:
                target_time = corridor.min_time
            priority = max(priority, 6)
            reason_prefix = f"[预约优先生效] 原话: '{decision.appointment.verbatim_quote}'"
        else:
            # 大盘推演时间
            if decision.next_call_time:
                proposed = ensure_tz_aware(decision.next_call_time)
                # 偏航校准：无证据凭据时，时间必须严格落在可行走廊 [T_min, T_max] 内
                if proposed < corridor.min_time:
                    logger.warning(f"[PolicyHarness] 排期过早偏航 ({proposed})，强制钳位至走廊下限: {corridor.min_time}")
                    target_time = corridor.min_time
                elif proposed > corridor.max_time:
                    logger.warning(f"[PolicyHarness] 排期过迟偏航 ({proposed})，强制钳位至走廊上限: {corridor.max_time}")
                    target_time = corridor.max_time
                else:
                    target_time = proposed
            else:
                # Agent 未给出排期时间，使用走廊默认点
                target_time = corridor.min_time + timedelta(hours=2)

            reason_prefix = f"[{decision.status.value.upper()}跟进] {decision.reason}"

        # ------------------------------------------------------------------
        # 分支 5: 确定性法定工时安全吸附 (Safety Snapping 09:00 - 21:00)
        # ------------------------------------------------------------------
        safe_time = snap_to_working_window(
            target_time, self.config.work_start_time, self.config.work_end_time
        )

        return ScheduleResult.schedule(
            call_time=safe_time,
            priority=priority,
            reason=f"{reason_prefix} (已通过Harness合规校验)",
            use_work_phone=use_work_phone,
            voice_agent_directive=decision.voice_agent_directive,
            business_status=decision.status.value,
            master_decision=decision,
        )

    @staticmethod
    async def pre_dial_eligibility_check(clue_id: str) -> bool:
        """
        拨打前终检门禁 (Pre-Dial Gatekeeper):
        排期任务到期唤醒送号前最后一毫秒执行，
        确认线索当前未进入终态、未加微成功或被人工取消，防止网络时序并发重拨。
        """
        from app.dbmodels import crud
        try:
            state = await crud.get_clue_state(clue_id)
            if state in ("accepted", "rejected", "completed", "cancelled", "failed"):
                logger.info(f"[PreDialGate] 线索 {clue_id} 已处于终态 ({state})，拦截送号")
                return False
            return True
        except Exception as e:
            logger.warning(f"[PreDialGate] 终检门禁异常 (默认放行): {e}")
            return True
