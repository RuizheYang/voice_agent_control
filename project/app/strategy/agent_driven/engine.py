from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from app.strategy.contracts.base import BaseStrategyEngine, ScheduleResult
from app.strategy.contracts.context import ClueContext
from app.strategy.agent_driven.models import (
    AgentStrategyConfig,
    MasterSummaryDecision,
    StandardBusinessStatus,
    ScheduleActionType,
)
from app.strategy.agent_driven.evaluators import SubPromptEvaluatorsPipeline
from app.strategy.agent_driven.synthesizer import MasterDecisionSynthesizer
from app.strategy.agent_driven.harness import PolicyHarness
from app.strategy.agent_driven.fallback import HeuristicSafetyFallbackEngine
from app.strategy.agent_driven.tools import snap_to_working_window, is_within_working_window
from app.utils.time_utils import get_now, ensure_tz_aware

logger = logging.getLogger(__name__)


class AgentDrivenStrategyEngine(BaseStrategyEngine):
    """
    完全由 Agent 驱动的智能策略排期引擎 (AgentDrivenStrategyEngine)
    包含：
    1. 垂直业务专家 Prompt 并发思考流水线 (Map 阶段)
    2. 主控汇总决策 Prompt 统揽分析 (Reduce 阶段，输出标准 4 态)
    3. 强护栏约束 Harness (走廊容差约束、原话证据核验、法定工时安全吸附)
    4. 双轨 5s 毫秒级启发式容灾降级 (方案 A)
    """

    def __init__(
        self,
        config: Optional[AgentStrategyConfig] = None,
        evaluators: Optional[SubPromptEvaluatorsPipeline] = None,
        synthesizer: Optional[MasterDecisionSynthesizer] = None,
        harness: Optional[PolicyHarness] = None,
        fallback: Optional[HeuristicSafetyFallbackEngine] = None,
    ):
        self._config = config or AgentStrategyConfig()
        self.evaluators = evaluators or SubPromptEvaluatorsPipeline()
        self.synthesizer = synthesizer or MasterDecisionSynthesizer(self._config)
        self.harness = harness or PolicyHarness(self._config)
        self.fallback = fallback or HeuristicSafetyFallbackEngine(self._config)

    @property
    def strategy_config(self) -> AgentStrategyConfig:
        return self._config

    def _extract_conversation_turns(self, context: ClueContext) -> List[Dict[str, Any]]:
        """从 ClueContext 提取最新通话对话转写记录"""
        records = getattr(context, "call_records", []) or []
        if not records:
            return []

        latest = records[0]
        snapshot = getattr(latest, "config_snapshot", None)
        if isinstance(snapshot, dict):
            raw_context = snapshot.get("aiCallContextList")
            if isinstance(raw_context, list):
                return raw_context
            elif isinstance(raw_context, str):
                try:
                    import json
                    return json.loads(raw_context)
                except Exception:
                    pass

        # 兼容 common_call_events 或其他对话字段
        if isinstance(latest, dict):
            return latest.get("conversation", []) or []
        return []

    def _extract_telephony_facts(self, context: ClueContext) -> Dict[str, Any]:
        """提取本次外呼的基础物理话务事实"""
        records = getattr(context, "call_records", []) or []
        latest = records[0] if records else None

        answered = False
        duration = 0
        outcome = "unanswered"

        if latest:
            if isinstance(latest, dict):
                ans = latest.get("answered")
                duration = latest.get("duration") or latest.get("duration_seconds") or 0
                outcome = latest.get("outcome") or ("connected" if ans == 1 else "unanswered")
                answered = (ans == 1)
            else:
                ans = getattr(latest, "answered", None)
                duration = getattr(latest, "duration_seconds", None) or getattr(latest, "duration", 0)
                outcome = getattr(latest, "outcome", None) or ("connected" if ans == 1 else "unanswered")
                answered = bool(ans == 1)

        return {
            "answered": answered,
            "answered_str": "已接通" if answered else "未接通",
            "duration_seconds": duration,
            "telephony_outcome": outcome,
        }

    async def calculate(self, context: ClueContext) -> ScheduleResult:
        """
        核心策略排期算法入口 (继承 BaseStrategyEngine 顶层契约)
        """
        clue_id = getattr(context, "clue_id", "unknown")
        total_calls = getattr(context, "total_call_count", 0)
        continuous_no_answer = getattr(context, "continuous_no_answer_count", 0)
        ref_time = ensure_tz_aware(getattr(context, "call_end_time", None) or get_now())

        logger.info(
            f"[AgentDrivenStrategyEngine] 【开始排期计算】clue_id={clue_id}, "
            f"已呼={total_calls}次, 连续未接={continuous_no_answer}次, 参考时间={ref_time}"
        )

        # ------------------------------------------------------------------
        # 1. 首呼快速路径 (流入首呼排期)
        # ------------------------------------------------------------------
        if total_calls == 0 and not getattr(context, "call_records", None):
            customer_date = getattr(context, "customer_date", None)
            if customer_date:
                appt_time = ensure_tz_aware(customer_date)
                safe_time = snap_to_working_window(
                    appt_time, self._config.work_start_time, self._config.work_end_time
                )
                logger.info(f"[AgentDrivenStrategyEngine] 首呼客户预约优先生效: {safe_time}")
                return ScheduleResult.schedule(
                    call_time=safe_time,
                    priority=6,
                    reason=f"新线索客户预约优先: {safe_time}",
                    use_work_phone=False,
                    business_status=StandardBusinessStatus.PARTIAL.value,
                    voice_agent_directive="新线索首呼，按约定时间致电客户。",
                )

            # 工作时间内即时首呼，非工作时间顺延至次日开工
            if is_within_working_window(ref_time, self._config.work_start_time, self._config.work_end_time):
                immediate_time = ref_time + timedelta(seconds=10)
                logger.info(f"[AgentDrivenStrategyEngine] 工作时间内新线索即刻外呼: {immediate_time}")
                return ScheduleResult.schedule(
                    call_time=immediate_time,
                    priority=6,
                    reason="工作时间内新线索即时外呼",
                    use_work_phone=False,
                    business_status=StandardBusinessStatus.PARTIAL.value,
                    voice_agent_directive="新线索首呼，开门见山自我介绍并探询意向。",
                )
            else:
                next_morning = snap_to_working_window(
                    ref_time, self._config.work_start_time, self._config.work_end_time
                )
                logger.info(f"[AgentDrivenStrategyEngine] 非工作时间新线索顺延至次日开工: {next_morning}")
                return ScheduleResult.schedule(
                    call_time=next_morning,
                    priority=5,
                    reason="非工作时间新线索顺延至次日开工外呼",
                    use_work_phone=False,
                    business_status=StandardBusinessStatus.PARTIAL.value,
                    voice_agent_directive="新线索顺延外呼，开门见山探询客户购车需求。",
                )

        # ------------------------------------------------------------------
        # 2. 话后追呼与多 Prompt 智能决策 (Post-Call Stage)
        # ------------------------------------------------------------------
        turns = self._extract_conversation_turns(context)
        telephony_facts = self._extract_telephony_facts(context)
        is_connected = telephony_facts["answered"]

        # 构造上下文数据
        context_data = {
            "clue_id": clue_id,
            "reference_time": ref_time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_calls": total_calls,
            "today_calls": getattr(context, "today_call_count", 1),
            "continuous_no_answer": continuous_no_answer,
            "telephony_outcome": telephony_facts["telephony_outcome"],
            "answered_str": telephony_facts["answered_str"],
            "duration_seconds": telephony_facts["duration_seconds"],
            "memory_index": getattr(context, "memory_index", "暂无历史记忆"),
            "domain_master_prompt": getattr(self._config, "domain_master_prompt", None),
            "allowed_handoff_targets": getattr(self._config, "allowed_handoff_targets", []),
        }

        # 尝试走完整 Agent 推理流水线 (带 5.0 秒超时控制，方案 A 约束)
        try:
            # 步骤 A: 垂直业务子场景 Prompt 并发思考 (Map 阶段)
            sub_outputs = await self.evaluators.evaluate_all(
                messages=turns,
                reference_time=context_data["reference_time"],
                enabled_types=self._config.enabled_sub_prompts,
                custom_evaluators=getattr(self._config, "custom_evaluators", None),
            )

            # 步骤 B: 主控汇总决策 Prompt 综合统筹 (Reduce 阶段)
            decision: MasterSummaryDecision = await self.synthesizer.synthesize(
                sub_outputs=sub_outputs,
                context_data=context_data,
                timeout=self._config.timeout_seconds,
            )

            # 步骤 C: 强护栏 Policy Harness 偏航拦截与安全吸附
            result: ScheduleResult = self.harness.evaluate_and_snap(
                decision=decision,
                context=context,
                conversation_turns=turns,
                base_time=ref_time,
            )
            return result

        except Exception as err:
            logger.warning(
                f"[AgentDrivenStrategyEngine] Agent 推理失败或超时，触发方案 A 启发式安全兜底: {err}",
                exc_info=True,
            )
            return self.fallback.generate_fallback_decision(
                context=context,
                is_connected=is_connected,
                error_reason=f"Agent推理异常 ({type(err).__name__}) 降级",
            )
