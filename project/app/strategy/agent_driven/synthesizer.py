from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from app.strategy.agent_driven.models import (
    MasterSummaryDecision,
    StandardBusinessStatus,
    ScheduleActionType,
    AppointmentEvidence,
    AgentStrategyConfig,
)
from app.strategy.agent_driven.prompts import MASTER_SYNTHESIZER_SYSTEM_PROMPT
from app.strategy.agent_driven.tools import snap_to_working_window, calculate_relative_time
from app.config import config
from app.utils.time_utils import get_now, ensure_tz_aware

logger = logging.getLogger(__name__)


class MasterDecisionSynthesizer:
    """
    主控汇总决策器 (Master Decision Synthesizer / Reduce 阶段)
    聚合所有垂直业务 Prompt 的中间事实与话务环境，
    利用 Pydantic-AI 严格收敛输出 4 态强类型 MasterSummaryDecision。
    """

    def __init__(self, agent_config: Optional[AgentStrategyConfig] = None):
        self.config = agent_config or AgentStrategyConfig()

    def _get_model(self):
        try:
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider

            llm_cfg = config.llm_factory_config
            if not llm_cfg or not llm_cfg.base_url:
                return None
            provider = OpenAIProvider(base_url=llm_cfg.base_url, api_key=llm_cfg.api_key or "sk-dummy")
            return OpenAIChatModel(llm_cfg.model_name or "default-model", provider=provider)
        except Exception as e:
            logger.debug(f"[MasterSynthesizer] 初始化模型失败: {e}")
            return None

    async def synthesize(
        self,
        sub_outputs: Dict[str, Any],
        context_data: Dict[str, Any],
        timeout: Optional[float] = None,
    ) -> MasterSummaryDecision:
        """
        主控汇总推演
        :param sub_outputs: 垂直业务场景分析器的中间结论 (intent, objection, appointment, sentiment)
        :param context_data: 物理话务及线索事实 (total_calls, continuous_no_answer, etc.)
        :param timeout: 超时控制 (默认 5.0 秒，方案 A 约束)
        """
        effective_timeout = timeout or self.config.timeout_seconds or 5.0
        ref_time = context_data.get("reference_time") or get_now().strftime("%Y-%m-%d %H:%M:%S")

        standard_keys = {"intent", "objection", "appointment", "sentiment"}
        custom_findings = {}
        for k, v in (sub_outputs or {}).items():
            if k not in standard_keys and v is not None:
                custom_findings[k] = v.model_dump() if hasattr(v, "model_dump") else v

        handoff_targets = context_data.get("allowed_handoff_targets") or getattr(self.config, "allowed_handoff_targets", [])
        target_descs = context_data.get("target_descriptions") or {}
        if handoff_targets:
            handoff_section = ", ".join(
                f"{tid} ({target_descs.get(tid, tid)})" for tid in handoff_targets
            )
        else:
            handoff_section = "无可用切换业务 (禁止 handoff)"

        domain_master_prompt = (
            context_data.get("domain_master_prompt")
            or getattr(self.config, "domain_master_prompt", None)
            or "遵循标准业务转化与客户体验准则。"
        )

        prompt = MASTER_SYNTHESIZER_SYSTEM_PROMPT.format(
            reference_time=ref_time,
            total_calls=context_data.get("total_calls", 0),
            today_calls=context_data.get("today_calls", 0),
            continuous_no_answer=context_data.get("continuous_no_answer", 0),
            telephony_outcome=context_data.get("telephony_outcome", "unknown"),
            answered_str=context_data.get("answered_str", "未接通"),
            duration_seconds=context_data.get("duration_seconds", 0),
            working_window=context_data.get("working_window") or f"{self.config.work_start_time} - {self.config.work_end_time}",
            memory_index=context_data.get("memory_index", "暂无历史记忆"),
            intent_analysis=json.dumps(
                getattr(sub_outputs.get("intent"), "model_dump", lambda: {})(), ensure_ascii=False
            ) if sub_outputs.get("intent") else "无",
            objection_analysis=json.dumps(
                getattr(sub_outputs.get("objection"), "model_dump", lambda: {})(), ensure_ascii=False
            ) if sub_outputs.get("objection") else "无",
            appointment_analysis=json.dumps(
                getattr(sub_outputs.get("appointment"), "model_dump", lambda: {})(), ensure_ascii=False
            ) if sub_outputs.get("appointment") else "无",
            sentiment_analysis=json.dumps(
                getattr(sub_outputs.get("sentiment"), "model_dump", lambda: {})(), ensure_ascii=False
            ) if sub_outputs.get("sentiment") else "无",
            custom_evaluators_analysis=json.dumps(custom_findings, ensure_ascii=False) if custom_findings else "无",
            domain_master_prompt_section=domain_master_prompt,
            handoff_targets_section=handoff_section,
            strategy_prompt_section=self.config.strategy_prompt or "标准销售跟进策略：促成客户转化并防范客户骚扰反感。",
        )

        model = self._get_model()
        if not model:
            logger.warning("[MasterSynthesizer] 无模型可用，抛出异常转入 Fallback")
            raise RuntimeError("No LLM model configured for MasterDecisionSynthesizer")

        from pydantic_ai import Agent
        synthesizer_agent = Agent(model=model, output_type=MasterSummaryDecision, defer_model_check=True)

        logger.info(f"[MasterSynthesizer] 【开始】主控汇总推演 (超时限制: {effective_timeout}s)")
        result = await asyncio.wait_for(
            synthesizer_agent.run(prompt),
            timeout=effective_timeout,
        )
        decision: MasterSummaryDecision = result.output
        logger.info(
            f"[MasterSynthesizer] 【完成】主控推演成功: status={decision.status.value}, "
            f"action={decision.action.value}, next_time={decision.next_call_time}"
        )
        return decision
