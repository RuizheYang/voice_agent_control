from __future__ import annotations

import asyncio
from dataclasses import replace
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
from app.strategy.agent_driven.tools import (
    format_conversation_for_prompt,
    snap_to_working_window,
    is_within_working_window,
)
from app.utils.time_utils import get_now, ensure_tz_aware

logger = logging.getLogger(__name__)

_NON_CONNECTED_OUTCOMES = {"no_answer", "busy", "rejected", "voicemail", "invalid_number", "error", "unanswered"}


class AgentDrivenStrategyEngine(BaseStrategyEngine):
    """
    自适应分层 Agent 驱动策略排期引擎 (Adaptive Tiered AgentDrivenStrategyEngine):
    - Tier 0 (物理短路, <1ms, 0 LLM 调用): 未接通/占线/空号/无对话记录时直接走确定性未接追呼规则与护栏，消除 70% 无效 Token 消耗。
    - Tier 1 (Single-Pass 统一推理, 默认): 挂载 ClueSupervisorAgent + Guarded ToolBus，单次调用完成维度提取、工具执行、审批挂起与 4 态决策。
    - Tier 2 (Parallel Map-Reduce): 并发子评估器 + 主控汇总决策 (按卡带 evaluation_mode='map_reduce' 开启)。
    - Tier 3 (PolicyHarness + 5s 容灾降级): 原话证据核验、走廊钳位、工时安全吸附与 1ms 启发式降级。
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
        self._custom_pipeline_injected = (evaluators is not None or synthesizer is not None)
        self.evaluators = evaluators or SubPromptEvaluatorsPipeline()
        self.synthesizer = synthesizer or MasterDecisionSynthesizer(self._config)
        self.harness = harness or PolicyHarness(self._config)
        self.fallback = fallback or HeuristicSafetyFallbackEngine(self._config)

    @property
    def strategy_config(self) -> AgentStrategyConfig:
        return self._config

    def _extract_conversation_turns(self, context: ClueContext) -> List[Dict[str, Any]]:
        """从 ClueContext 提取最新通话有效对话转写记录（过滤空白与内部工具占位串）"""
        records = getattr(context, "call_records", []) or []
        if not records:
            return []

        latest = records[0]
        raw_turns: Any = []
        snapshot = getattr(latest, "config_snapshot", None)
        if isinstance(snapshot, dict):
            raw_context = snapshot.get("aiCallContextList")
            if isinstance(raw_context, list):
                raw_turns = raw_context
            elif isinstance(raw_context, str):
                try:
                    import json
                    raw_turns = json.loads(raw_context)
                except Exception:
                    raw_turns = []
        if not raw_turns and isinstance(latest, dict):
            raw_turns = latest.get("conversation", []) or []

        cleaned: List[Dict[str, Any]] = []
        for item in (raw_turns or []):
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or item.get("content") or "").strip()
            if not text or text.startswith("Call the "):
                continue
            cleaned.append(item)
        return cleaned

    def _extract_telephony_facts(self, context: ClueContext) -> Dict[str, Any]:
        """提取本次外呼的基础物理话务事实"""
        records = getattr(context, "call_records", []) or []
        latest = records[0] if records else None

        answered = False
        duration = 0
        outcome = "unanswered"
        is_voicemail = False

        if latest:
            if isinstance(latest, dict):
                ans = latest.get("answered")
                duration = latest.get("duration") or latest.get("duration_seconds") or 0
                outcome = latest.get("outcome") or ("connected" if ans == 1 else "unanswered")
                answered = (ans == 1)
                cce = latest.get("common_call_events")
            else:
                ans = getattr(latest, "answered", None)
                duration = getattr(latest, "duration_seconds", None) or getattr(latest, "duration", 0)
                outcome = getattr(latest, "outcome", None) or ("connected" if ans == 1 else "unanswered")
                answered = bool(ans == 1)
                cce = getattr(latest, "common_call_events", None)
            if isinstance(cce, dict) and cce.get("is_voicemail") == "yes":
                is_voicemail = True
            if str(outcome).lower() == "voicemail":
                is_voicemail = True

        return {
            "answered": answered and not is_voicemail,
            "answered_str": "已接通" if (answered and not is_voicemail) else "未接通",
            "duration_seconds": duration,
            "telephony_outcome": outcome,
            "is_voicemail": is_voicemail,
        }

    def _should_short_circuit_tier0(
        self,
        telephony_facts: Dict[str, Any],
        turns: List[Dict[str, Any]],
    ) -> bool:
        """Tier 0 物理短路判定：语音信箱、或未接通且无有效客户发言时，零 LLM 调用直接走确定性规则"""
        if telephony_facts.get("is_voicemail"):
            return True
        is_connected = telephony_facts["answered"]
        outcome = str(telephony_facts.get("telephony_outcome") or "").lower()
        if not is_connected and len(turns) == 0:
            return True
        user_turns = [
            t for t in turns
            if str(t.get("role") or t.get("sender") or "user").strip().lower() not in ("robot", "assistant", "system")
        ]
        if not is_connected and len(user_turns) == 0:
            return True
        if outcome in _NON_CONNECTED_OUTCOMES and len(turns) < 2 and not is_connected:
            return True
        return False

    def _build_single_pass_prompt(
        self,
        turns: List[Dict[str, Any]],
        context_data: Dict[str, Any],
        cfg: Optional[AgentStrategyConfig] = None,
    ) -> str:
        """构造 Single-Pass 复合提示词：在单次推理中兼顾内置/声明式维度提取与 ToolBus 调用"""
        active_cfg = cfg or self._config
        conversation_text = format_conversation_for_prompt(turns)
        custom_specs = getattr(active_cfg, "custom_evaluators", []) or []
        custom_guide = ""
        if custom_specs:
            lines = []
            for spec in custom_specs:
                name = spec.get("name") if isinstance(spec, dict) else getattr(spec, "name", "")
                prompt = spec.get("prompt") if isinstance(spec, dict) else getattr(spec, "prompt", "")
                fields = spec.get("output_fields") if isinstance(spec, dict) else getattr(spec, "output_fields", {})
                lines.append(f"- {name}: {prompt} (请将结果写入 custom_evaluator_findings['{name}'], 字段={fields})")
            custom_guide = "\n【业务专属提取维度 (custom_evaluator_findings)】：\n" + "\n".join(lines)

        handoff_targets = context_data.get("allowed_handoff_targets") or []
        handoff_desc = ", ".join(handoff_targets) if handoff_targets else "无 (禁止 handoff)"

        return (
            f"请基于本次外呼事实与对话转写，结合你的业务工具集完成分析与排期决策 (输出 MasterSummaryDecision)：\n"
            f"- 当前系统基准时间: {context_data['reference_time']}\n"
            f"- 线索总呼叫次数: {context_data['total_calls']} (今日: {context_data['today_calls']}), "
            f"连续未接: {context_data['continuous_no_answer']}\n"
            f"- 物理话务结果: {context_data['telephony_outcome']} ({context_data['answered_str']}, "
            f"时长 {context_data['duration_seconds']}s)\n"
            f"- 合法外呼工时窗口: {active_cfg.work_start_time} - {active_cfg.work_end_time}\n"
            f"- 线索结构化记忆索引: {context_data['memory_index']}\n"
            f"- 允许切换下游业务: {handoff_desc}\n"
            f"{custom_guide}\n\n"
            f"【本轮对话记录】：\n{conversation_text}"
        )

    async def _resolve_runtime_config(self, context: ClueContext) -> AgentStrategyConfig:
        """按上下文 workflow_id 与 OutboundStrategy.followup 配额动态装载运行时策略配置"""
        cfg = self._config
        ctx_wf = getattr(context, "workflow_id", None)
        load_pack = bool(ctx_wf and ctx_wf != cfg.workflow_id and not (ctx_wf == "default" and cfg.workflow_id != "default"))
        if load_pack:
            try:
                from app.agents.domain_registry import domain_pack_registry

                pack = await domain_pack_registry.get_pack(ctx_wf)
                cfg = replace(
                    cfg,
                    workflow_id=pack.workflow_id,
                    work_start_time=pack.work_start_time,
                    work_end_time=pack.work_end_time,
                    evaluation_mode=pack.evaluation_mode,
                    max_touches_per_24h=pack.max_touches_per_24h,
                    domain_master_prompt=pack.domain_master_prompt,
                    allowed_handoff_targets=list(pack.allowed_handoff_targets or []),
                    custom_evaluators=list(pack.custom_evaluators or []),
                    enabled_sub_prompts=list(pack.enabled_sub_evaluators or cfg.enabled_sub_prompts),
                )
            except Exception as err:
                logger.debug(f"[AgentDrivenStrategyEngine] 加载卡带 {ctx_wf} 配置降级为默认配置: {err}")

        followup = getattr(context, "strategy_followup", None)
        if isinstance(followup, dict) and followup:
            updates: Dict[str, Any] = {}
            if isinstance(followup.get("maxCalls"), int) and followup["maxCalls"] > 0:
                updates["max_call_num"] = followup["maxCalls"]
            if isinstance(followup.get("maxNoAnswerCalls"), int) and followup["maxNoAnswerCalls"] > 0:
                updates["noanswer_num"] = followup["maxNoAnswerCalls"]
            if isinstance(followup.get("maxFollowupHours"), int) and followup["maxFollowupHours"] > 0:
                updates["call_max_time"] = followup["maxFollowupHours"]
            if isinstance(followup.get("maxCallPerDay"), int) and followup["maxCallPerDay"] > 0:
                updates["max_calls_per_day"] = followup["maxCallPerDay"]
            if updates:
                cfg = replace(cfg, **updates)

        return cfg

    async def calculate(self, context: ClueContext) -> ScheduleResult:
        """
        核心策略排期算法入口 (继承 BaseStrategyEngine 顶层契约)
        """
        clue_id = getattr(context, "clue_id", "unknown")
        total_calls = getattr(context, "total_call_count", 0)
        continuous_no_answer = getattr(context, "continuous_no_answer_count", 0)
        ref_time = ensure_tz_aware(getattr(context, "call_end_time", None) or get_now())
        runtime_cfg = await self._resolve_runtime_config(context)
        runtime_harness = self.harness if runtime_cfg is self._config else PolicyHarness(runtime_cfg)
        runtime_fallback = self.fallback if runtime_cfg is self._config else HeuristicSafetyFallbackEngine(runtime_cfg)
        runtime_synthesizer = self.synthesizer if runtime_cfg is self._config else MasterDecisionSynthesizer(runtime_cfg)

        logger.info(
            f"[AgentDrivenStrategyEngine] 【开始排期计算】clue_id={clue_id}, workflow={runtime_cfg.workflow_id}, "
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
                    appt_time, runtime_cfg.work_start_time, runtime_cfg.work_end_time
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

            if is_within_working_window(ref_time, runtime_cfg.work_start_time, runtime_cfg.work_end_time):
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
                    ref_time, runtime_cfg.work_start_time, runtime_cfg.work_end_time
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
        # 2. Tier 0 物理短路层 (未接通且无对话，0 次 LLM 调用，<1ms)
        # ------------------------------------------------------------------
        turns = self._extract_conversation_turns(context)
        telephony_facts = self._extract_telephony_facts(context)
        is_connected = telephony_facts["answered"]

        if self._should_short_circuit_tier0(telephony_facts, turns):
            logger.info(
                f"[AgentDrivenStrategyEngine] 【Tier 0 物理短路】clue_id={clue_id} 未接通且无对话，"
                f"跳过 6 次 LLM 推理直接走确定性兜底排期"
            )
            return runtime_fallback.generate_fallback_decision(
                context=context,
                is_connected=False,
                error_reason="Tier0物理未接通短路降级(0 LLM调用)",
            )

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
            "working_window": f"{runtime_cfg.work_start_time} - {runtime_cfg.work_end_time}",
            "memory_index": getattr(context, "memory_index", "暂无历史记忆"),
            "domain_master_prompt": getattr(runtime_cfg, "domain_master_prompt", None),
            "allowed_handoff_targets": getattr(runtime_cfg, "allowed_handoff_targets", []),
        }

        # ------------------------------------------------------------------
        # 3. Tier 1 (Single-Pass + ToolBus) / Tier 2 (Map-Reduce) 智能推演 + Tier 3 护栏
        # ------------------------------------------------------------------
        try:
            mode = getattr(runtime_cfg, "evaluation_mode", "single_pass")
            decision: Optional[MasterSummaryDecision] = None

            if mode == "single_pass" and not self._custom_pipeline_injected:
                try:
                    from app.agents.supervisor import clue_supervisor_agent
                    single_prompt = self._build_single_pass_prompt(turns, context_data, runtime_cfg)
                    decision = await clue_supervisor_agent.run_single_pass_postcall(
                        clue_id=clue_id,
                        prompt=single_prompt,
                        workflow_id=getattr(runtime_cfg, "workflow_id", "default"),
                        dry_run=(clue_id == "dry_run_clue"),
                        timeout=runtime_cfg.timeout_seconds,
                        coalesced_events=getattr(context, "coalesced_events", None) or None,
                    )
                except RuntimeError:
                    # 模型未配置时转入 Tier 2/Fallback 统一处理
                    decision = None

            if decision is None:
                # Tier 2: Parallel Map-Reduce (或注入了自定义 evaluators/synthesizer)
                sub_outputs = await self.evaluators.evaluate_all(
                    messages=turns,
                    reference_time=context_data["reference_time"],
                    enabled_types=runtime_cfg.enabled_sub_prompts,
                    custom_evaluators=getattr(runtime_cfg, "custom_evaluators", None),
                )
                decision = await runtime_synthesizer.synthesize(
                    sub_outputs=sub_outputs,
                    context_data=context_data,
                    timeout=runtime_cfg.timeout_seconds,
                )

            # Tier 3: 强护栏 Policy Harness 偏航拦截与安全吸附
            result: ScheduleResult = runtime_harness.evaluate_and_snap(
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
            return runtime_fallback.generate_fallback_decision(
                context=context,
                is_connected=is_connected,
                error_reason=f"Agent推理异常 ({type(err).__name__}) 降级",
            )
