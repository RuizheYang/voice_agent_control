"""首期业务：未有效接通继续，真人接通后按敏感情绪结束。"""
import logging
import time
from typing import Any

from app.summary.analyzers.common_call_events import ANALYZER
from app.summary.analyzers.strategy_summary import build_extraction_analyzer
from app.summary.analyzers.base import AnalyzerExecutor
from app.summary.contracts import (
    CallFacts,
    PrecheckDecision,
    SummaryContext,
    SummaryInput,
    SummaryProcessingError,
    SummaryResult,
)
from app.summary.scenarios.base import BaseSummaryScenario

logger = logging.getLogger(__name__)

REASONS = {
    "unanswered": "本次未接通",
    "ring_rejected": "本次振铃被拒接",
    "phone_limit": "本次号码受到话务限制",
    "no_user_conversation": "本次没有有效用户发言",
    "voice_assistant": "非真人有效接听",
    "hostility_or_opt_out": "用户敏感情绪或明确拒绝再联系",
    "connected": "真人已接通，无敏感情绪，结束线索",
}


class SingleCallScenario(BaseSummaryScenario):
    version = "1"

    def process_pre(self, summary_input: SummaryInput, facts: CallFacts) -> SummaryContext:
        """使用 normalize_call 派生的内部事实决定是否调用模型。"""
        reason = None
        if facts.outcome in {"unanswered", "ring_rejected", "phone_limit"}:
            reason = facts.outcome
        elif facts.outcome != "connected":
            raise SummaryProcessingError("unsupported_telephony_result")
        elif facts.conversation_status != "valid":
            raise SummaryProcessingError("invalid_connected_conversation")
        elif not facts.has_user_conversation:
            reason = "no_user_conversation"
        elif facts.is_voice_assistant:
            reason = "voice_assistant"
        return SummaryContext(
            summary_input=summary_input,
            facts=facts,
            precheck=PrecheckDecision(
                run_model=reason is None,
                skip_reason=reason,
                telephony_result_code=facts.result_code,
                has_user_conversation=facts.has_user_conversation,
                is_voice_assistant=facts.is_voice_assistant,
                conversation_status=facts.conversation_status,
            ),
        )

    async def process_summary(
        self, context: SummaryContext, executor: AnalyzerExecutor, summary_prompt: str | None,
    ) -> dict[str, Any]:
        if not context.precheck.run_model:
            return {}
        common = await executor.analyze(ANALYZER, context.summary_input)
        analysis = {"common": common.model_dump(mode="json")}
        if summary_prompt and common.is_voicemail != "yes":
            started = time.perf_counter()
            try:
                extraction = await executor.analyze(
                    build_extraction_analyzer(summary_prompt), context.summary_input,
                )
                analysis["prompt"] = extraction.model_dump(mode="json")
            except Exception as error:
                logger.warning(
                    "[Summary] 补充提取失败: error_type=%s, status_code=%s, elapsed_ms=%d",
                    type(error).__name__, getattr(error, "status_code", None),
                    (time.perf_counter() - started) * 1000,
                )
        return analysis

    def process_post(
        self, context: SummaryContext, analysis: dict[str, Any], summary_prompt: str | None,
    ) -> SummaryResult:
        common = analysis.get("common")
        reason = context.precheck.skip_reason
        decision = "schedule"
        if context.precheck.run_model:
            if common["is_voicemail"] == "yes":
                reason = "voice_assistant"
            elif common["is_voicemail"] != "no" or common["customer_hostility_opt_out"] == "unknown":
                raise SummaryProcessingError("unknown_critical_summary_fact")
            elif common["customer_hostility_opt_out"] == "yes":
                decision, reason = "reject", "hostility_or_opt_out"
            else:
                decision, reason = "success", "connected"
        return SummaryResult(
            summary_type="single_call",
            summary_version=self.version,
            common_call_events=common,
            prompt_result=analysis.get("prompt"),
            decision=decision,
            reason=REASONS[reason],
        )
