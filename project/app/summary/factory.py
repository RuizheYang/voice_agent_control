"""完整话务的三阶段总结入口；保留独立的低层对话分析工具。"""

from __future__ import annotations

from typing import TYPE_CHECKING
from collections.abc import Mapping
from uuid import uuid4

from pydantic import TypeAdapter

from app.summary.analyzers.base import AnalyzerBoundary, AnalyzerDefinition, AnalyzerExecutor, AnalyzerInvocation
from app.summary.contracts import ConversationMessage, SummaryInput, SummaryResult
from app.summary.pipeline import SummaryPipeline
from app.summary.preprocessor import SummaryPreprocessor
from app.utils.time_utils import get_now

if TYPE_CHECKING:
    from app.config import LLMFactoryConfig
    from app.schemas.call_end import CallEndRequest


class SummaryEngine:
    """前处理 → 按需模型分析 → 后处理，不读取数据库或执行排期。"""

    def __init__(
        self,
        model_config: LLMFactoryConfig | None = None,
        *,
        model_boundary: AnalyzerBoundary | None = None,
    ) -> None:
        self._model_config = model_config
        self._runner: _ModelRunner | None = None
        if model_config is not None:
            _validate_model_settings(model_config)
        # 确定性分支无需 SDK 或有效模型配置；首次真正分析时再初始化客户端。
        runner = model_boundary if model_boundary is not None else self._run_model
        self.executor = AnalyzerExecutor(runner)
        self.preprocessor = SummaryPreprocessor()

    async def _run_model(self, **kwargs):
        if self._runner is None:
            self._runner = _ModelRunner(self._model_config)
        return await self._runner(**kwargs)

    async def analyze_call(
        self,
        call_info: CallEndRequest | Mapping,
        *,
        summary_type: str | None = "single_call",
        summary_prompt: str | None = None,
        reference_time: str | None = None,
    ) -> SummaryResult:
        """完整回调 → 前处理 → 按需模型分析 → 纯业务后处理。"""
        from app.summary.registry import SUMMARY_SCENARIOS, normalize_summary_type

        summary_type = normalize_summary_type(summary_type)
        if summary_prompt is not None and not isinstance(summary_prompt, str):
            raise TypeError("summary_prompt must be a string or None")
        summary_prompt = (summary_prompt or "").strip() or None
        reference_time = reference_time or get_now().isoformat()
        summary_input, facts = self.preprocessor.normalize_call(call_info, reference_time)
        return await SUMMARY_SCENARIOS[summary_type]().run(
            summary_input, facts, self.executor, summary_prompt,
        )

    async def analyze_conversation(
        self,
        conversation: list[ConversationMessage],
        reference_time: str | None = None,
        *,
        summary_prompt: str | None = None,
        summary_type: str | None = None,
    ) -> dict[str, dict]:
        """总结对话列表，每条消息只包含 role、content。

        reference_time：解释“明天、半小时后”等预约时间；重试时沿用本轮固定时间。
        summary_prompt：策略快照中的额外业务要求；为空时只运行通用总结。
        summary_type：业务线总结套件类型标识。
        返回 common_call_events，以及有额外要求时的 strategy_summary。
        一路失败时抛出 SummaryAnalysisError，其 partial_results 保留另一路成功结果。
        """
        if reference_time is None:
            reference_time = get_now().isoformat()
        summary_input = self.preprocessor.normalize(conversation, reference_time)
        pipeline = SummaryPipeline(self.executor, summary_prompt=summary_prompt)
        results = await pipeline.analyze(summary_input)
        return {name: result.model_dump(mode="json") for name, result in results.items()}

    async def analyze_early_media_text(
        self,
        text: str,
        reference_time: str | None = None,
    ) -> dict[str, dict]:
        """早媒体只做通用总结，复用同一个对话入口。"""
        return await self.analyze_conversation(
            [{"role": "assistant", "content": text}],
            reference_time,
        )


def _validate_model_settings(model_config):
    if not model_config.base_url.startswith(("http://", "https://")):
        raise ValueError("总结模型 base_url 必须是 HTTP(S) 地址")
    if not model_config.model_name.strip():
        raise ValueError("总结模型 model_name 不能为空")
    if model_config.model_type != "sglang_openai":
        raise ValueError("总结模型 model_type 必须是 sglang_openai")


class _ModelRunner:
    """模型配置、连接复用和单次请求；业务调用方不需要操作这个类。"""

    def __init__(self, model_config: LLMFactoryConfig | None = None) -> None:
        if model_config is None:
            from app.config import config

            model_config = config.llm_factory_config
        _validate_model_settings(model_config)

        try:
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider
        except ModuleNotFoundError as error:
            raise RuntimeError("总结模型依赖未安装，请先安装项目 requirements.txt") from error

        self.config = model_config
        provider = OpenAIProvider(base_url=model_config.base_url, api_key=model_config.api_key)
        self.model = OpenAIChatModel(model_config.model_name, provider=provider)

    async def __call__(
        self,
        *,
        definition: AnalyzerDefinition,
        prompt: str,
        summary_input: SummaryInput,
    ) -> AnalyzerInvocation:
        from pydantic_ai import Agent

        # 模型连接可复用，每路总结各自创建 Agent，避免混入其他通话的上下文。
        agent = Agent(model=self.model, output_type=definition.result_type, defer_model_check=True)
        result = await agent.run(
            prompt,
            model_settings={
                "extra_body": dict(self.config.extra_body),
                "extra_headers": {
                    **self.config.headers,
                    "RequestId": uuid4().hex,
                    "TransId": uuid4().hex,
                },
            },
        )
        # 当前 SDK 的 usage 是属性；先读取一次，兼容旧版的方法形式。
        usage: object = result.usage
        if callable(usage):
            usage = usage()
        return AnalyzerInvocation(
            output=result.output,
            usage=TypeAdapter(type(usage)).dump_python(usage, mode="json") if usage is not None else None,
        )


_ENGINE: SummaryEngine | None = None


def get_summary_engine(model_config: LLMFactoryConfig | None = None) -> SummaryEngine:
    """每个进程复用一个总结引擎。"""
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = SummaryEngine(model_config)
    return _ENGINE


async def main() -> None:
    import json

    # 修改完整回调中的结束码和对话，即可验证不同通话场景。
    conversation: list[ConversationMessage] = [
        {"role": "assistant", "content": "您好，请问现在方便了解一下我们的服务吗？"},
        {"role": "user", "content": "现在不方便，明天下午三点再联系我吧。"},
        {"role": "assistant", "content": "好的，明天下午三点再联系您。"},
        {"role": "user", "content": "可以，谢谢。"},
    ]
    # 使用固定时间，方便反复验证“明天”等相对时间的解析结果。
    reference_time = "2026-10-19T10:00:00+08:00"
    call_info = {"data": {"currentCall": {
        "resultCode": "00000",
        "aiCallContextList": json.dumps(conversation, ensure_ascii=False),
    }}}
    result = await get_summary_engine().analyze_call(
        call_info,
        summary_type="single_call",
        reference_time=reference_time,
        summary_prompt="概括客户需求，并提取关键词",
    )
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    import asyncio
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(main())
