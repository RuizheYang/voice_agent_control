"""轻量三阶段模板；模型与规则可测试，不依赖数据库或排期服务。"""
from abc import ABC, abstractmethod
from typing import Any

from app.summary.analyzers.base import AnalyzerExecutor
from app.summary.contracts import CallFacts, SummaryContext, SummaryInput, SummaryResult


class BaseSummaryScenario(ABC):
    async def run(self, summary_input: SummaryInput, facts: CallFacts, executor: AnalyzerExecutor,
                  summary_prompt: str | None) -> SummaryResult:
        context = self.process_pre(summary_input, facts)
        analysis = await self.process_summary(context, executor, summary_prompt)
        return self.process_post(context, analysis, summary_prompt)

    @abstractmethod
    def process_pre(self, summary_input: SummaryInput, facts: CallFacts) -> SummaryContext: ...

    @abstractmethod
    async def process_summary(self, context: SummaryContext, executor: AnalyzerExecutor,
                              summary_prompt: str | None) -> dict[str, Any]: ...

    @abstractmethod
    def process_post(self, context: SummaryContext, analysis: dict[str, Any],
                     summary_prompt: str | None) -> SummaryResult: ...
