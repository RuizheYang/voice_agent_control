"""Common facts and optional strategy summary orchestration."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from pydantic import BaseModel

from app.summary.analyzers import COMMON_CALL_EVENTS
from app.summary.analyzers.base import AnalyzerDefinition, AnalyzerExecutor
from app.summary.analyzers.strategy_summary import build_analyzer
from app.summary.contracts import SummaryInput


logger = logging.getLogger(__name__)


class AnalysisResults(dict[str, BaseModel]):
    """Structured analyzer values carrying invocation metadata."""

    def __init__(self, *args, metadata=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.metadata = dict(metadata or {})


class SummaryAnalysisError(RuntimeError):
    """Expose completed outputs when an independent analyzer fails.

    The workflow caller owns persistence and retry decisions; no failure is
    converted into a fabricated successful summary.
    """

    def __init__(self, results: AnalysisResults, failures: dict[str, str]):
        self.partial_results = {name: value.model_dump(mode="json") for name, value in results.items()}
        self.metadata = dict(results.metadata)
        self.failures = dict(failures)
        super().__init__(f"summary analysis failed: {', '.join(failures)}")


class SummaryPipeline:
    """Run fixed common facts plus an optional, independent business summary."""

    def __init__(
        self,
        executor: AnalyzerExecutor,
        *,
        common_analyzer: AnalyzerDefinition[Any] = COMMON_CALL_EVENTS,
        summary_prompt: str | None = None,
    ) -> None:
        self.executor = executor
        self.common_analyzer = common_analyzer
        if summary_prompt is not None and not isinstance(summary_prompt, str):
            raise TypeError("summary_prompt must be a string or None")
        summary_prompt = summary_prompt.strip() if summary_prompt is not None else ""
        self.analyzers = [common_analyzer]
        if summary_prompt:
            self.analyzers.append(build_analyzer(summary_prompt))

    async def analyze(self, summary_input: SummaryInput) -> AnalysisResults:
        logger.info("[Summary] 【开始】通话总结: analyzers=%s", list(self.analyzer_versions()))
        outcomes = await asyncio.gather(
            *(self.executor.analyze_with_metadata(analyzer, summary_input) for analyzer in self.analyzers),
            return_exceptions=True,
        )
        results = AnalysisResults()
        failures = {}
        first_failure: Exception | None = None
        for analyzer, outcome in zip(self.analyzers, outcomes):
            if isinstance(outcome, BaseException):
                if not isinstance(outcome, Exception):
                    raise outcome
                failures[analyzer.name] = type(outcome).__name__
                if first_failure is None:
                    first_failure = outcome
                logger.error("[Summary] 【失败】分析器执行失败: analyzer=%s, error_type=%s",
                             analyzer.name, type(outcome).__name__)
                continue
            result, metadata = outcome
            results[analyzer.name] = result
            results.metadata[analyzer.name] = metadata
            logger.info("[Summary] 【完成】分析器执行完成: analyzer=%s, version=%s, latency_ms=%s",
                        analyzer.name, metadata["version"], metadata["latencyMs"])
        if failures:
            # 保留第一路失败的原始异常链，手动运行时可定位模型请求的底层原因。
            raise SummaryAnalysisError(results, failures) from first_failure
        return results

    def analyzer_versions(self) -> dict[str, str]:
        return {analyzer.name: analyzer.version for analyzer in self.analyzers}
