"""Typed analyzer definitions and the model-free execution boundary."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re
import time
from typing import Any, Generic, Protocol, TypeVar

from pydantic import BaseModel

from app.summary.contracts import SummaryInput


T = TypeVar("T", bound=BaseModel)
logger = logging.getLogger(__name__)


class AnalyzerBoundary(Protocol):
    """The runtime-owned boundary that performs one structured model call."""

    async def __call__(
        self,
        *,
        definition: "AnalyzerDefinition[Any]",
        prompt: str,
        summary_input: SummaryInput,
    ) -> Any: ...


@dataclass(frozen=True)
class AnalyzerInvocation:
    """One provider response plus optional provider-reported usage."""

    output: Any
    usage: dict[str, Any] | None = None


@dataclass(frozen=True)
class AnalyzerDefinition(Generic[T]):
    """Static contract for one independently evaluated analyzer."""

    name: str
    result_type: type[T]
    prompt: str
    version: str
    skip_default: T

    @property
    def result_schema(self) -> dict[str, Any]:
        """Structured-output schema supplied to a runtime model adapter."""
        return self.result_type.model_json_schema()

    def render_prompt(self, summary_input: SummaryInput) -> str:
        """Bind deterministic call facts without consulting a process clock."""
        reference_time = summary_input.referenceTime or "not provided"
        transcript = summary_input.conversation.modelText or "(no transcript available)"
        values = {"reference_time": reference_time, "transcript": transcript}
        # Only substitute known placeholders once; user prompts may contain JSON
        # braces, and transcript contents must never be interpreted as a template.
        return re.sub(r"\{(reference_time|transcript)\}", lambda match: values[match[1]], self.prompt)

    def make_skip_result(self) -> T:
        """Return the declared skip value, never an error-recovery value."""
        return self.skip_default.model_copy(deep=True)


class AnalyzerExecutor:
    """Executes definitions through a caller-injected model or agent boundary."""

    def __init__(self, boundary: AnalyzerBoundary) -> None:
        self.boundary = boundary

    async def analyze(
        self, definition: AnalyzerDefinition[T], summary_input: SummaryInput
    ) -> T:
        result, _metadata = await self.analyze_with_metadata(definition, summary_input)
        return result

    async def analyze_with_metadata(
        self, definition: AnalyzerDefinition[T], summary_input: SummaryInput
    ) -> tuple[T, dict[str, Any]]:
        started = time.perf_counter()
        prompt = definition.render_prompt(summary_input)
        logger.info("[Summary] 分析开始: analyzer=%s", definition.name)
        raw_result = await self.boundary(
            definition=definition,
            prompt=prompt,
            summary_input=summary_input,
        )
        usage = None
        if isinstance(raw_result, AnalyzerInvocation):
            usage = raw_result.usage
            raw_result = raw_result.output
        result = definition.result_type.model_validate(raw_result)
        metadata: dict[str, Any] = {
            "version": definition.version,
            "latencyMs": max(0, int((time.perf_counter() - started) * 1000)),
        }
        if usage:
            metadata["usage"] = usage
        logger.info("[Summary] 分析完成: analyzer=%s, elapsed_ms=%s", definition.name, metadata["latencyMs"])
        return result, metadata
