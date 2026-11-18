"""Immutable, model-ready facts derived from a raw call session."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, TypedDict

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class ConversationMessage(TypedDict):
    """总结入口的对话消息；调用方负责从话务字段转换为此结构。"""

    role: Literal["assistant", "user", "tool", "system"]
    content: str


class FrozenDict(Mapping[str, Any]):
    """A read-only mapping backed by private recursively frozen storage."""

    __slots__ = ("__data",)

    def __init__(self, items: Mapping[str, Any]) -> None:
        self.__data = dict(items)

    def __getitem__(self, key: str) -> Any:
        return self.__data[key]

    def __iter__(self):
        return iter(self.__data)

    def __len__(self) -> int:
        return len(self.__data)


def deep_freeze(value: Any) -> Any:
    """Recursively freeze JSON-like values held by immutable contracts."""
    if isinstance(value, Mapping):
        return FrozenDict({key: deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(deep_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(deep_freeze(item) for item in value)
    return value


def deep_thaw(value: Any) -> Any:
    """Convert recursively frozen JSON-like values back to plain API data."""
    if isinstance(value, Mapping):
        return {key: deep_thaw(item) for key, item in value.items()}
    if isinstance(value, (tuple, frozenset)):
        return [deep_thaw(item) for item in value]
    return value


def _freeze_fields(model: BaseModel, *field_names: str) -> None:
    for field_name in field_names:
        object.__setattr__(model, field_name, deep_freeze(getattr(model, field_name)))


class ConversationTurn(BaseModel):
    """One preserved transcript event after deterministic cleanup."""

    model_config = ConfigDict(frozen=True)

    role: str
    content: str
    timestamp: Any | None = None
    interrupted: bool = False
    toolName: str | None = None
    toolPayload: dict[str, Any] = Field(default_factory=dict)

    def model_post_init(self, __context: Any) -> None:
        _freeze_fields(self, "timestamp", "toolPayload")

    @field_serializer("timestamp", "toolPayload")
    def _serialize_deeply_frozen_fields(self, value: Any) -> Any:
        return deep_thaw(value)


class NormalizedConversation(BaseModel):
    """Ordered immutable turns and their stable model rendering."""

    model_config = ConfigDict(frozen=True)

    turns: tuple[ConversationTurn, ...] = Field(default_factory=tuple)
    modelText: str = ""


class CallMetadata(BaseModel):
    """Opaque provider facts retained for decisions without model inference."""

    model_config = ConfigDict(frozen=True)

    facts: dict[str, Any] = Field(default_factory=dict)

    def model_post_init(self, __context: Any) -> None:
        _freeze_fields(self, "facts")

    @field_serializer("facts")
    def _serialize_facts(self, value: Any) -> Any:
        return deep_thaw(value)

    def get(self, field_name: str, default: Any = None) -> Any:
        return self.facts.get(field_name, default)

    @property
    def answered(self) -> Any:
        return self.get("answered")

    @property
    def duration(self) -> Any:
        return self.get("duration")

    @property
    def hangupPart(self) -> Any:
        return self.get("hangupPart", self.get("hangup_part"))

    @property
    def platformError(self) -> Any:
        return self.get("platformError", self.get("platform_error"))


class SummaryInput(BaseModel):
    """The complete deterministic input passed to summary analyzers."""

    model_config = ConfigDict(frozen=True)

    conversation: NormalizedConversation
    callMetadata: CallMetadata
    referenceTime: str | None = None
    normalizationVersion: str = ""

    @classmethod
    def empty(cls, reference_time: str) -> "SummaryInput":
        """Build a clock-free empty input for explicit skip paths and tests."""
        return cls(
            conversation=NormalizedConversation(),
            callMetadata=CallMetadata(),
            referenceTime=reference_time,
        )


class SummaryProcessingError(ValueError):
    """可安全写入任务诊断的固定错误码，不携带模型或通话原文。"""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class CallFacts(BaseModel):
    """前处理从完整回调提取的内部事实，调用方无需传入。"""

    model_config = ConfigDict(frozen=True)
    result_code: str | None
    outcome: Literal["connected", "unanswered", "ring_rejected", "phone_limit", "abnormal", "unknown"]
    conversation_status: Literal["valid", "missing", "invalid"]
    has_user_conversation: bool
    is_voice_assistant: bool


class PrecheckDecision(BaseModel):
    """内部模型门控；不属于对外返回结果。"""

    model_config = ConfigDict(frozen=True)
    run_model: bool
    skip_reason: str | None = None
    telephony_result_code: str | None
    has_user_conversation: bool
    is_voice_assistant: bool
    conversation_status: str
    rule_version: str = "single-call-precheck-v1"


class SummaryContext(BaseModel):
    model_config = ConfigDict(frozen=True)
    summary_input: SummaryInput
    facts: CallFacts
    precheck: PrecheckDecision


class SummaryResult(BaseModel):
    """业务判断与补充提取；执行层另行保存实际执行结果。"""

    model_config = ConfigDict(extra="forbid")
    summary_type: str
    summary_version: str
    common_call_events: dict[str, Any] | None = None
    business_result: dict[str, Any] = Field(default_factory=dict)
    prompt_result: dict[str, Any] | None = None
    decision: Literal["reject", "success", "schedule"]
    reason: str
