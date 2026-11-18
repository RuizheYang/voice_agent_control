"""从完整话务或独立对话生成总结输入，不读写数据库。"""

from __future__ import annotations

import json
from collections.abc import Mapping

from app.summary.contracts import (
    CallMetadata,
    ConversationMessage,
    ConversationTurn,
    NormalizedConversation,
    SummaryInput,
    CallFacts,
)
from app.summary.facts import classify_result_code, is_voice_assistant, normalize_code

ROLES = {"assistant", "user", "tool", "system"}


class SummaryPreprocessor:
    def __init__(self, version: str = "2026.10") -> None:
        self.version = version

    def normalize(
        self,
        conversation: list[ConversationMessage],
        reference_time: str | None = None,
    ) -> SummaryInput:
        if not isinstance(conversation, list):
            raise TypeError("conversation 必须是包含 role、content 的对话列表")
        turns = []
        for message in conversation:
            if not isinstance(message, dict) or set(message) != {"role", "content"}:
                raise ValueError("每条对话必须且只能包含 role、content")
            if not isinstance(message["role"], str) or message["role"] not in ROLES:
                raise ValueError("role 只支持 assistant、user、tool、system")
            if not isinstance(message["content"], str):
                raise TypeError("content 必须是字符串")
            content = message["content"].strip()
            if content:
                turns.append(ConversationTurn(role=message["role"], content=content))
        if not turns:
            raise ValueError("conversation 至少需要一条非空对话")

        return SummaryInput(
            conversation=NormalizedConversation(
                turns=tuple(turns),
                modelText="\n".join(f"{turn.role}: {turn.content}" for turn in turns),
            ),
            callMetadata=CallMetadata(),
            referenceTime=reference_time,
            normalizationVersion=self.version,
        )

    def normalize_call(self, call_info, reference_time: str) -> tuple[SummaryInput, CallFacts]:
        """解包完整回调；空、缺失和损坏转写必须分别保留。"""
        if hasattr(call_info, "model_dump"):
            call_info = call_info.model_dump(mode="json")
        if not isinstance(call_info, Mapping):
            raise TypeError("call_info 必须是完整通话回调")
        data = call_info.get("data")
        if not isinstance(data, Mapping) or not isinstance(data.get("currentCall"), Mapping):
            raise ValueError("missing_current_call")
        current = data["currentCall"]
        encoded = current.get("aiCallContextList")
        status, turns = "valid", ()
        if encoded is None or encoded == "":
            status = "missing"
        else:
            try:
                events = json.loads(encoded) if isinstance(encoded, str) else encoded
                if not isinstance(events, list):
                    raise ValueError("conversation must be a list")
                turns = tuple(
                    turn for event in events
                    if (turn := self._normalize_call_turn(event)) is not None
                )
            except (ValueError, TypeError):
                status = "invalid"
        facts = CallFacts(
            result_code=normalize_code(current.get("resultCode")),
            outcome=classify_result_code(current.get("resultCode")),
            conversation_status=status,
            has_user_conversation=any(t.role == "user" and t.content for t in turns),
            is_voice_assistant=is_voice_assistant(turns),
        )
        summary_input = SummaryInput(
            conversation=NormalizedConversation(
                turns=turns,
                modelText="\n".join(f"{t.role}: {t.content}" for t in turns if t.content),
            ),
            callMetadata=CallMetadata(facts={
                key: current.get(key)
                for key in ("resultCode", "resultMsg", "hangupPart", "aiCallSeconds")
            }),
            referenceTime=reference_time,
            normalizationVersion=self.version,
        )
        return summary_input, facts

    @staticmethod
    def _normalize_call_turn(event) -> ConversationTurn | None:
        if not isinstance(event, Mapping):
            raise ValueError("invalid_conversation_event")
        role = event.get("role", event.get("sender"))
        role = "assistant" if role == "robot" else role
        if role != "tool" and "content" not in event and "text" not in event:
            raise ValueError("missing_conversation_content")
        content = event.get("content", event.get("text", ""))
        payload = event.get("payload") or {}
        if role not in ROLES or not isinstance(content, str) or not isinstance(payload, Mapping):
            raise ValueError("invalid_conversation_event")
        tool_name = event.get("toolName", event.get("tool_name", payload.get("toolName")))
        if not content.strip() and role != "tool" and not tool_name:
            return None
        return ConversationTurn(
            role=role,
            content=content.strip(),
            timestamp=event.get("timestamp", event.get("startDateTime")),
            interrupted=event.get("interrupted") is True,
            toolName=tool_name,
            toolPayload=dict(payload),
        )
