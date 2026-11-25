"""Domain-agnostic sub-state-machine contracts for RSI V3.

Defines the strongly typed contracts between the SubFlowRuntime, the L2
FollowupPolicy state machine snapshot, and both YAML-driven and custom Python
subflow handlers (e.g., qualification screening, vehicle license pre-check,
evaluation submission, etc.).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from app.rsi.schemas import ActionProposal


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SubFlowStateSnapshot(BaseModel):
    """Persisted inside L2 `FollowupPolicy.active_subflow_state`."""

    subflow_id: str = Field(..., min_length=1, max_length=64)
    current_node_id: str = Field(..., min_length=1, max_length=64)
    collected_slots: Dict[str, Any] = Field(default_factory=dict)
    node_histories: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)
    qa_call_stack: List[Dict[str, Any]] = Field(default_factory=list)
    visited_nodes: List[str] = Field(default_factory=list)
    status: Literal["active", "paused", "completed", "cancelled", "escalated"] = "active"
    turn_count: int = Field(default=0, ge=0)
    started_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)
    extra: Dict[str, Any] = Field(default_factory=dict)


class SubFlowRunContext(BaseModel):
    """Execution context handed to a subflow handler for a single turn."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    tenant_id: str
    entity_id: str
    subflow_id: str
    node_id: str
    incoming_text: str = ""
    incoming_channel: str = "wechat"
    media_urls: List[str] = Field(default_factory=list)
    event_payload: Dict[str, Any] = Field(default_factory=dict)
    collected_slots: Dict[str, Any] = Field(default_factory=dict)
    node_history: List[Dict[str, Any]] = Field(default_factory=list)
    visited_nodes: List[str] = Field(default_factory=list)
    qa_call_stack: List[Dict[str, Any]] = Field(default_factory=list)
    l2_state: Dict[str, Any] = Field(default_factory=dict)
    l3_notes: Dict[str, Any] = Field(default_factory=dict)
    compact_context_index: str = ""
    model: Any = None
    first_visit_to_node: bool = False


class StepResult(BaseModel):
    """Output returned by a subflow step back to SubFlowRuntime."""

    status: Literal["in_progress", "completed", "paused", "escalated", "qa_answered"]
    reply_text: str = ""
    next_node_id: Optional[str] = None
    updated_slots: Dict[str, Any] = Field(default_factory=dict)
    conclusion: Optional[Dict[str, Any]] = None
    proposed_actions: List[ActionProposal] = Field(default_factory=list)
    used_fixed_opening: bool = False
    used_llm: bool = True
    qa_jump_back: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BaseSubFlowHandler(ABC):
    """Base interface for declarative YAML and specialized Python subflows."""

    subflow_id: str = ""
    description: str = ""
    initial_node_id: str = "start"
    qa_topics: List[Any] = []

    @abstractmethod
    async def step(self, ctx: SubFlowRunContext) -> StepResult:
        """Execute one conversational/stateful step on the active node."""

    def get_qa_topics(self) -> List[Any]:
        """Return optional global QA topics available across subflow nodes."""
        return list(getattr(self, "qa_topics", []) or [])

    def build_initial_snapshot(
        self,
        *,
        now: Optional[datetime] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> SubFlowStateSnapshot:
        ts = now or _utc_now()
        return SubFlowStateSnapshot(
            subflow_id=self.subflow_id,
            current_node_id=self.initial_node_id,
            collected_slots={},
            node_histories={},
            qa_call_stack=[],
            visited_nodes=[],
            status="active",
            turn_count=0,
            started_at=ts,
            updated_at=ts,
            extra=dict(extra or {}),
        )
