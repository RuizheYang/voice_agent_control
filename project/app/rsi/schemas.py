"""RSI V3 strict Pydantic v2 contracts for unified event ingestion & memory views."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, field_validator, model_validator

EventCategory = Literal[
    "voice",
    "wechat",
    "messaging",
    "content_engagement",
    "private_domain",
    "churn_signal",
    "fulfillment",
    "finance_order",
    "risk_boundary",
    "human_touch",
    "approval",
    "role_handoff",
    "commitment",
    "lifecycle",
    "business_broadcast",
    "action_receipt",
]

ActorType = Literal["customer", "agent", "human_rep", "crm", "system"]


def _normalize_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class EntityKeyRef(BaseModel):
    """Cross-channel external identifier reference belonging to a customer entity."""

    key_type: str = Field(..., min_length=1, max_length=64)
    key_value: str = Field(..., min_length=1, max_length=256)

    @field_validator("key_type", "key_value")
    @classmethod
    def strip_non_empty(cls, v: str) -> str:
        clean = v.strip()
        if not clean:
            raise ValueError("key_type and key_value must not be blank")
        return clean


class RSIEventEnvelope(BaseModel):
    """Unified external/internal event envelope accepted by POST /v1/events."""

    event_id: str = Field(..., min_length=1, max_length=128)
    tenant_id: str = Field(default="default", min_length=1, max_length=64)
    schema_version: str = Field(default="1.0", max_length=16)
    event_type: str = Field(..., min_length=1, max_length=128)
    category: EventCategory
    entity_id: Optional[str] = Field(default=None, max_length=128)
    entity_keys: List[EntityKeyRef] = Field(default_factory=list)
    occurred_at: datetime
    source: str = Field(default="external", min_length=1, max_length=128)
    actor_type: ActorType = "system"
    actor_id: Optional[str] = Field(default=None, max_length=128)
    correlation_id: Optional[str] = Field(default=None, max_length=128)
    causation_id: Optional[str] = Field(default=None, max_length=128)
    policy_id: Optional[str] = Field(default=None, max_length=128)
    summary: Optional[str] = None
    facts: Dict[str, Any] = Field(default_factory=dict)
    extra: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("occurred_at")
    @classmethod
    def ensure_utc_occurred_at(cls, v: datetime) -> datetime:
        return _normalize_utc(v)

    @model_validator(mode="after")
    def validate_entity_locator(self) -> "RSIEventEnvelope":
        if self.entity_id is not None:
            self.entity_id = self.entity_id.strip() or None
        if not self.entity_id and not self.entity_keys:
            raise ValueError("Either entity_id or at least one entity_keys entry is required")
        return self


class RSIEventBatchRequest(BaseModel):
    """Batch event ingestion payload for POST /v1/events/batch."""

    events: List[RSIEventEnvelope] = Field(..., min_length=1, max_length=500)


class EventIngestResult(BaseModel):
    """Per-event ingestion result with resolved canonical entity and versions."""

    event_id: str
    tenant_id: str
    entity_id: str
    duplicate: bool
    created_entity: bool
    state_version: int
    projection_version: int
    inbound_msg_seq: int
    rule_tier: Optional[str] = None
    matched_rule: Optional[str] = None
    scheduled_task_id: Optional[int] = None
    cancelled_task_count: int = 0
    warnings: List[str] = Field(default_factory=list)


class BatchEventIngestResponse(BaseModel):
    """Aggregated response for POST /v1/events/batch."""

    accepted: int
    duplicates: int
    failed: int
    results: List[Dict[str, Any]] = Field(default_factory=list)


class EntityStateResponse(BaseModel):
    """Unified L2+L3 entity state snapshot exposed to CRM and runtime."""

    tenant_id: str
    entity_id: str
    policy_id: str
    status: str
    merged_into_entity_id: Optional[str] = None
    state_version: int
    projection_version: int
    inbound_msg_seq: int
    last_event_at: Optional[datetime] = None
    aliases: List[EntityKeyRef] = Field(default_factory=list)
    hard_boundaries: Dict[str, Any] = Field(default_factory=dict)
    commitments: List[Dict[str, Any]] = Field(default_factory=list)
    lifecycle: Dict[str, Any] = Field(default_factory=dict)
    ownership: Dict[str, Any] = Field(default_factory=dict)
    channel_stats: Dict[str, Any] = Field(default_factory=dict)
    working_memory: Dict[str, Any] = Field(default_factory=dict)


class EntityTimelineItem(BaseModel):
    """Single immutable L1 event ledger record returned in timeline view."""

    id: int
    event_id: str
    event_type: str
    category: str
    occurred_at: datetime
    received_at: Optional[datetime] = None
    source: str
    actor_type: str
    actor_id: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    policy_id: Optional[str] = None
    summary: Optional[str] = None
    facts: Dict[str, Any] = Field(default_factory=dict)
    extra: Dict[str, Any] = Field(default_factory=dict)
    ingest_status: str


class EntityTimelineResponse(BaseModel):
    """Chronological event history for an entity."""

    tenant_id: str
    entity_id: str
    total: int
    items: List[EntityTimelineItem] = Field(default_factory=list)


class EntityReplayResponse(BaseModel):
    """Response returned after deterministic replay of L1 events into L2 state."""

    tenant_id: str
    entity_id: str
    replayed_events: int
    projection_version: int
    state: EntityStateResponse


RuleTier = Literal[
    "L0_SILENT",
    "L1_REFLEX_CANCEL",
    "L1_REFLEX_BLACKLIST",
    "L1_REFLEX_TAKEOVER",
    "L2_COGNITIVE_WAKEUP",
    "L2_PRIORITY_BOOST",
]


class RuleGatewayDecision(BaseModel):
    """Zero-LLM deterministic rule gateway classification and side-effect plan."""

    tier: RuleTier
    matched_rule: str
    cancel_pending_tasks: bool = False
    blacklist_entity: bool = False
    blacklist_reason: Optional[str] = None
    set_takeover_until: Optional[datetime] = None
    schedule_wakeup: bool = False
    wakeup_delay_ms: int = 0
    wakeup_priority: int = 5
    boost_existing_tasks: bool = False
    auto_create_commitment_timer: bool = False
    notes: List[str] = Field(default_factory=list)


class CoalesceRunOutcome(BaseModel):
    """Result of executing a debounced / preemptible cognitive run."""

    status: Literal["committed", "preempted", "discarded_stale"]
    started_msg_seq: int
    latest_msg_seq: int
    processed_event_ids: List[str] = Field(default_factory=list)
    output: Optional[Any] = None


PermissionLevel = Literal["L0", "L1", "L2", "L3"]


class ActionProposal(BaseModel):
    """Structured outbound action intent submitted by Steward/Planner/SubFlow into Global Outbox."""

    entity_id: str = Field(..., min_length=1, max_length=128)
    tenant_id: str = Field(default="default", min_length=1, max_length=64)
    action_type: str = Field(..., min_length=1, max_length=64)
    channel: str = Field(..., min_length=1, max_length=64)
    adapter_name: Optional[str] = Field(default=None, max_length=64)
    permission_level: Optional[PermissionLevel] = None
    priority: int = Field(default=5, ge=1, le=10)
    scheduled_at: Optional[datetime] = None
    idempotency_key: Optional[str] = Field(default=None, max_length=128)
    correlation_id: Optional[str] = Field(default=None, max_length=128)
    causation_id: Optional[str] = Field(default=None, max_length=128)
    policy_id: Optional[str] = Field(default=None, max_length=128)
    payload: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("scheduled_at")
    @classmethod
    def ensure_utc_scheduled_at(cls, v: Optional[datetime]) -> Optional[datetime]:
        return _normalize_utc(v) if v is not None else None


class GuardrailVerdict(BaseModel):
    """Deterministic Outbox guardrail evaluation result."""

    decision: Literal["allowed", "deferred", "blocked", "awaiting_approval"]
    rule_id: str
    reason: str = ""
    deferred_until: Optional[datetime] = None


class AdapterExecutionResult(BaseModel):
    """Standardized execution receipt returned by edge ChannelAdapters."""

    ok: bool
    status: Literal["succeeded", "pending_crm", "failed", "retry"]
    provider_ref: Optional[str] = None
    response: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    latency_ms: int = 0


class OutboxSubmitResponse(BaseModel):
    """Response returned when submitting an ActionProposal through the Global Outbox."""

    outbox_id: int
    tenant_id: str
    entity_id: str
    idempotency_key: str
    duplicate: bool = False
    status: str
    verdict: str
    verdict_rule: Optional[str] = None
    verdict_reason: Optional[str] = None
    scheduled_at: datetime
    execution_result_id: Optional[int] = None
    execution_status: Optional[str] = None
    provider_ref: Optional[str] = None


class ActionResultItem(BaseModel):
    """ActionResultStore item exposed to upstream CRM for decisioning."""

    id: int
    tenant_id: str
    entity_id: str
    outbox_id: int
    channel: str
    adapter_name: str
    action_type: str
    verdict: str
    verdict_rule: Optional[str] = None
    execution_status: str
    provider_ref: Optional[str] = None
    proposal_snapshot: Dict[str, Any] = Field(default_factory=dict)
    adapter_response: Dict[str, Any] = Field(default_factory=dict)
    latency_ms: int = 0
    crm_acked_at: Optional[datetime] = None
    create_time: Optional[datetime] = None


class ActionResultListResponse(BaseModel):
    """Paginated query response from ActionResultStore."""

    tenant_id: str
    total: int
    items: List[ActionResultItem] = Field(default_factory=list)


class ApprovalResolveRequest(BaseModel):
    """Operator approval decision payload."""

    approved: bool
    reviewer: str = Field(default="operator", min_length=1, max_length=128)
    comment: str = ""


class ApprovalResolveResponse(BaseModel):
    """Result after resolving a pending L2 approval outbox item."""

    outbox_id: int
    tenant_id: str
    entity_id: str
    already_resolved: bool = False
    approved: Optional[bool] = None
    status: str
    verdict: Optional[str] = None
    verdict_rule: Optional[str] = None
    execution_status: Optional[str] = None
    provider_ref: Optional[str] = None


class PolicyUpsertRequest(BaseModel):
    """Declarative FollowupPolicy registration/update payload."""

    tenant_id: str = Field(default="default", min_length=1, max_length=64)
    policy_id: str = Field(..., min_length=1, max_length=128)
    name: str = ""
    version: str = "1.0"
    config: Dict[str, Any] = Field(default_factory=dict)
    is_active: bool = True


class KillSwitchRequest(BaseModel):
    """Circuit-breaker pause/resume control payload."""

    tenant_id: str = Field(default="default", min_length=1, max_length=64)
    scope_type: Literal["global", "tenant", "channel", "policy"]
    target_id: str = Field(default="*", min_length=1, max_length=128)
    reason: Optional[str] = Field(default=None, max_length=512)
    paused_until: Optional[datetime] = None

    @field_validator("paused_until")
    @classmethod
    def ensure_utc_paused_until(cls, v: Optional[datetime]) -> Optional[datetime]:
        return _normalize_utc(v) if v is not None else None


class TenantAuthRegisterRequest(BaseModel):
    """Multi-tenant App credential registration payload."""

    tenant_id: str = Field(default="default", min_length=1, max_length=64)
    app_id: str = Field(..., min_length=1, max_length=128)
    app_secret: str = Field(..., min_length=1, max_length=256)
    name: str = ""
    allowed_policies: List[str] = Field(default_factory=list)


