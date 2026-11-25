"""RSI V3 core domain ORM models (entity-centric, multi-tenant, domain-agnostic)."""
from __future__ import annotations

from tortoise import fields
from tortoise.models import Model


class FollowupEntity(Model):
    """Unified customer follow-up entity (canonical root per tenant_id + entity_id)."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default", db_index=True)
    entity_id = fields.CharField(max_length=128, db_index=True)
    policy_id = fields.CharField(max_length=128, default="default", db_index=True)
    status = fields.CharField(
        max_length=32,
        default="active",
        db_index=True,
        description="active / paused / converted / closed / blacklisted / merged",
    )
    merged_into_entity_id = fields.CharField(max_length=128, null=True, db_index=True)
    inbound_msg_seq = fields.BigIntField(default=0)
    state_version = fields.IntField(default=0)
    last_event_at = fields.DatetimeField(null=True)
    create_time = fields.DatetimeField(auto_now_add=True)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "rsi_followup_entity"
        unique_together = (("tenant_id", "entity_id"),)


class EntityAlias(Model):
    """Cross-channel external identifier mapping (alias -> canonical entity_id)."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default", db_index=True)
    key_type = fields.CharField(max_length=64)
    key_value = fields.CharField(max_length=256)
    entity_id = fields.CharField(max_length=128, db_index=True)
    create_time = fields.DatetimeField(auto_now_add=True)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "rsi_entity_alias"
        unique_together = (("tenant_id", "key_type", "key_value"),)


class EntityEventLog(Model):
    """L1 immutable append-only fact ledger (idempotent per tenant_id + event_id)."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default", db_index=True)
    event_id = fields.CharField(max_length=128)
    entity_id = fields.CharField(max_length=128, db_index=True)
    schema_version = fields.CharField(max_length=16, default="1.0")
    event_type = fields.CharField(max_length=128, db_index=True)
    category = fields.CharField(max_length=64, db_index=True)
    occurred_at = fields.DatetimeField(db_index=True)
    received_at = fields.DatetimeField(auto_now_add=True)
    source = fields.CharField(max_length=128, default="external")
    actor_type = fields.CharField(max_length=32, default="system")
    actor_id = fields.CharField(max_length=128, null=True)
    correlation_id = fields.CharField(max_length=128, null=True, db_index=True)
    causation_id = fields.CharField(max_length=128, null=True)
    policy_id = fields.CharField(max_length=128, null=True)
    summary = fields.TextField(null=True)
    facts = fields.JSONField(default=dict)
    extra = fields.JSONField(default=dict)
    entity_keys = fields.JSONField(default=list)
    ingest_status = fields.CharField(max_length=32, default="ingested", db_index=True)

    class Meta:
        table = "rsi_entity_event_log"
        unique_together = (("tenant_id", "event_id"),)


class EntityMemoryState(Model):
    """L2 deterministic typed state projection derived strictly from L1 events."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default")
    entity_id = fields.CharField(max_length=128)
    hard_boundaries = fields.JSONField(default=dict)
    commitments = fields.JSONField(default=list)
    lifecycle = fields.JSONField(default=dict)
    ownership = fields.JSONField(default=dict)
    channel_stats = fields.JSONField(default=dict)
    slot_watermarks = fields.JSONField(default=dict)
    last_event_id = fields.CharField(max_length=128, null=True)
    projection_version = fields.IntField(default=0)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "rsi_entity_memory_state"
        unique_together = (("tenant_id", "entity_id"),)


class EntityWorkingMemory(Model):
    """L3 semantic working memory compressed for LLM context (never overrides L2)."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default")
    entity_id = fields.CharField(max_length=128)
    customer_profile_notes = fields.JSONField(default=dict)
    rolling_digest = fields.JSONField(default=list)
    active_guidance = fields.TextField(default="")
    last_dreamed_at = fields.DatetimeField(null=True)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "rsi_entity_working_memory"
        unique_together = (("tenant_id", "entity_id"),)


class FollowupPolicy(Model):
    """Tenant-scoped declarative follow-up policy package injected by upstream CRM."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default")
    policy_id = fields.CharField(max_length=128)
    name = fields.CharField(max_length=128, default="")
    version = fields.CharField(max_length=32, default="1.0")
    config = fields.JSONField(default=dict)
    is_active = fields.BooleanField(default=True)
    create_time = fields.DatetimeField(auto_now_add=True)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "rsi_followup_policy"
        unique_together = (("tenant_id", "policy_id"),)


class RsiTask(Model):
    """Unified durable wakeup, timer, watchdog, and dreaming task table."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default", db_index=True)
    task_key = fields.CharField(max_length=128)
    task_type = fields.CharField(
        max_length=64,
        db_index=True,
        description="cognitive_wakeup / timer_self_check / timer_commitment / timer_pre_call_notice / timer_watchdog / memory_dream",
    )
    entity_id = fields.CharField(max_length=128, db_index=True)
    priority = fields.IntField(default=5, db_index=True)
    status = fields.CharField(
        max_length=32,
        default="pending",
        db_index=True,
        description="pending / retry / processing / succeeded / cancelled / superseded / dead_letter",
    )
    run_at = fields.DatetimeField(db_index=True)
    payload = fields.JSONField(default=dict)
    result = fields.JSONField(default=dict)
    retry_count = fields.IntField(default=0)
    max_retries = fields.IntField(default=3)
    last_error = fields.TextField(null=True)
    lock_owner = fields.CharField(max_length=128, null=True)
    lock_token = fields.CharField(max_length=64, null=True)
    locked_until = fields.DatetimeField(null=True, db_index=True)
    finished_at = fields.DatetimeField(null=True)
    create_time = fields.DatetimeField(auto_now_add=True)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "rsi_task"
        unique_together = (("tenant_id", "task_key"),)


class ActionOutbox(Model):
    """Global hard-guardrailed action outbox (sole outbound side-effect exit)."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default", db_index=True)
    entity_id = fields.CharField(max_length=128, db_index=True)
    idempotency_key = fields.CharField(max_length=128)
    action_type = fields.CharField(max_length=64, db_index=True)
    channel = fields.CharField(max_length=64, db_index=True)
    adapter_name = fields.CharField(max_length=64, default="crm_passive")
    permission_level = fields.CharField(max_length=16, default="L0")
    status = fields.CharField(
        max_length=32,
        default="proposed",
        db_index=True,
        description="proposed/awaiting_approval/guardrail_blocked/deferred/scheduled/dispatching/succeeded/failed/cancelled",
    )
    priority = fields.IntField(default=5, db_index=True)
    scheduled_at = fields.DatetimeField(db_index=True)
    payload = fields.JSONField(default=dict)
    verdict_rule = fields.CharField(max_length=128, null=True)
    verdict_reason = fields.TextField(null=True)
    correlation_id = fields.CharField(max_length=128, null=True, db_index=True)
    causation_id = fields.CharField(max_length=128, null=True)
    policy_id = fields.CharField(max_length=128, default="default")
    policy_version = fields.CharField(max_length=32, default="1.0")
    approval_status = fields.CharField(max_length=32, null=True)
    approval_reviewer = fields.CharField(max_length=128, null=True)
    approval_comment = fields.TextField(null=True)
    approved_at = fields.DatetimeField(null=True)
    retry_count = fields.IntField(default=0)
    max_retries = fields.IntField(default=3)
    last_error = fields.TextField(null=True)
    dispatched_at = fields.DatetimeField(null=True)
    finished_at = fields.DatetimeField(null=True)
    create_time = fields.DatetimeField(auto_now_add=True)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "rsi_action_outbox"
        unique_together = (("tenant_id", "idempotency_key"),)


class ActionExecutionResult(Model):
    """ActionResultStore persisting guardrail verdicts and adapter receipts for CRM decisioning."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default", db_index=True)
    entity_id = fields.CharField(max_length=128, db_index=True)
    outbox_id = fields.BigIntField(db_index=True)
    channel = fields.CharField(max_length=64, db_index=True)
    adapter_name = fields.CharField(max_length=64)
    action_type = fields.CharField(max_length=64)
    verdict = fields.CharField(
        max_length=32,
        db_index=True,
        description="allowed / deferred / blocked / awaiting_approval",
    )
    verdict_rule = fields.CharField(max_length=128, null=True)
    execution_status = fields.CharField(
        max_length=32,
        db_index=True,
        description="pending_crm / succeeded / failed / skipped / retry",
    )
    provider_ref = fields.CharField(max_length=128, null=True)
    proposal_snapshot = fields.JSONField(default=dict)
    adapter_response = fields.JSONField(default=dict)
    latency_ms = fields.IntField(default=0)
    crm_acked_at = fields.DatetimeField(null=True)
    create_time = fields.DatetimeField(auto_now_add=True)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "rsi_action_execution_result"


class RsiKillSwitch(Model):
    """Persistent global/tenant/channel/policy circuit-breaker pause state."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default", db_index=True)
    scope_type = fields.CharField(
        max_length=32,
        description="global / tenant / channel / policy",
    )
    target_id = fields.CharField(max_length=128)
    paused = fields.BooleanField(default=True)
    reason = fields.CharField(max_length=512, null=True)
    paused_until = fields.DatetimeField(null=True)
    create_time = fields.DatetimeField(auto_now_add=True)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "rsi_kill_switch"
        unique_together = (("tenant_id", "scope_type", "target_id"),)


class RsiTenantAuth(Model):
    """Multi-tenant App authentication credentials and policy scope bindings."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default", db_index=True)
    app_id = fields.CharField(max_length=128)
    app_secret = fields.CharField(max_length=256)
    name = fields.CharField(max_length=128, default="")
    allowed_policies = fields.JSONField(default=list)
    is_active = fields.BooleanField(default=True)
    create_time = fields.DatetimeField(auto_now_add=True)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "rsi_tenant_auth"
        unique_together = (("tenant_id", "app_id"),)


class RsiFeedbackRecord(Model):
    """Attribution outcomes, human script edits, and guardrail blocks for offline RSI."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default", db_index=True)
    entity_id = fields.CharField(max_length=128, db_index=True)
    feedback_type = fields.CharField(max_length=64, db_index=True)
    policy_id = fields.CharField(max_length=128, default="default", db_index=True)
    policy_version = fields.CharField(max_length=32, default="1.0", db_index=True)
    script_version = fields.CharField(max_length=64, default="v1", db_index=True)
    channel = fields.CharField(max_length=64, default="wechat", db_index=True)
    outbox_id = fields.BigIntField(null=True, db_index=True)
    outcome = fields.CharField(max_length=64, null=True, db_index=True)
    value_score = fields.FloatField(null=True)
    original_text = fields.TextField(null=True)
    edited_text = fields.TextField(null=True)
    human_edit_diff = fields.JSONField(default=dict)
    guardrail_rule = fields.CharField(max_length=128, null=True)
    reviewer_id = fields.CharField(max_length=128, null=True)
    metadata = fields.JSONField(default=dict)
    occurred_at = fields.DatetimeField(db_index=True)
    create_time = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "rsi_feedback_record"


class RsiShadowEvalRun(Model):
    """Persisted zero-outbound shadow comparison evaluation between policy versions."""

    id = fields.BigIntField(primary_key=True)
    tenant_id = fields.CharField(max_length=64, default="default", db_index=True)
    eval_id = fields.CharField(max_length=64, unique=True)
    baseline_policy_id = fields.CharField(max_length=128)
    baseline_version = fields.CharField(max_length=32, default="1.0")
    candidate_policy_id = fields.CharField(max_length=128)
    candidate_version = fields.CharField(max_length=32, default="candidate")
    sample_size = fields.IntField(default=0)
    summary = fields.JSONField(default=dict)
    samples = fields.JSONField(default=list)
    create_time = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "rsi_shadow_eval_run"



