"""Offline RSI Feedback Ingestion, Human-Edit Diff Analysis, and Block Sync."""

from __future__ import annotations

import difflib
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from app.rsi.guardrails.time_window import to_utc
from app.rsi.identity import resolve_canonical_entity
from app.rsi.models import ActionOutbox, RsiFeedbackRecord

FeedbackType = Literal[
    "conversion_outcome",
    "human_edit",
    "call_connect",
    "message_reply",
    "guardrail_block",
    "satisfaction_rating",
]

_SOFTEN_WORDS = (
    "保证",
    "绝对",
    "马上",
    "立刻",
    "稳过",
    "最低",
    "必须",
    "百分百",
    "放款",
)
_POLITE_CUES = ("方便", "打扰", "请问", "随时", "感谢", "您好", "～")
_CLARITY_TERMS = ("不押车", "绿本", "年化", "透明", "方案", "月供", "期数")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class HumanEditDiff(BaseModel):
    changed: bool = False
    similarity: float = 1.0
    edit_ratio: float = 0.0
    char_delta: int = 0
    removed_segments: List[str] = Field(default_factory=list)
    added_segments: List[str] = Field(default_factory=list)
    unified_diff: str = ""
    edit_tags: List[str] = Field(default_factory=list)


class FeedbackRecordRequest(BaseModel):
    tenant_id: str = Field(default="default", min_length=1, max_length=64)
    entity_id: str = Field(..., min_length=1, max_length=128)
    feedback_type: FeedbackType
    policy_id: Optional[str] = Field(default=None, max_length=128)
    policy_version: Optional[str] = Field(default=None, max_length=32)
    script_version: Optional[str] = Field(default=None, max_length=64)
    channel: Optional[str] = Field(default=None, max_length=64)
    outbox_id: Optional[int] = None
    outcome: Optional[str] = Field(default=None, max_length=64)
    value_score: Optional[float] = None
    original_text: Optional[str] = None
    edited_text: Optional[str] = None
    guardrail_rule: Optional[str] = Field(default=None, max_length=128)
    reviewer_id: Optional[str] = Field(default=None, max_length=128)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    occurred_at: Optional[datetime] = None


class FeedbackRecordResponse(BaseModel):
    id: int
    tenant_id: str
    entity_id: str
    feedback_type: str
    policy_id: str
    policy_version: str
    script_version: str
    channel: str
    outbox_id: Optional[int] = None
    outcome: Optional[str] = None
    value_score: Optional[float] = None
    original_text: Optional[str] = None
    edited_text: Optional[str] = None
    human_edit_diff: Dict[str, Any] = Field(default_factory=dict)
    guardrail_rule: Optional[str] = None
    reviewer_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime
    create_time: datetime


def compute_human_edit_diff(original_text: str, edited_text: str) -> HumanEditDiff:
    """Compute character/phrase-level diff and semantic revision tags."""
    orig = str(original_text or "")
    edited = str(edited_text or "")
    changed = orig != edited

    sm = difflib.SequenceMatcher(None, orig, edited)
    similarity = round(sm.ratio(), 4)
    edit_ratio = round(max(0.0, 1.0 - similarity), 4)
    char_delta = len(edited) - len(orig)

    removed_segments: List[str] = []
    added_segments: List[str] = []
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag in ("replace", "delete"):
            seg = orig[i1:i2].strip()
            if seg:
                removed_segments.append(seg)
        if tag in ("replace", "insert"):
            seg = edited[j1:j2].strip()
            if seg:
                added_segments.append(seg)

    u_diff = "\n".join(
        difflib.unified_diff(
            orig.splitlines(),
            edited.splitlines(),
            fromfile="ai_draft",
            tofile="human_edited",
            lineterm="",
        )
    )

    edit_tags: List[str] = []
    if changed:
        if char_delta <= -8:
            edit_tags.append("shortened")
        elif char_delta >= 12:
            edit_tags.append("expanded")
        if edit_ratio >= 0.6:
            edit_tags.append("major_rewrite")

        removed_blob = " ".join(removed_segments)
        added_blob = " ".join(added_segments)

        if any(
            (w in orig and w not in edited) or w in removed_blob
            for w in _SOFTEN_WORDS
        ):
            edit_tags.append("compliance_or_tone_softened")

        if any(
            (w in added_blob) or (edited.count(w) > orig.count(w))
            for w in _POLITE_CUES
        ):
            edit_tags.append("politeness_added")

        if any(
            (w in added_blob) or (w in edited and w not in orig)
            for w in _CLARITY_TERMS
        ):
            edit_tags.append("clarity_enhanced")

        if not edit_tags:
            edit_tags.append("minor_polish")

    return HumanEditDiff(
        changed=changed,
        similarity=similarity,
        edit_ratio=edit_ratio,
        char_delta=char_delta,
        removed_segments=removed_segments,
        added_segments=added_segments,
        unified_diff=u_diff,
        edit_tags=edit_tags,
    )


def _row_to_response(row: RsiFeedbackRecord) -> FeedbackRecordResponse:
    return FeedbackRecordResponse(
        id=row.id,
        tenant_id=row.tenant_id,
        entity_id=row.entity_id,
        feedback_type=row.feedback_type,
        policy_id=row.policy_id,
        policy_version=row.policy_version,
        script_version=row.script_version,
        channel=row.channel,
        outbox_id=row.outbox_id,
        outcome=row.outcome,
        value_score=row.value_score,
        original_text=row.original_text,
        edited_text=row.edited_text,
        human_edit_diff=dict(row.human_edit_diff or {}),
        guardrail_rule=row.guardrail_rule,
        reviewer_id=row.reviewer_id,
        metadata=dict(row.metadata or {}),
        occurred_at=to_utc(row.occurred_at),
        create_time=to_utc(row.create_time),
    )


async def record_feedback(req: FeedbackRecordRequest) -> FeedbackRecordResponse:
    """Ingest feedback attribution or human rewrite diff cleanly."""
    canonical = await resolve_canonical_entity(req.tenant_id, req.entity_id)
    cid = canonical.entity_id if canonical is not None else req.entity_id

    policy_id = req.policy_id
    policy_version = req.policy_version
    script_version = req.script_version
    channel = req.channel
    guardrail_rule = req.guardrail_rule
    original_text = req.original_text

    if req.outbox_id is not None:
        outbox = await ActionOutbox.get_or_none(
            id=req.outbox_id, tenant_id=req.tenant_id
        )
        if outbox is not None:
            policy_id = policy_id or outbox.policy_id
            policy_version = policy_version or outbox.policy_version
            channel = channel or outbox.channel
            guardrail_rule = guardrail_rule or outbox.verdict_rule
            payload = dict(outbox.payload or {})
            script_version = script_version or payload.get("script_version")
            if original_text is None:
                for key in ("text", "message", "opening_script", "sms_text", "content"):
                    if isinstance(payload.get(key), str) and payload[key].strip():
                        original_text = payload[key].strip()
                        break

    diff_dict: Dict[str, Any] = {}
    if original_text is not None and req.edited_text is not None:
        diff_dict = compute_human_edit_diff(original_text, req.edited_text).model_dump()

    occurred_at = to_utc(req.occurred_at) if req.occurred_at else _utc_now()
    row = await RsiFeedbackRecord.create(
        tenant_id=req.tenant_id,
        entity_id=cid,
        feedback_type=req.feedback_type,
        policy_id=policy_id or "default",
        policy_version=policy_version or "1.0",
        script_version=script_version or "v1",
        channel=channel or "wechat",
        outbox_id=req.outbox_id,
        outcome=req.outcome,
        value_score=req.value_score,
        original_text=original_text,
        edited_text=req.edited_text,
        human_edit_diff=diff_dict,
        guardrail_rule=guardrail_rule,
        reviewer_id=req.reviewer_id,
        metadata=dict(req.metadata or {}),
        occurred_at=occurred_at,
    )
    return _row_to_response(row)


async def sync_guardrail_blocks_to_feedback(
    tenant_id: str = "default",
    *,
    since: Optional[datetime] = None,
) -> int:
    """Idempotently sync ActionOutbox guardrail blocks into RsiFeedbackRecord."""
    qs = ActionOutbox.filter(tenant_id=tenant_id, status="guardrail_blocked")
    if since is not None:
        qs = qs.filter(create_time__gte=to_utc(since))
    blocked_rows = await qs.order_by("id").all()
    if not blocked_rows:
        return 0

    existing_ids = set(
        await RsiFeedbackRecord.filter(
            tenant_id=tenant_id,
            feedback_type="guardrail_block",
            outbox_id__not_isnull=True,
        ).values_list("outbox_id", flat=True)
    )

    created = 0
    for row in blocked_rows:
        if row.id in existing_ids:
            continue
        payload = dict(row.payload or {})
        draft_text = None
        for key in ("text", "message", "opening_script", "sms_text", "content"):
            val = payload.get(key)
            if isinstance(val, str) and val.strip():
                draft_text = val.strip()
                break

        await RsiFeedbackRecord.create(
            tenant_id=tenant_id,
            entity_id=row.entity_id,
            feedback_type="guardrail_block",
            policy_id=row.policy_id,
            policy_version=row.policy_version,
            script_version=str(payload.get("script_version") or "v1"),
            channel=row.channel,
            outbox_id=row.id,
            outcome="blocked",
            original_text=draft_text,
            guardrail_rule=row.verdict_rule,
            metadata={"verdict_reason": row.verdict_reason},
            occurred_at=to_utc(row.finished_at or row.create_time or _utc_now()),
        )
        created += 1

    return created


async def list_feedback_records(
    tenant_id: str = "default",
    *,
    entity_id: Optional[str] = None,
    policy_id: Optional[str] = None,
    feedback_type: Optional[str] = None,
    limit: int = 100,
) -> List[FeedbackRecordResponse]:
    qs = RsiFeedbackRecord.filter(tenant_id=tenant_id)
    if entity_id:
        qs = qs.filter(entity_id=entity_id)
    if policy_id:
        qs = qs.filter(policy_id=policy_id)
    if feedback_type:
        qs = qs.filter(feedback_type=feedback_type)
    rows = await qs.order_by("-occurred_at", "-id").limit(limit).all()
    return [_row_to_response(r) for r in rows]
