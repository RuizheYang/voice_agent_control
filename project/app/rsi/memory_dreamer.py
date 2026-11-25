"""L3 Semantic Working Memory Dreamer for RSI V3.

Strict isolation invariant:
- READ-ONLY on L1 (`EntityEventLog`, `FollowupEntity`) and L2 (`EntityMemoryState`).
- WRITE-ONLY on L3 (`EntityWorkingMemory`: `customer_profile_notes`,
  `rolling_digest`, `active_guidance`, `last_dreamed_at`).
- Dreamer never mutates deterministic L2 fields such as `hard_boundaries`,
  `lifecycle`, `commitments`, or `ownership`.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from app.rsi.identity import resolve_canonical_entity
from app.rsi.models import EntityEventLog, EntityMemoryState, EntityWorkingMemory, RsiTask
from app.rsi.subflows.runtime import resolve_default_llm_model, sanitize_llm_output
from app.rsi.timers import schedule_timer


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _sanitize_nested(value: Any) -> Any:
    if isinstance(value, str):
        return sanitize_llm_output(value)
    if isinstance(value, list):
        return [_sanitize_nested(v) for v in value]
    if isinstance(value, dict):
        return {k: _sanitize_nested(v) for k, v in value.items()}
    return value


class SemanticDreamOutput(BaseModel):
    """Structured semantic synthesis written solely to L3 `EntityWorkingMemory`."""

    customer_profile_notes: Dict[str, Any] = Field(default_factory=dict)
    rolling_digest: str = ""
    active_guidance: str = ""
    detected_hesitations: List[str] = Field(default_factory=list)
    preferred_contact_windows: List[str] = Field(default_factory=list)


_DREAMER_SYSTEM_PROMPT = """你是一位资深的客户跟进记忆整理师（Memory Dreamer）。
你的任务是基于客户最近的 L1 事实事件日志与 L2 当前状态快照，提炼出供后续多智能体使用的 L3 语义工作记忆：
1. customer_profile_notes: 客户画像摘要（如职业/作息习惯/关注重点/沟通偏好）
2. rolling_digest: 对近期交互的简洁滚动摘要（避免冗长细节）
3. active_guidance: 供下一步跟进决策参考的温和策略建议
4. detected_hesitations: 识别出的客户顾虑列表
5. preferred_contact_windows: 客户表达或体现出的偏好触达时段

注意：你只做客观语义整理与建议，绝不输出任何思考过程标签（如 <think>），也不要捏造事实。"""


class RsiMemoryDreamer:
    """Asynchronous semantic memory synthesizer strictly isolated to L3 EntityWorkingMemory."""

    def __init__(
        self,
        *,
        model: Any = None,
        max_events: int = 50,
        max_digest_entries: int = 12,
    ) -> None:
        self.model = model
        self.max_events = max(1, int(max_events))
        self.max_digest_entries = max(1, int(max_digest_entries))

    async def dream_entity(
        self,
        tenant_id: str,
        entity_id: str,
        *,
        model: Any = None,
        now: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Synthesize L1 + L2 context into L3 `EntityWorkingMemory` without touching L1/L2."""
        ts = now or _utc_now()
        canonical = await resolve_canonical_entity(tenant_id, entity_id)
        if canonical is None:
            return {
                "status": "entity_not_found",
                "tenant_id": tenant_id,
                "entity_id": entity_id,
            }
        cid = canonical.entity_id

        events_desc = (
            await EntityEventLog.filter(tenant_id=tenant_id, entity_id=cid)
            .order_by("-occurred_at", "-id")
            .limit(self.max_events)
            .all()
        )
        events = list(reversed(events_desc))

        mem = await EntityMemoryState.get_or_none(tenant_id=tenant_id, entity_id=cid)
        working = await EntityWorkingMemory.get_or_none(
            tenant_id=tenant_id, entity_id=cid
        )

        event_summaries = [
            {
                "event_id": ev.event_id,
                "event_type": ev.event_type,
                "category": ev.category,
                "occurred_at": ev.occurred_at.isoformat() if ev.occurred_at else None,
                "summary": ev.summary,
                "facts": ev.facts or {},
            }
            for ev in events
        ]
        l2_snapshot = (
            {
                "lifecycle": mem.lifecycle or {},
                "hard_boundaries": mem.hard_boundaries or {},
                "commitments": mem.commitments or [],
                "ownership": mem.ownership or {},
                "channel_stats": mem.channel_stats or {},
            }
            if mem is not None
            else {}
        )
        prev_l3 = (
            {
                "customer_profile_notes": working.customer_profile_notes or {},
                "rolling_digest": working.rolling_digest or [],
                "active_guidance": working.active_guidance or "",
            }
            if working is not None
            else {}
        )

        prompt_payload = {
            "tenant_id": tenant_id,
            "entity_id": cid,
            "l2_readonly_snapshot": l2_snapshot,
            "existing_l3": prev_l3,
            "recent_l1_events": event_summaries,
        }

        active_model = model or self.model or resolve_default_llm_model()
        agent = Agent(
            model=active_model,
            output_type=SemanticDreamOutput,
            system_prompt=_DREAMER_SYSTEM_PROMPT,
            defer_model_check=True,
        )
        res = await agent.run(json.dumps(prompt_payload, ensure_ascii=False))
        dream: SemanticDreamOutput = res.output

        clean_notes: Dict[str, Any] = _sanitize_nested(dream.customer_profile_notes or {})
        clean_digest = sanitize_llm_output(dream.rolling_digest)
        clean_guidance = sanitize_llm_output(dream.active_guidance)
        clean_hesitations = _sanitize_nested(dream.detected_hesitations or [])
        clean_windows = _sanitize_nested(dream.preferred_contact_windows or [])

        if working is None:
            working = await EntityWorkingMemory.create(
                tenant_id=tenant_id,
                entity_id=cid,
                customer_profile_notes={},
                rolling_digest=[],
                active_guidance="",
            )

        merged_notes = dict(working.customer_profile_notes or {})
        merged_notes.update(clean_notes)
        if clean_hesitations:
            merged_notes["detected_hesitations"] = clean_hesitations
        if clean_windows:
            merged_notes["preferred_contact_windows"] = clean_windows

        digests = list(working.rolling_digest or [])
        if clean_digest:
            digests.append(
                {
                    "ts": ts.isoformat(),
                    "digest": clean_digest,
                    "event_count": len(events),
                    "last_event_id": events[-1].event_id if events else None,
                }
            )
            if len(digests) > self.max_digest_entries:
                digests = digests[-self.max_digest_entries :]

        working.customer_profile_notes = merged_notes
        working.rolling_digest = digests
        working.active_guidance = clean_guidance
        working.last_dreamed_at = ts
        await working.save()

        return {
            "status": "dreamed",
            "tenant_id": tenant_id,
            "entity_id": cid,
            "event_count": len(events),
            "last_dreamed_at": ts.isoformat(),
            "customer_profile_notes": working.customer_profile_notes,
            "rolling_digest": working.rolling_digest,
            "active_guidance": working.active_guidance,
        }


async def enqueue_memory_dream_task(
    tenant_id: str,
    entity_id: str,
    *,
    run_at: Optional[datetime] = None,
    notify: bool = True,
    connection=None,
) -> RsiTask:
    """Enqueue an idempotent background L3 dreaming task via RsiTask."""
    task = await schedule_timer(
        tenant_id,
        entity_id,
        timer_type="memory_dream",
        run_at=run_at or _utc_now(),
        payload={"trigger": "memory_dream"},
        priority=8,
        replace_existing=True,
        connection=connection,
    )
    if notify:
        from app.rsi.worker import notify_rsi_worker

        notify_rsi_worker()
    return task
