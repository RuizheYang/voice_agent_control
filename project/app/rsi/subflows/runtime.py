"""Universal SubFlow Runtime for RSI V3.

Implements:
- Output sanitization (strips <think>, <thinking>, <reasoning>, <internal> blocks)
- Per-node trimmed conversation history
- Global Q&A topics with call-stack jump-back
- Dynamic Pydantic schema construction and strict Pydantic-AI union output_type
  (`output_schema | str`) locking so incomplete steps never advance
- Compact L2 + L3 context index builder
- End-to-end persistence to `FollowupPolicy.active_subflow_state` and submission
  of proposed actions through the Step 3 Outbox Guardrail.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Type,
    Union,
)

from pydantic import BaseModel, Field, create_model
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from app.config import config
from app.rsi.identity import resolve_canonical_entity
from app.rsi.models import EntityMemoryState, EntityWorkingMemory
from app.rsi.subflows.base import (
    BaseSubFlowHandler,
    StepResult,
    SubFlowRunContext,
    SubFlowStateSnapshot,
)

_CLOSED_THOUGHT_RE = re.compile(
    r"<(?:think|thinking|reasoning|internal)\b[^>]*>.*?</(?:think|thinking|reasoning|internal)>",
    re.IGNORECASE | re.DOTALL,
)
_UNCLOSED_THOUGHT_RE = re.compile(
    r"<(?:think|thinking|reasoning|internal)\b[^>]*>.*$",
    re.IGNORECASE | re.DOTALL,
)
_MULTI_NEWLINE_RE = re.compile(r"\n{3,}")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def sanitize_llm_output(text: Optional[str]) -> str:
    """Remove internal chain-of-thought tags before any external delivery or storage."""
    if not text:
        return ""
    cleaned = _CLOSED_THOUGHT_RE.sub("", str(text))
    cleaned = _UNCLOSED_THOUGHT_RE.sub("", cleaned)
    cleaned = _MULTI_NEWLINE_RE.sub("\n\n", cleaned)
    return cleaned.strip()


def trim_node_history(
    history: Sequence[Dict[str, Any]],
    max_turns: int = 12,
) -> List[Dict[str, Any]]:
    """Keep only the most recent turns for a specific node to constrain prompt size."""
    if max_turns <= 0:
        return []
    items = list(history or [])
    if len(items) <= max_turns:
        return items
    return items[-max_turns:]


class GlobalQATopicSpec(BaseModel):
    """Cross-node FAQ specification supporting call-stack interruption and jump-back."""

    topic_id: str = Field(..., min_length=1, max_length=64)
    keywords: List[str] = Field(default_factory=list)
    question_patterns: List[str] = Field(default_factory=list)
    answer_template: str = Field(..., min_length=1)
    resume_prompt: str = ""
    priority: int = Field(default=10, ge=0)


def match_global_qa_topic(
    incoming_text: str,
    topics: Sequence[GlobalQATopicSpec],
) -> Optional[GlobalQATopicSpec]:
    """Match an incoming customer utterance against configured global Q&A topics."""
    text = (incoming_text or "").strip()
    if not text or not topics:
        return None
    lowered = text.lower()
    ordered = sorted(topics, key=lambda t: t.priority)
    for topic in ordered:
        for kw in topic.keywords:
            if kw and kw.lower() in lowered:
                return topic
        for pattern in topic.question_patterns:
            if pattern and re.search(pattern, text, re.IGNORECASE):
                return topic
    return None


def execute_qa_jump_back(
    ctx: SubFlowRunContext,
    topic: GlobalQATopicSpec,
    *,
    origin_resume_hint: Optional[str] = None,
    now: Optional[datetime] = None,
) -> StepResult:
    """Push QA frame onto call stack, answer the off-topic question, and jump back."""
    ts = now or _utc_now()
    pushed_frame = {
        "return_node_id": ctx.node_id,
        "topic_id": topic.topic_id,
        "question": ctx.incoming_text,
        "pushed_at": ts.isoformat(),
    }
    answer = sanitize_llm_output(topic.answer_template)
    resume = sanitize_llm_output(topic.resume_prompt or origin_resume_hint or "")
    combined_reply = f"{answer}\n{resume}".strip() if resume else answer

    return StepResult(
        status="qa_answered",
        reply_text=combined_reply,
        next_node_id=ctx.node_id,
        used_fixed_opening=False,
        used_llm=False,
        qa_jump_back={
            "pushed_frame": pushed_frame,
            "return_node_id": ctx.node_id,
            "returned_to_node": ctx.node_id,
            "topic_id": topic.topic_id,
            "stack_depth_during_qa": len(ctx.qa_call_stack) + 1,
        },
        metadata={"qa_topic_id": topic.topic_id},
    )


def _resolve_field_type(spec: Dict[str, Any]) -> Any:
    raw_type = str(spec.get("type", "str")).strip().lower()
    enum_values = spec.get("enum")
    if isinstance(enum_values, list) and enum_values:
        return Literal[tuple(enum_values)]  # type: ignore[valid-type]

    mapping: Dict[str, Any] = {
        "str": str,
        "string": str,
        "int": int,
        "integer": int,
        "float": float,
        "number": float,
        "bool": bool,
        "boolean": bool,
        "list[str]": List[str],
        "list": List[Any],
        "dict": Dict[str, Any],
    }
    return mapping.get(raw_type, str)


def build_dynamic_step_model(
    schema_name: str,
    fields: Sequence[Dict[str, Any]],
) -> Type[BaseModel]:
    """Compile a strongly typed Pydantic schema from declarative YAML node field specs."""
    model_fields: Dict[str, Any] = {}
    for spec in fields:
        name = str(spec["name"])
        base_type = _resolve_field_type(spec)
        required = bool(spec.get("required", True))
        field_type = base_type if required else Optional[base_type]

        field_kwargs: Dict[str, Any] = {}
        if "description" in spec and spec["description"]:
            field_kwargs["description"] = str(spec["description"])
        for num_key in ("min_length", "max_length", "ge", "le", "gt", "lt"):
            if num_key in spec and spec[num_key] is not None:
                field_kwargs[num_key] = spec[num_key]
        if "pattern" in spec and spec["pattern"]:
            field_kwargs["pattern"] = str(spec["pattern"])

        if required and "default" not in spec:
            model_fields[name] = (field_type, Field(..., **field_kwargs))
        else:
            default_val = spec.get("default", None)
            model_fields[name] = (field_type, Field(default=default_val, **field_kwargs))

    safe_name = re.sub(r"[^0-9A-Za-z_]", "_", schema_name or "DynamicStepSchema")
    return create_model(safe_name, **model_fields)


def resolve_default_llm_model() -> OpenAIChatModel:
    """Build the default OpenAI-compatible Pydantic-AI model lazily from config."""
    llm_cfg = config.llm_factory_config
    provider = OpenAIProvider(
        base_url=llm_cfg.base_url,
        api_key=llm_cfg.api_key or "dummy-key",
    )
    return OpenAIChatModel(model_name=llm_cfg.model_name, provider=provider)


def _format_step_prompt(
    ctx: SubFlowRunContext,
    *,
    extra_instructions: str = "",
) -> str:
    parts: List[str] = []
    if ctx.compact_context_index:
        parts.append(f"[客户与状态摘要]\n{ctx.compact_context_index}")
    if ctx.collected_slots:
        parts.append(
            f"[已收集槽位]\n{json.dumps(ctx.collected_slots, ensure_ascii=False)}"
        )
    if ctx.node_history:
        history_lines = [
            f"{m.get('role', 'user')}: {m.get('content', '')}"
            for m in ctx.node_history
        ]
        parts.append("[当前节点最近对话]\n" + "\n".join(history_lines))
    if ctx.media_urls:
        parts.append(
            "[附件链接]\n" + json.dumps(ctx.media_urls, ensure_ascii=False)
        )
    if extra_instructions:
        parts.append(f"[补充要求]\n{extra_instructions}")
    if ctx.incoming_text:
        parts.append(f"[本轮客户最新输入 ({ctx.incoming_channel})]\n{ctx.incoming_text}")
    else:
        parts.append("[本轮触发] 首次进入节点或无文本输入，请引导客户补充所需信息。")
    return "\n\n".join(parts)


async def run_typed_step_agent(
    *,
    ctx: SubFlowRunContext,
    system_prompt: str,
    output_schema: Optional[Type[BaseModel]] = None,
    toolsets: Optional[Sequence[Any]] = None,
    completion_reply: str = "",
    model: Any = None,
    extra_instructions: str = "",
) -> tuple[bool, Dict[str, Any], str]:
    """Run a Pydantic-AI step agent with strict `output_schema | str` union locking.

    Returns `(completed, extracted_slots, sanitized_reply)`:
    - If `res.output` is an instance of `output_schema`, `completed=True`.
    - If `res.output` is `str`, `completed=False` (step stays locked on current node).
    """
    active_model = model or ctx.model or resolve_default_llm_model()
    union_type: Any = Union[output_schema, str] if output_schema is not None else str

    agent_kwargs: Dict[str, Any] = {
        "model": active_model,
        "output_type": union_type,
        "system_prompt": system_prompt,
        "defer_model_check": True,
    }
    if toolsets:
        agent_kwargs["toolsets"] = list(toolsets)

    agent = Agent(**agent_kwargs)
    prompt = _format_step_prompt(ctx, extra_instructions=extra_instructions)
    res = await agent.run(prompt)
    output = res.output

    if output_schema is not None and isinstance(output, output_schema):
        return True, output.model_dump(mode="json"), sanitize_llm_output(completion_reply)

    return False, {}, sanitize_llm_output(str(output))


def build_compact_context_index(
    l2_state: Optional[Dict[str, Any]],
    l3_notes: Optional[Dict[str, Any]],
    snapshot: Optional[SubFlowStateSnapshot] = None,
) -> str:
    """Compile a concise ~250-token L2 + L3 summary for subflows and decision agents."""
    l2 = l2_state or {}
    l3 = l3_notes or {}
    lifecycle = l2.get("lifecycle") or {}
    ownership = l2.get("ownership") or {}
    boundaries = l2.get("hard_boundaries") or {}
    commitments = l2.get("commitments") or []
    channel_stats = l2.get("channel_stats") or {}

    owner_role = ownership.get("current_role") or ownership.get("owner_role") or "ai"
    lines = [
        f"stage={lifecycle.get('stage', 'unknown')} | owner={owner_role}",
    ]
    active_limits = [k for k, v in boundaries.items() if bool(v)]
    if active_limits:
        lines.append(f"hard_boundaries={','.join(active_limits)}")
    if commitments:
        lines.append(f"open_commitments={len(commitments)}")
    if channel_stats:
        lines.append(
            f"touches={json.dumps(channel_stats, ensure_ascii=False)[:120]}"
        )
    if snapshot is not None:
        lines.append(
            f"subflow={snapshot.subflow_id}:{snapshot.current_node_id} "
            f"(status={snapshot.status}, slots={list(snapshot.collected_slots.keys())})"
        )

    raw_digest = l3.get("rolling_digest")
    if isinstance(raw_digest, list) and raw_digest:
        parts = []
        for item in raw_digest[-2:]:
            if isinstance(item, dict):
                parts.append(str(item.get("digest") or item.get("summary") or ""))
            else:
                parts.append(str(item))
        digest = sanitize_llm_output(" | ".join(p for p in parts if p))
    else:
        digest = sanitize_llm_output(str(raw_digest or ""))

    guidance = sanitize_llm_output(str(l3.get("active_guidance") or ""))
    if digest:
        lines.append(f"digest={digest[:220]}")
    if guidance:
        lines.append(f"guidance={guidance[:180]}")

    return "\n".join(lines)


async def _ensure_l2_memory_state(
    tenant_id: str, entity_id: str
) -> EntityMemoryState:
    mem = await EntityMemoryState.get_or_none(tenant_id=tenant_id, entity_id=entity_id)
    if mem is not None:
        return mem
    return await EntityMemoryState.create(
        tenant_id=tenant_id,
        entity_id=entity_id,
        hard_boundaries={
            "dnd": False,
            "blacklisted": False,
            "human_takeover_until": None,
            "risk_flags": [],
            "erasure_requested": False,
        },
        commitments=[],
        lifecycle={
            "stage": "new",
            "converted": False,
            "churn_risk": None,
            "churn_signals": [],
            "fulfillment_status": None,
            "fulfillment_detail": {},
            "orders": {},
        },
        ownership={
            "current_role": None,
            "owner_id": None,
            "handoff_history": [],
            "pending_approvals": {},
        },
        channel_stats={
            "by_channel": {},
            "wechat_added": False,
            "content_signals": [],
            "private_domain_events": [],
            "broadcasts": [],
            "last_receipt": None,
        },
    )


class SubFlowRuntime:
    """Executes sub-state-machines, maintains node history & QA stack, and dispatches Outbox proposals."""

    def __init__(
        self,
        *,
        registry: Optional[Any] = None,
        default_max_node_turns: int = 12,
    ) -> None:
        self._registry = registry
        self.default_max_node_turns = max(2, int(default_max_node_turns))

    @property
    def registry(self) -> Any:
        if self._registry is None:
            from app.rsi.subflows.registry import subflow_registry

            self._registry = subflow_registry
        return self._registry

    async def start_subflow(
        self,
        tenant_id: str,
        entity_id: str,
        subflow_id: str,
        *,
        initial_slots: Optional[Dict[str, Any]] = None,
        extra: Optional[Dict[str, Any]] = None,
        model: Any = None,
        emit_initial_opening: bool = True,
        dispatch_actions: bool = False,
        now: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Initialize a new active subflow snapshot inside L2 `FollowupPolicy`."""
        ts = now or _utc_now()
        canonical = await resolve_canonical_entity(tenant_id, entity_id)
        if canonical is None:
            raise ValueError(f"Unknown entity {entity_id!r} for tenant {tenant_id!r}")

        handler: BaseSubFlowHandler = self.registry.get(subflow_id)
        snapshot = handler.build_initial_snapshot(now=ts, extra=extra)
        if initial_slots:
            snapshot.collected_slots.update(dict(initial_slots))

        mem = await _ensure_l2_memory_state(tenant_id, canonical.entity_id)
        lifecycle = dict(mem.lifecycle or {})
        lifecycle["active_subflow_state"] = snapshot.model_dump(mode="json")
        mem.lifecycle = lifecycle
        await mem.save()

        if emit_initial_opening:
            return await self.advance_entity_subflow(
                tenant_id,
                canonical.entity_id,
                incoming_text="",
                subflow_id=subflow_id,
                model=model,
                dispatch_actions=dispatch_actions,
                now=ts,
            )

        return {
            "status": "started",
            "tenant_id": tenant_id,
            "entity_id": canonical.entity_id,
            "subflow_id": subflow_id,
            "node_id": snapshot.current_node_id,
            "snapshot": snapshot.model_dump(mode="json"),
            "action_receipts": [],
        }

    async def advance_entity_subflow(
        self,
        tenant_id: str,
        entity_id: str,
        *,
        incoming_text: str = "",
        incoming_channel: str = "wechat",
        media_urls: Optional[List[str]] = None,
        event_payload: Optional[Dict[str, Any]] = None,
        subflow_id: Optional[str] = None,
        model: Any = None,
        qa_topics: Optional[Sequence[GlobalQATopicSpec]] = None,
        dispatch_actions: bool = True,
        now: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Run one turn on an entity's active subflow and persist state + outbox proposals."""
        ts = now or _utc_now()
        canonical = await resolve_canonical_entity(tenant_id, entity_id)
        if canonical is None:
            return {
                "status": "entity_not_found",
                "tenant_id": tenant_id,
                "entity_id": entity_id,
                "action_receipts": [],
            }
        cid = canonical.entity_id

        mem = await _ensure_l2_memory_state(tenant_id, cid)
        lifecycle_data = dict(mem.lifecycle or {})
        raw_snapshot = dict(lifecycle_data.get("active_subflow_state") or {})

        # Initialize snapshot if none exists yet or caller explicitly switches to another subflow
        if not raw_snapshot or (
            subflow_id and raw_snapshot.get("subflow_id") != subflow_id
        ):
            if not subflow_id:
                return {
                    "status": "no_active_subflow",
                    "tenant_id": tenant_id,
                    "entity_id": cid,
                    "action_receipts": [],
                }
            handler = self.registry.get(subflow_id)
            snapshot = handler.build_initial_snapshot(now=ts)
            lifecycle_data["active_subflow_state"] = snapshot.model_dump(mode="json")
            mem.lifecycle = lifecycle_data
            await mem.save()
        else:
            # Strip non-snapshot helper metadata keys if any before validation
            snap_payload = {
                k: v for k, v in raw_snapshot.items() if k != "last_conclusion"
            }
            snapshot = SubFlowStateSnapshot.model_validate(snap_payload)
            handler = self.registry.get(snapshot.subflow_id)

        if snapshot.status in ("completed", "cancelled", "escalated"):
            return {
                "status": f"subflow_{snapshot.status}",
                "tenant_id": tenant_id,
                "entity_id": cid,
                "subflow_id": snapshot.subflow_id,
                "node_id": snapshot.current_node_id,
                "snapshot": snapshot.model_dump(mode="json"),
                "action_receipts": [],
            }

        working = await EntityWorkingMemory.get_or_none(
            tenant_id=tenant_id, entity_id=cid
        )

        l2_dict = {
            "lifecycle": dict(mem.lifecycle or {}),
            "hard_boundaries": dict(mem.hard_boundaries or {}),
            "commitments": list(mem.commitments or []),
            "ownership": dict(mem.ownership or {}),
            "channel_stats": dict(mem.channel_stats or {}),
        }
        l3_dict = (
            {
                "customer_profile_notes": dict(working.customer_profile_notes or {}),
                "rolling_digest": working.rolling_digest or [],
                "active_guidance": working.active_guidance or "",
            }
            if working is not None
            else {}
        )

        origin_node_id = snapshot.current_node_id
        node_history = trim_node_history(
            snapshot.node_histories.get(origin_node_id, []),
            max_turns=self.default_max_node_turns,
        )
        first_visit = origin_node_id not in snapshot.visited_nodes

        ctx = SubFlowRunContext(
            tenant_id=tenant_id,
            entity_id=cid,
            subflow_id=snapshot.subflow_id,
            node_id=origin_node_id,
            incoming_text=incoming_text,
            incoming_channel=incoming_channel,
            media_urls=list(media_urls or []),
            event_payload=dict(event_payload or {}),
            collected_slots=dict(snapshot.collected_slots),
            node_history=node_history,
            visited_nodes=list(snapshot.visited_nodes),
            qa_call_stack=list(snapshot.qa_call_stack),
            l2_state=l2_dict,
            l3_notes=l3_dict,
            compact_context_index=build_compact_context_index(
                l2_dict, l3_dict, snapshot
            ),
            model=model,
            first_visit_to_node=first_visit,
        )

        # 1) Global Q&A call-stack check before step LLM execution
        combined_topics: List[GlobalQATopicSpec] = []
        for t in handler.get_qa_topics():
            combined_topics.append(
                t if isinstance(t, GlobalQATopicSpec) else GlobalQATopicSpec.model_validate(t)
            )
        if qa_topics:
            for t in qa_topics:
                combined_topics.append(
                    t if isinstance(t, GlobalQATopicSpec) else GlobalQATopicSpec.model_validate(t)
                )

        matched_topic = match_global_qa_topic(incoming_text, combined_topics)
        if matched_topic is not None:
            origin_hint = None
            get_hint = getattr(handler, "get_node_resume_hint", None)
            if callable(get_hint):
                origin_hint = get_hint(origin_node_id)
            result = execute_qa_jump_back(
                ctx, matched_topic, origin_resume_hint=origin_hint, now=ts
            )
        else:
            result = await handler.step(ctx)

        # Sanitize reply text unconditionally at runtime exit
        clean_reply = sanitize_llm_output(result.reply_text)

        # 2) Update slots, visited nodes, QA stack trace, and per-node histories
        if result.updated_slots:
            snapshot.collected_slots.update(result.updated_slots)

        if origin_node_id not in snapshot.visited_nodes:
            snapshot.visited_nodes.append(origin_node_id)

        target_node = result.next_node_id or origin_node_id
        if result.used_fixed_opening and target_node not in snapshot.visited_nodes:
            snapshot.visited_nodes.append(target_node)

        if result.qa_jump_back:
            qa_hist = snapshot.extra.setdefault("qa_history", [])
            qa_hist.append(result.qa_jump_back)
            # Immediate jump-back completes in the same turn, leaving stack balanced
            snapshot.qa_call_stack = []

        # Append conversational turns to appropriate node history
        if incoming_text.strip():
            orig_hist = list(snapshot.node_histories.get(origin_node_id, []))
            orig_hist.append(
                {
                    "role": "user",
                    "content": incoming_text.strip(),
                    "ts": ts.isoformat(),
                }
            )
            snapshot.node_histories[origin_node_id] = trim_node_history(
                orig_hist, max_turns=self.default_max_node_turns
            )

        if clean_reply:
            reply_hist_node = (
                target_node
                if result.used_fixed_opening and target_node != origin_node_id
                else origin_node_id
            )
            r_hist = list(snapshot.node_histories.get(reply_hist_node, []))
            r_hist.append(
                {
                    "role": "assistant",
                    "content": clean_reply,
                    "ts": ts.isoformat(),
                }
            )
            snapshot.node_histories[reply_hist_node] = trim_node_history(
                r_hist, max_turns=self.default_max_node_turns
            )

        snapshot.current_node_id = target_node
        if result.status == "completed":
            snapshot.status = "completed"
        elif result.status == "paused":
            snapshot.status = "paused"
        elif result.status == "escalated":
            snapshot.status = "escalated"
        else:
            snapshot.status = "active"

        if result.conclusion is not None:
            snapshot.extra["conclusion"] = result.conclusion

        snapshot.turn_count += 1
        snapshot.updated_at = ts

        persisted = snapshot.model_dump(mode="json")
        if result.conclusion is not None:
            persisted["last_conclusion"] = result.conclusion

        updated_lifecycle = dict(mem.lifecycle or {})
        updated_lifecycle["active_subflow_state"] = persisted
        mem.lifecycle = updated_lifecycle
        await mem.save()

        # 3) Dispatch proposed actions through Outbox Guardrail
        action_receipts: List[Dict[str, Any]] = []
        if result.proposed_actions:
            from app.rsi.guardrails.outbox_guardrail import submit_action_proposal

            for proposal in result.proposed_actions:
                receipt = await submit_action_proposal(
                    proposal,
                    now=ts,
                    dispatch_immediately=dispatch_actions,
                )
                action_receipts.append(receipt)

        return {
            "status": result.status,
            "tenant_id": tenant_id,
            "entity_id": cid,
            "subflow_id": snapshot.subflow_id,
            "previous_node_id": origin_node_id,
            "node_id": snapshot.current_node_id,
            "reply_text": clean_reply,
            "collected_slots": dict(snapshot.collected_slots),
            "conclusion": result.conclusion,
            "used_fixed_opening": result.used_fixed_opening,
            "used_llm": result.used_llm,
            "qa_jump_back": result.qa_jump_back,
            "action_receipts": action_receipts,
            "snapshot": persisted,
        }


subflow_runtime = SubFlowRuntime()
