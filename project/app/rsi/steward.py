"""Top-level StewardAgent orchestrator for RSI V3.

Reads cross-day L2 deterministic state + L3 semantic working memory + active/completed
subflow snapshots and available subflow registry, then decides per wakeup batch whether to:
- delegate outbound call scheduling to CallPlannerAgent
- start/resume/hand-back a strongly typed SubFlowHandler via SubFlowRuntime
- send a gentle WeChat nudge via Global Outbox
- escalate to human rep via Global Outbox
- schedule a self-check monitor timer or noop.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from app.rsi.adapters.base import AdapterRegistry
from app.rsi.call_planner import CallPlannerAgent
from app.rsi.guardrails.outbox_guardrail import submit_action_proposal
from app.rsi.guardrails.time_window import to_utc
from app.rsi.identity import resolve_canonical_entity
from app.rsi.memory_dreamer import enqueue_memory_dream_task
from app.rsi.models import EntityMemoryState, EntityWorkingMemory, FollowupPolicy
from app.rsi.schemas import ActionProposal
from app.rsi.subflows.registry import SubFlowRegistry, subflow_registry
from app.rsi.subflows.runtime import (
    SubFlowRuntime,
    build_compact_context_index,
    resolve_default_llm_model,
    sanitize_llm_output,
)
from app.rsi.timers import schedule_timer

StewardRoute = Literal[
    "delegate_call_planner",
    "delegate_subflow",
    "send_wechat_nudge",
    "escalate_human",
    "wait_and_monitor",
    "noop",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def extract_batch_inbound_message(
    events: Sequence[Dict[str, Any]],
) -> Tuple[str, str, List[str]]:
    """Merge coalesced inbound customer message chunks, channel, and media URLs."""
    text_parts: List[str] = []
    media_urls: List[str] = []
    channel = "wechat"

    for ev in events:
        if not isinstance(ev, dict):
            continue
        cat = str(ev.get("category") or "")
        actor_type = str(ev.get("actor_type") or "")
        facts = ev.get("facts") if isinstance(ev.get("facts"), dict) else {}
        extra = ev.get("extra") if isinstance(ev.get("extra"), dict) else {}

        if cat in ("wechat", "messaging") or actor_type == "customer":
            ch_val = facts.get("channel") or extra.get("channel")
            if ch_val:
                channel = str(ch_val)
            elif cat in ("wechat", "messaging"):
                channel = cat

            txt = (
                facts.get("text")
                or facts.get("message")
                or facts.get("content")
                or ev.get("summary")
            )
            if isinstance(txt, str) and txt.strip():
                text_parts.append(txt.strip())

        for container in (facts, extra):
            m_list = container.get("media_urls") or container.get("attachments")
            if isinstance(m_list, list):
                for item in m_list:
                    if isinstance(item, str) and item.strip():
                        media_urls.append(item.strip())
                    elif isinstance(item, dict) and item.get("url"):
                        media_urls.append(str(item["url"]).strip())
            for single_key in ("image_url", "media_url", "file_url"):
                single_val = container.get(single_key)
                if isinstance(single_val, str) and single_val.strip():
                    media_urls.append(single_val.strip())

    # Preserve insertion order while deduplicating media URLs
    dedup_media = list(dict.fromkeys(media_urls))
    return "\n".join(text_parts), channel, dedup_media


class StewardDecision(BaseModel):
    """Strongly typed routing decision emitted by Pydantic-AI StewardAgent."""

    route: StewardRoute
    target_subflow_id: Optional[str] = None
    subflow_initial_slots: Dict[str, Any] = Field(default_factory=dict)
    wechat_message: str = ""
    handoff_reason: str = ""
    next_check_minutes: Optional[int] = Field(default=None, ge=1)
    priority: int = Field(default=5, ge=1, le=10)
    rationale: str = ""
    trigger_memory_dream: bool = False


class StewardExecutionResult(BaseModel):
    tenant_id: str
    entity_id: str
    route: StewardRoute
    decision: Optional[StewardDecision] = None
    used_llm: bool = True
    subflow_result: Optional[Dict[str, Any]] = None
    call_plan_result: Optional[Dict[str, Any]] = None
    follow_up_decision: Optional[Dict[str, Any]] = None
    outbox_receipts: List[Dict[str, Any]] = Field(default_factory=list)
    scheduled_task_id: Optional[int] = None
    notes: List[str] = Field(default_factory=list)


_STEWARD_SYSTEM_PROMPT = """你是客户长程跟进系统的顶层“大管家”主控 Agent（StewardAgent）。
你掌握客户跨天的 L2 确定性状态、L3 语义记忆与活跃/已完成子状态机结论，但不直接下场做复杂多轮填表。
请根据当前触发批次事件（通话结果/SIP挂断/App活跃埋点/微信连发消息/子状态机完工回交等），输出严格结构化的 StewardDecision：
1. 若涉及外呼排期/SIP 挂断码应对/活跃埋点回呼 -> route="delegate_call_planner"
2. 若当前已有活跃子流程或适合进入某个专业业务子流程收集资料/预审 -> route="delegate_subflow" 并指定 target_subflow_id
3. 若只需一次温和微信轻触达或推进提醒 -> route="send_wechat_nudge"
4. 若客户明确要求人工或涉及复杂投诉/风控异常 -> route="escalate_human"
5. 若当前无需立刻触达、仅设定时复查 -> route="wait_and_monitor" 并给出 next_check_minutes
6. 若事件已闭环或无需动作 -> route="noop"

禁止在文本字段中泄露任何 <think> 思考过程或违规承诺用语。"""


class StewardAgent:
    """Top-level RSI steward coordinating CallPlanner, SubFlowRuntime, Timers, and Outbox."""

    def __init__(
        self,
        *,
        model: Any = None,
        call_planner: Optional[CallPlannerAgent] = None,
        call_planner_model: Any = None,
        subflow_runtime: Optional[SubFlowRuntime] = None,
        subflow_model: Any = None,
        registry: Optional[SubFlowRegistry] = None,
        adapter_registry: Optional[AdapterRegistry] = None,
        dispatch_actions: bool = False,
        max_conclusion_hops: int = 1,
    ) -> None:
        self.model = model
        self.subflow_model = subflow_model
        self.registry = registry or subflow_registry
        self.adapter_registry = adapter_registry
        self.dispatch_actions = dispatch_actions
        self.max_conclusion_hops = max(0, int(max_conclusion_hops))
        self.call_planner = call_planner or CallPlannerAgent(
            model=call_planner_model or model,
            adapter_registry=adapter_registry,
        )
        self.subflow_runtime = subflow_runtime or SubFlowRuntime(
            registry=self.registry
        )

    def as_worker_handler(
        self,
        *,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]:
        """Return an async handler compatible with `RsiInboxWorker(cognitive_handler=...)`."""

        async def _handler(batch_context: Dict[str, Any]) -> Dict[str, Any]:
            now_dt = clock() if clock is not None else None
            return await self.handle_cognitive_batch(batch_context, now=now_dt)

        return _handler

    async def handle_cognitive_batch(
        self,
        batch_context: Dict[str, Any],
        *,
        now: Optional[datetime] = None,
        _hop: int = 0,
    ) -> Dict[str, Any]:
        check_now = to_utc(now) if now is not None else _utc_now()
        tenant_id = str(batch_context.get("tenant_id") or "default")
        entity_id = str(batch_context.get("entity_id") or "")
        events: List[Dict[str, Any]] = list(batch_context.get("events") or [])
        task_payload: Dict[str, Any] = dict(batch_context.get("task_payload") or {})

        canonical = await resolve_canonical_entity(tenant_id, entity_id)
        if canonical is None:
            return StewardExecutionResult(
                tenant_id=tenant_id,
                entity_id=entity_id,
                route="noop",
                used_llm=False,
                notes=["entity_not_found"],
            ).model_dump(mode="json")

        cid = canonical.entity_id
        mem = await EntityMemoryState.get_or_none(tenant_id=tenant_id, entity_id=cid)
        working = await EntityWorkingMemory.get_or_none(tenant_id=tenant_id, entity_id=cid)
        policy = await FollowupPolicy.get_or_none(
            tenant_id=tenant_id, policy_id=canonical.policy_id, is_active=True
        )
        if policy is None and canonical.policy_id != "default":
            policy = await FollowupPolicy.get_or_none(
                tenant_id=tenant_id, policy_id="default", is_active=True
            )

        hb = dict(mem.hard_boundaries or {}) if mem else {}
        lc = dict(mem.lifecycle or {}) if mem else {}

        # 0-LLM hard boundary guard
        if (
            hb.get("blacklisted")
            or hb.get("erasure_requested")
            or canonical.status == "blacklisted"
        ):
            return StewardExecutionResult(
                tenant_id=tenant_id,
                entity_id=cid,
                route="noop",
                used_llm=False,
                notes=["entity_blacklisted"],
            ).model_dump(mode="json")

        l2_dict = (
            {
                "lifecycle": lc,
                "hard_boundaries": hb,
                "commitments": list(mem.commitments or []),
                "ownership": dict(mem.ownership or {}),
                "channel_stats": dict(mem.channel_stats or {}),
            }
            if mem is not None
            else {}
        )
        l3_dict = (
            {
                "customer_profile_notes": dict(working.customer_profile_notes or {}),
                "rolling_digest": list(working.rolling_digest or []),
                "active_guidance": working.active_guidance or "",
            }
            if working is not None
            else {}
        )
        active_subflow_state = dict(lc.get("active_subflow_state") or {})
        completed_subflows = dict(lc.get("completed_subflows") or {})
        available_subflows = self.registry.list_specs()
        compact_index = build_compact_context_index(l2_dict, l3_dict)

        active_model = self.model or resolve_default_llm_model()
        agent = Agent(
            model=active_model,
            output_type=StewardDecision,
            system_prompt=_STEWARD_SYSTEM_PROMPT,
            defer_model_check=True,
        )
        prompt_payload = {
            "tenant_id": tenant_id,
            "entity_id": cid,
            "task_type": batch_context.get("task_type"),
            "task_payload": task_payload,
            "compact_context_index": compact_index,
            "active_subflow_state": active_subflow_state,
            "completed_subflows": completed_subflows,
            "available_subflows": available_subflows,
            "policy_id": canonical.policy_id,
            "policy_config": dict(policy.config or {}) if policy else {},
            "events": events,
            "hop": _hop,
        }
        res = await agent.run(json.dumps(prompt_payload, ensure_ascii=False))
        decision: StewardDecision = res.output

        # Unconditionally strip internal thinking tags from steward prose fields
        decision.wechat_message = sanitize_llm_output(decision.wechat_message)
        decision.handoff_reason = sanitize_llm_output(decision.handoff_reason)
        decision.rationale = sanitize_llm_output(decision.rationale)

        receipts: List[Dict[str, Any]] = []
        notes: List[str] = []
        subflow_res: Optional[Dict[str, Any]] = None
        call_plan_res: Optional[Dict[str, Any]] = None
        follow_up_decision: Optional[Dict[str, Any]] = None
        scheduled_task_id: Optional[int] = None
        corr_id = f"steward_{uuid.uuid4().hex[:12]}"

        if decision.route == "delegate_call_planner":
            plan_exec = await self.call_planner.plan_followup_call(
                tenant_id,
                cid,
                trigger_events=events,
                task_payload=task_payload,
                now=check_now,
                submit_to_outbox=True,
                dispatch_actions=self.dispatch_actions,
                correlation_id=corr_id,
            )
            call_plan_res = plan_exec.model_dump(mode="json")
            receipts.extend(plan_exec.outbox_receipts)

        elif decision.route == "delegate_subflow":
            target_sid = decision.target_subflow_id or active_subflow_state.get("subflow_id")
            if not target_sid or not self.registry.has(str(target_sid)):
                notes.append(f"unknown_subflow:{target_sid}")
            else:
                target_sid = str(target_sid)
                incoming_text, reply_channel, media_urls = extract_batch_inbound_message(events)
                is_starting_fresh = (
                    not active_subflow_state
                    or active_subflow_state.get("subflow_id") != target_sid
                    or active_subflow_state.get("status") in ("completed", "cancelled", "escalated")
                )

                if is_starting_fresh and not incoming_text and not media_urls:
                    subflow_res = await self.subflow_runtime.start_subflow(
                        tenant_id,
                        cid,
                        target_sid,
                        initial_slots=decision.subflow_initial_slots or None,
                        model=self.subflow_model,
                        emit_initial_opening=True,
                        dispatch_actions=self.dispatch_actions,
                        now=check_now,
                    )
                else:
                    if is_starting_fresh and decision.subflow_initial_slots:
                        await self.subflow_runtime.start_subflow(
                            tenant_id,
                            cid,
                            target_sid,
                            initial_slots=decision.subflow_initial_slots,
                            model=self.subflow_model,
                            emit_initial_opening=False,
                            dispatch_actions=False,
                            now=check_now,
                        )
                    subflow_res = await self.subflow_runtime.advance_entity_subflow(
                        tenant_id,
                        cid,
                        incoming_text=incoming_text,
                        incoming_channel=reply_channel,
                        media_urls=media_urls,
                        subflow_id=target_sid,
                        model=self.subflow_model,
                        dispatch_actions=self.dispatch_actions,
                        now=check_now,
                    )

                receipts.extend(list(subflow_res.get("action_receipts") or []))
                clean_reply = sanitize_llm_output(subflow_res.get("reply_text"))
                if clean_reply and not subflow_res.get("action_receipts"):
                    reply_seed = (
                        f"st:subflow:{tenant_id}:{cid}:{target_sid}:{corr_id}"
                    )
                    reply_prop = ActionProposal(
                        tenant_id=tenant_id,
                        entity_id=cid,
                        action_type="send_message",
                        channel=reply_channel or "wechat",
                        priority=decision.priority,
                        scheduled_at=check_now,
                        idempotency_key=f"st_{hashlib.sha256(reply_seed.encode()).hexdigest()[:24]}",
                        correlation_id=corr_id,
                        policy_id=canonical.policy_id,
                        payload={
                            "text": clean_reply,
                            "subflow_id": target_sid,
                            "node_id": subflow_res.get("node_id"),
                            "status": subflow_res.get("status"),
                        },
                    )
                    rec = await submit_action_proposal(
                        reply_prop,
                        adapter_registry=self.adapter_registry,
                        now=check_now,
                        dispatch_immediately=self.dispatch_actions,
                    )
                    receipts.append(rec.model_dump(mode="json"))

                # Persist typed conclusion and hop back to Steward for next-stage promotion
                if (
                    subflow_res.get("status") == "completed"
                    and subflow_res.get("conclusion") is not None
                ):
                    latest_mem = await EntityMemoryState.get_or_none(
                        tenant_id=tenant_id, entity_id=cid
                    )
                    if latest_mem is not None:
                        latest_lc = dict(latest_mem.lifecycle or {})
                        done_map = dict(latest_lc.get("completed_subflows") or {})
                        done_map[target_sid] = subflow_res["conclusion"]
                        latest_lc["completed_subflows"] = done_map
                        latest_mem.lifecycle = latest_lc
                        await latest_mem.save()

                    if _hop < self.max_conclusion_hops:
                        synthetic_ctx = {
                            "tenant_id": tenant_id,
                            "entity_id": cid,
                            "task_type": "subflow_completed",
                            "task_payload": {
                                "completed_subflow_id": target_sid,
                                "conclusion": subflow_res["conclusion"],
                            },
                            "events": [
                                {
                                    "event_id": f"subflow_done:{target_sid}:{corr_id}",
                                    "event_type": "subflow.completed",
                                    "category": "lifecycle",
                                    "occurred_at": check_now.isoformat(),
                                    "actor_type": "system",
                                    "summary": f"Subflow {target_sid} completed with conclusion",
                                    "facts": {
                                        "completed_subflow_id": target_sid,
                                        "conclusion": subflow_res["conclusion"],
                                    },
                                }
                            ],
                        }
                        follow_up_decision = await self.handle_cognitive_batch(
                            synthetic_ctx,
                            now=check_now,
                            _hop=_hop + 1,
                        )
                        receipts.extend(
                            list(follow_up_decision.get("outbox_receipts") or [])
                        )

        elif decision.route == "send_wechat_nudge":
            msg_text = (
                decision.wechat_message
                or "您好！方案这边随时为您保留，有问题欢迎随时微信沟通～"
            )
            seed = f"st:nudge:{tenant_id}:{cid}:{corr_id}"
            prop = ActionProposal(
                tenant_id=tenant_id,
                entity_id=cid,
                action_type="send_message",
                channel="wechat",
                priority=decision.priority,
                scheduled_at=check_now,
                idempotency_key=f"st_{hashlib.sha256(seed.encode()).hexdigest()[:24]}",
                correlation_id=corr_id,
                policy_id=canonical.policy_id,
                payload={"text": msg_text, "rationale": decision.rationale},
            )
            rec = await submit_action_proposal(
                prop,
                adapter_registry=self.adapter_registry,
                now=check_now,
                dispatch_immediately=self.dispatch_actions,
            )
            receipts.append(rec.model_dump(mode="json"))

        elif decision.route == "escalate_human":
            seed = f"st:escalate:{tenant_id}:{cid}:{corr_id}"
            prop = ActionProposal(
                tenant_id=tenant_id,
                entity_id=cid,
                action_type="escalate_human",
                channel="crm_handoff",
                priority=max(decision.priority, 9),
                scheduled_at=check_now,
                idempotency_key=f"st_{hashlib.sha256(seed.encode()).hexdigest()[:24]}",
                correlation_id=corr_id,
                policy_id=canonical.policy_id,
                payload={
                    "reason": decision.handoff_reason or decision.rationale or "steward_escalation",
                    "rationale": decision.rationale,
                },
            )
            rec = await submit_action_proposal(
                prop,
                adapter_registry=self.adapter_registry,
                now=check_now,
                dispatch_immediately=self.dispatch_actions,
            )
            receipts.append(rec.model_dump(mode="json"))

        elif decision.route == "wait_and_monitor":
            delay_mins = decision.next_check_minutes or 60
            task = await schedule_timer(
                tenant_id,
                cid,
                timer_type="timer_self_check",
                run_at=check_now + timedelta(minutes=delay_mins),
                payload={
                    "reason": decision.rationale or "steward_monitor",
                    "trigger": "steward_monitor",
                },
                priority=decision.priority,
                replace_existing=True,
            )
            scheduled_task_id = task.id

        # Optional asynchronous L3 dreaming trigger
        if decision.trigger_memory_dream:
            await enqueue_memory_dream_task(
                tenant_id, cid, run_at=check_now, notify=False
            )

        return StewardExecutionResult(
            tenant_id=tenant_id,
            entity_id=cid,
            route=decision.route,
            decision=decision,
            used_llm=True,
            subflow_result=subflow_res,
            call_plan_result=call_plan_res,
            follow_up_decision=follow_up_decision,
            outbox_receipts=receipts,
            scheduled_task_id=scheduled_task_id,
            notes=notes,
        ).model_dump(mode="json")
