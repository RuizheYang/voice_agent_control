"""Dynamic outbound voice CallPlannerAgent for RSI V3.

Translates SIP hangup dispositions, call transcripts (verbal reschedules),
shift-worker daytime miss patterns, and App/MiniApp active-engagement boosts
into strictly typed CallPlanDecision schedules and compound Outbox proposals
(e.g., T-5m pre-call SMS notice + city-matched voice call).
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from app.rsi.adapters.base import AdapterRegistry
from app.rsi.guardrails.outbox_guardrail import submit_action_proposal
from app.rsi.guardrails.time_window import (
    is_within_working_window,
    parse_time_str,
    resolve_working_window,
    snap_to_working_window,
    to_utc,
)
from app.rsi.identity import resolve_canonical_entity
from app.rsi.models import (
    EntityAlias,
    EntityEventLog,
    EntityMemoryState,
    EntityWorkingMemory,
    FollowupPolicy,
)
from app.rsi.schemas import ActionProposal
from app.rsi.subflows.runtime import (
    build_compact_context_index,
    resolve_default_llm_model,
    sanitize_llm_output,
)

SipDisposition = Literal[
    "connected",
    "instant_reject",
    "powered_off",
    "no_answer",
    "invalid_number",
    "voicemail",
    "unknown",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def classify_sip_disposition(
    facts: Optional[Dict[str, Any]],
    *,
    summary: str = "",
) -> SipDisposition:
    """Classify SIP hangup codes and cause strings into normalized dispositions."""
    data = facts if isinstance(facts, dict) else {}
    sip_code = (
        _safe_int(data.get("sip_code"))
        or _safe_int(data.get("sip_status_code"))
        or _safe_int(data.get("hangup_code"))
    )
    cause_text = " ".join(
        str(x)
        for x in (
            data.get("hangup_cause"),
            data.get("cause"),
            data.get("disposition"),
            data.get("outcome"),
            data.get("reason"),
            summary,
        )
        if x
    ).lower()

    answered = data.get("answered")
    duration_sec = _safe_float(data.get("duration_sec"))
    if duration_sec is None:
        duration_sec = _safe_float(data.get("billsec"))

    invalid_kws = (
        "invalid_number",
        "out_of_service",
        "unallocated",
        "number_not_in_service",
        "空号",
        "停机",
        "错号",
    )
    if sip_code in (404, 410, 604) or any(kw in cause_text for kw in invalid_kws):
        return "invalid_number"

    reject_kws = (
        "busy",
        "decline",
        "rejected",
        "instant_reject",
        "user_busy",
        "秒挂",
        "挂断",
        "拒接",
    )
    if sip_code in (486, 600, 603) or any(kw in cause_text for kw in reject_kws):
        return "instant_reject"
    if (
        answered is True
        and duration_sec is not None
        and 0 <= duration_sec <= 5
        and any(kw in cause_text for kw in ("reject", "hangup", "挂"))
    ):
        return "instant_reject"

    power_kws = (
        "powered_off",
        "switched_off",
        "power_off",
        "unreachable",
        "关机",
        "不在服务区",
        "无法接通",
    )
    if sip_code in (480, 503) or any(kw in cause_text for kw in power_kws):
        return "powered_off"

    no_answer_kws = (
        "no_answer",
        "ring_timeout",
        "timeout",
        "unanswered",
        "未接听",
        "无人接听",
        "响铃超时",
    )
    if sip_code in (408, 487) or any(kw in cause_text for kw in no_answer_kws):
        return "no_answer"

    if any(kw in cause_text for kw in ("voicemail", "语音信箱")):
        return "voicemail"

    outcome = str(data.get("outcome") or "").lower()
    if (
        answered is True
        or (duration_sec is not None and duration_sec > 5)
        or outcome in ("connected", "answered", "completed")
    ):
        return "connected"

    return "unknown"


def _extract_event_field(ev: Any, field: str, default: Any = None) -> Any:
    if isinstance(ev, dict):
        return ev.get(field, default)
    return getattr(ev, field, default)


def _parse_iso_in_tz(raw: str, tz: ZoneInfo) -> Optional[datetime]:
    text = (raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(timezone.utc)


def detect_shift_worker_pattern(
    voice_events: Sequence[Any],
    *,
    tz: ZoneInfo,
    threshold: int = 2,
) -> Tuple[int, bool]:
    """Detect customers who repeatedly miss daytime [09:00, 18:00) calls -> evening shift."""
    daytime_misses = 0
    for ev in voice_events:
        occ_raw = _extract_event_field(ev, "occurred_at")
        if occ_raw is None:
            continue
        if isinstance(occ_raw, str):
            occ_dt = _parse_iso_in_tz(occ_raw, tz)
            if occ_dt is None:
                continue
        else:
            occ_dt = to_utc(occ_raw)
        local_dt = occ_dt.astimezone(tz)
        if not (9 <= local_dt.hour < 18):
            continue
        facts = _extract_event_field(ev, "facts", {}) or {}
        summary = str(_extract_event_field(ev, "summary", "") or "")
        disp = classify_sip_disposition(facts, summary=summary)
        if disp in ("no_answer", "powered_off", "instant_reject"):
            daytime_misses += 1

    return daytime_misses, daytime_misses >= max(1, int(threshold))


def detect_active_engagement_signal(
    events: Sequence[Any],
    task_payload: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    """Detect high-intent App/MiniApp telemetry justifying immediate priority callback."""
    tp = task_payload if isinstance(task_payload, dict) else {}
    if tp.get("boosted") is True or tp.get("trigger") == "active_engagement":
        return True, str(tp.get("event_type") or "task_payload_boost")

    high_intent_types = (
        "app.loan_simulated",
        "miniapp.active_high_intent",
        "app.quote_viewed",
    )
    high_intent_pages = (
        "loan_calculator",
        "apply_form",
        "quote",
        "precheck",
        "pricing",
    )
    for ev in events:
        cat = str(_extract_event_field(ev, "category", "") or "")
        etype = str(_extract_event_field(ev, "event_type", "") or "")
        facts = _extract_event_field(ev, "facts", {}) or {}
        if cat == "content_engagement" or etype in high_intent_types:
            page = str(facts.get("page") or facts.get("content") or "").strip()
            score = _safe_float(facts.get("score"))
            if (
                etype in high_intent_types
                or facts.get("boost") is True
                or page in high_intent_pages
                or (score is not None and score >= 70)
            ):
                return True, f"{etype}:{page or 'active'}"
    return False, ""


class SipSignalSummary(BaseModel):
    disposition: SipDisposition = "unknown"
    sip_code: Optional[int] = None
    hangup_cause: str = ""
    duration_sec: Optional[float] = None
    answered: Optional[bool] = None
    daytime_miss_count: int = 0
    is_shift_worker_candidate: bool = False
    active_engagement_boost: bool = False
    active_engagement_detail: str = ""
    customer_city: Optional[str] = None
    phone_number: Optional[str] = None


class CallPlanDecision(BaseModel):
    """Strictly typed output returned by Pydantic-AI CallPlannerAgent."""

    decision: Literal[
        "schedule_call",
        "switch_to_wechat",
        "wait_and_retry",
        "escalate_human",
        "close_followup",
    ]
    scheduled_at_iso: Optional[str] = None
    delay_minutes: Optional[int] = Field(default=None, ge=0)
    priority: int = Field(default=5, ge=1, le=10)
    opening_script: str = ""
    fallback_channel: Optional[str] = None
    wechat_message: str = ""
    send_pre_call_sms: bool = False
    pre_call_sms_text: str = ""
    shift_to_evening: bool = False
    verbal_reschedule_detected: bool = False
    verbal_reschedule_quote: Optional[str] = None
    rationale: str = ""


class CallPlanExecutionResult(BaseModel):
    tenant_id: str
    entity_id: str
    decision: CallPlanDecision
    signals: SipSignalSummary
    resolved_scheduled_at: Optional[datetime] = None
    used_llm: bool = True
    proposals: List[ActionProposal] = Field(default_factory=list)
    outbox_receipts: List[Dict[str, Any]] = Field(default_factory=list)


def _compute_evening_slot(check_now: datetime, work_end: str, tz: ZoneInfo) -> datetime:
    now_local = check_now.astimezone(tz)
    end_t = parse_time_str(work_end)
    target_time = time(19, 0, 0)
    if end_t <= target_time:
        # Step 30 min inside work_end while preserving evening preference
        end_dt = datetime.combine(now_local.date(), end_t, tz) - timedelta(minutes=30)
        target_time = end_dt.time()
    candidate = datetime.combine(now_local.date(), target_time, tz)
    if candidate <= now_local + timedelta(minutes=15):
        candidate = datetime.combine(now_local.date() + timedelta(days=1), target_time, tz)
    return candidate.astimezone(timezone.utc)


def build_call_plan_proposals(
    *,
    tenant_id: str,
    entity_id: str,
    decision: CallPlanDecision,
    signals: SipSignalSummary,
    policy_id: str = "default",
    policy_config: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
    correlation_id: Optional[str] = None,
) -> Tuple[Optional[datetime], List[ActionProposal]]:
    """Convert CallPlanDecision + signals into compliant Outbox ActionProposals."""
    check_now = to_utc(now) if now is not None else _utc_now()
    cfg = policy_config if isinstance(policy_config, dict) else {}
    work_start, work_end, tz = resolve_working_window(cfg)
    corr_id = correlation_id or f"callplan_{uuid.uuid4().hex[:12]}"

    # Sanitize all LLM-originated strings
    clean_opening = sanitize_llm_output(decision.opening_script)
    clean_wechat = sanitize_llm_output(decision.wechat_message)
    clean_sms = sanitize_llm_output(decision.pre_call_sms_text)
    clean_rationale = sanitize_llm_output(decision.rationale)
    decision.opening_script = clean_opening
    decision.wechat_message = clean_wechat
    decision.pre_call_sms_text = clean_sms
    decision.rationale = clean_rationale

    if decision.decision == "close_followup":
        return None, []

    if decision.decision == "switch_to_wechat":
        text = clean_wechat or "您好，刚才电话未能接通，微信上为您同步一份产品方案说明，方便时随时回复我～"
        key_seed = f"cp:wechat:{tenant_id}:{entity_id}:{corr_id}"
        return check_now, [
            ActionProposal(
                tenant_id=tenant_id,
                entity_id=entity_id,
                action_type="send_message",
                channel="wechat",
                priority=decision.priority,
                scheduled_at=check_now,
                idempotency_key=f"cp_{hashlib.sha256(key_seed.encode()).hexdigest()[:24]}",
                correlation_id=corr_id,
                policy_id=policy_id,
                payload={
                    "text": text,
                    "sip_disposition": signals.disposition,
                    "reason": clean_rationale,
                },
            )
        ]

    if decision.decision == "escalate_human":
        key_seed = f"cp:human:{tenant_id}:{entity_id}:{corr_id}"
        return check_now, [
            ActionProposal(
                tenant_id=tenant_id,
                entity_id=entity_id,
                action_type="escalate_human",
                channel="crm_handoff",
                priority=max(decision.priority, 9),
                scheduled_at=check_now,
                idempotency_key=f"cp_{hashlib.sha256(key_seed.encode()).hexdigest()[:24]}",
                correlation_id=corr_id,
                policy_id=policy_id,
                payload={
                    "reason": clean_rationale or f"sip_disposition:{signals.disposition}",
                    "sip_disposition": signals.disposition,
                    "customer_city": signals.customer_city,
                },
            )
        ]

    # schedule_call or wait_and_retry
    effective_priority = decision.priority
    if signals.active_engagement_boost:
        effective_priority = max(effective_priority, 9)

    if (
        signals.active_engagement_boost
        and decision.decision == "schedule_call"
        and not decision.verbal_reschedule_detected
    ):
        target_dt = check_now
    else:
        parsed_iso = (
            _parse_iso_in_tz(decision.scheduled_at_iso, tz)
            if decision.scheduled_at_iso
            else None
        )
        if parsed_iso is not None:
            target_dt = parsed_iso
        elif decision.delay_minutes is not None:
            target_dt = check_now + timedelta(minutes=decision.delay_minutes)
        else:
            fallback_hours = {
                "instant_reject": 24,
                "powered_off": 4,
                "no_answer": 2,
                "voicemail": 3,
            }.get(signals.disposition, 3)
            target_dt = check_now + timedelta(hours=fallback_hours)

    if (
        signals.is_shift_worker_candidate
        and not decision.verbal_reschedule_detected
        and not signals.active_engagement_boost
    ):
        target_dt = _compute_evening_slot(check_now, work_end, tz)
        decision.shift_to_evening = True

    buffer_mins = int(cfg.get("morning_buffer_minutes", 30))
    target_dt = snap_to_working_window(
        target_dt, work_start, work_end, tz, morning_buffer_minutes=buffer_mins
    )

    proposals: List[ActionProposal] = []
    enable_sms = bool(decision.send_pre_call_sms or cfg.get("enable_pre_call_sms", True))
    will_send_sms = enable_sms and target_dt >= check_now + timedelta(minutes=6)

    if will_send_sms:
        sms_dt = target_dt - timedelta(minutes=5)
        if not is_within_working_window(sms_dt, work_start, work_end, tz):
            sms_dt = snap_to_working_window(
                sms_dt, work_start, work_end, tz, morning_buffer_minutes=0
            )
            target_dt = snap_to_working_window(
                sms_dt + timedelta(minutes=5),
                work_start,
                work_end,
                tz,
                morning_buffer_minutes=0,
            )

        sms_text = (
            clean_sms
            or "您好，您的专属汽车金融顾问将在5分钟后使用官方固话致电为您服务，请注意接听。"
        )
        sms_seed = f"cp:sms:{tenant_id}:{entity_id}:{corr_id}"
        proposals.append(
            ActionProposal(
                tenant_id=tenant_id,
                entity_id=entity_id,
                action_type="send_pre_call_sms",
                channel="sms",
                priority=max(effective_priority, 7),
                scheduled_at=sms_dt,
                idempotency_key=f"cp_{hashlib.sha256(sms_seed.encode()).hexdigest()[:24]}",
                correlation_id=corr_id,
                policy_id=policy_id,
                payload={
                    "text": sms_text,
                    "city": signals.customer_city,
                    "phone_number": signals.phone_number,
                    "target_call_at": target_dt.isoformat(),
                    "notice_lead_minutes": 5,
                },
            )
        )

    voice_seed = f"cp:voice:{tenant_id}:{entity_id}:{corr_id}"
    proposals.append(
        ActionProposal(
            tenant_id=tenant_id,
            entity_id=entity_id,
            action_type="schedule_call",
            channel="voice",
            adapter_name="voice",
            priority=effective_priority,
            scheduled_at=target_dt,
            idempotency_key=f"cp_{hashlib.sha256(voice_seed.encode()).hexdigest()[:24]}",
            correlation_id=corr_id,
            policy_id=policy_id,
            payload={
                "city": signals.customer_city,
                "phone_number": signals.phone_number,
                "opening_script": clean_opening or "您好，这里是汽车金融专属顾问，刚才了解您的情况后跟您进一步沟通一下方案～",
                "sip_disposition": signals.disposition,
                "sip_code": signals.sip_code,
                "shift_to_evening": decision.shift_to_evening,
                "active_engagement_boost": signals.active_engagement_boost,
                "verbal_reschedule_detected": decision.verbal_reschedule_detected,
                "verbal_reschedule_quote": decision.verbal_reschedule_quote,
                "pre_call_sms_attached": will_send_sms,
                "reason": clean_rationale,
            },
        )
    )
    return target_dt, proposals


_CALL_PLANNER_SYSTEM_PROMPT = """你是一位精通外呼节奏掌控的 AI 动态排期决策专家（CallPlanner）。
请基于：
1) 客户 L2/L3 画像与历史顾虑；
2) 最近通话转写（识别口头改约如“明天下午下班再打”、“晚上8点打”等并结合 current_local_time 解析出准确的 ISO 时间戳 scheduled_at_iso）；
3) 底层 SIP 挂断码（区分 instant_reject 秒挂拒接/486/603、powered_off 关机/480、no_answer 未接听/408、connected）；
4) 倒班作息特征（is_shift_worker_candidate=true 时优先建议 19:00 晚间补呼且设 shift_to_evening=true）；
5) App/小程序活跃埋点（active_engagement_boost=true 时高优即时回呼 priority>=9）。

请输出严格结构化的 CallPlanDecision，切勿泄露任何 <think> 标签或违反合规范畴的承诺词。"""


class CallPlannerAgent:
    """Plans dynamic outbound voice redials, channel fallbacks, and pre-call SMS."""

    def __init__(
        self,
        *,
        model: Any = None,
        adapter_registry: Optional[AdapterRegistry] = None,
    ) -> None:
        self.model = model
        self.adapter_registry = adapter_registry

    async def plan_followup_call(
        self,
        tenant_id: str,
        entity_id: str,
        *,
        trigger_events: Optional[Sequence[Dict[str, Any]]] = None,
        task_payload: Optional[Dict[str, Any]] = None,
        model: Any = None,
        now: Optional[datetime] = None,
        submit_to_outbox: bool = True,
        dispatch_actions: bool = False,
        correlation_id: Optional[str] = None,
    ) -> CallPlanExecutionResult:
        check_now = to_utc(now) if now is not None else _utc_now()
        canonical = await resolve_canonical_entity(tenant_id, entity_id)
        if canonical is None:
            raise ValueError(f"Unknown entity {entity_id!r} for tenant {tenant_id!r}")
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
        policy_cfg = dict(policy.config or {}) if policy else {}
        _, _, tz = resolve_working_window(policy_cfg)

        recent_voice = (
            await EntityEventLog.filter(tenant_id=tenant_id, entity_id=cid, category="voice")
            .order_by("-occurred_at", "-id")
            .limit(20)
            .all()
        )
        trig_list = list(trigger_events or [])
        all_voice_for_pattern: List[Any] = list(reversed(recent_voice))
        existing_voice_ids = {r.event_id for r in recent_voice}
        for ev in trig_list:
            if (
                ev.get("category") == "voice"
                and ev.get("event_id")
                and ev.get("event_id") not in existing_voice_ids
            ):
                all_voice_for_pattern.append(ev)

        shift_count, is_shift_worker = detect_shift_worker_pattern(
            all_voice_for_pattern, tz=tz
        )
        boosted, boost_detail = detect_active_engagement_signal(
            trig_list, task_payload=task_payload
        )

        # Latest voice signal from trigger_events (preferred) or most recent L1 voice row
        latest_voice: Optional[Any] = None
        for ev in reversed(trig_list):
            if ev.get("category") == "voice":
                latest_voice = ev
                break
        if latest_voice is None and recent_voice:
            latest_voice = recent_voice[0]

        v_facts = (_extract_event_field(latest_voice, "facts", {}) or {}) if latest_voice else {}
        v_extra = (_extract_event_field(latest_voice, "extra", {}) or {}) if latest_voice else {}
        v_summary = str(_extract_event_field(latest_voice, "summary", "") or "") if latest_voice else ""

        disp = classify_sip_disposition(v_facts, summary=v_summary) if latest_voice else "unknown"
        sip_code = (
            _safe_int(v_facts.get("sip_code"))
            or _safe_int(v_facts.get("sip_status_code"))
            or _safe_int(v_facts.get("hangup_code"))
        )
        cause = str(
            v_facts.get("hangup_cause")
            or v_facts.get("cause")
            or v_facts.get("disposition")
            or ""
        )

        # Resolve city & phone number safely across trigger -> working notes -> aliases
        l3_notes = dict(working.customer_profile_notes or {}) if working else {}
        customer_city = (
            v_facts.get("city")
            or v_extra.get("city")
            or l3_notes.get("city")
        )
        if not customer_city:
            for ev in trig_list:
                f = ev.get("facts") or {}
                if f.get("city"):
                    customer_city = f.get("city")
                    break

        phone_number = v_facts.get("phone_number") or v_extra.get("phone_number")
        if not phone_number:
            alias = await EntityAlias.filter(
                tenant_id=tenant_id, entity_id=cid, key_type="phone"
            ).first()
            if alias is not None:
                phone_number = alias.key_value

        signals = SipSignalSummary(
            disposition=disp,
            sip_code=sip_code,
            hangup_cause=cause,
            duration_sec=_safe_float(v_facts.get("duration_sec")),
            answered=v_facts.get("answered"),
            daytime_miss_count=shift_count,
            is_shift_worker_candidate=is_shift_worker,
            active_engagement_boost=boosted,
            active_engagement_detail=boost_detail,
            customer_city=str(customer_city) if customer_city else None,
            phone_number=str(phone_number) if phone_number else None,
        )

        l2_dict = (
            {
                "lifecycle": dict(mem.lifecycle or {}),
                "hard_boundaries": dict(mem.hard_boundaries or {}),
                "commitments": list(mem.commitments or []),
                "ownership": dict(mem.ownership or {}),
                "channel_stats": dict(mem.channel_stats or {}),
            }
            if mem is not None
            else {}
        )
        l3_dict = (
            {
                "customer_profile_notes": l3_notes,
                "rolling_digest": list(working.rolling_digest or []),
                "active_guidance": working.active_guidance or "",
            }
            if working is not None
            else {}
        )
        compact_index = build_compact_context_index(l2_dict, l3_dict)

        # Deterministic 0-LLM fast-path for invalid_number
        if signals.disposition == "invalid_number":
            wechat_added = bool((l2_dict.get("channel_stats") or {}).get("wechat_added"))
            decision = CallPlanDecision(
                decision="switch_to_wechat" if wechat_added else "close_followup",
                wechat_message="您好，刚才致电号码提示异常，通过微信继续为您跟进办理进度～"
                if wechat_added
                else "",
                rationale=f"invalid_number:{signals.sip_code or signals.hangup_cause}",
            )
            used_llm = False
        else:
            active_model = model or self.model or resolve_default_llm_model()
            agent = Agent(
                model=active_model,
                output_type=CallPlanDecision,
                system_prompt=_CALL_PLANNER_SYSTEM_PROMPT,
                defer_model_check=True,
            )
            prompt_payload = {
                "tenant_id": tenant_id,
                "entity_id": cid,
                "current_utc": check_now.isoformat(),
                "current_local_time": check_now.astimezone(tz).isoformat(),
                "timezone": str(tz),
                "compact_context_index": compact_index,
                "sip_signals": signals.model_dump(mode="json"),
                "trigger_events": trig_list,
                "recent_voice_history": [
                    {
                        "event_id": r.event_id,
                        "occurred_at": r.occurred_at.isoformat() if r.occurred_at else None,
                        "summary": r.summary,
                        "facts": r.facts or {},
                    }
                    for r in recent_voice[:6]
                ],
            }
            res = await agent.run(json.dumps(prompt_payload, ensure_ascii=False))
            decision = res.output
            used_llm = True

        resolved_dt, proposals = build_call_plan_proposals(
            tenant_id=tenant_id,
            entity_id=cid,
            decision=decision,
            signals=signals,
            policy_id=canonical.policy_id or "default",
            policy_config=policy_cfg,
            now=check_now,
            correlation_id=correlation_id,
        )

        receipts: List[Dict[str, Any]] = []
        if submit_to_outbox and proposals:
            for prop in proposals:
                rec = await submit_action_proposal(
                    prop,
                    adapter_registry=self.adapter_registry,
                    now=check_now,
                    dispatch_immediately=dispatch_actions,
                )
                receipts.append(rec.model_dump(mode="json"))

        return CallPlanExecutionResult(
            tenant_id=tenant_id,
            entity_id=cid,
            decision=decision,
            signals=signals,
            resolved_scheduled_at=resolved_dt,
            used_llm=used_llm,
            proposals=proposals,
            outbox_receipts=receipts,
        )
