"""Zero-LLM deterministic Rule Gateway protecting tokens, boundaries, and human overrides."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.rsi.models import EntityMemoryState
from app.rsi.schemas import RSIEventEnvelope, RuleGatewayDecision

DEFAULT_HIGH_RISK_KEYWORDS: Tuple[str, ...] = (
    "投诉",
    "举报",
    "工信部",
    "12315",
    "银保监",
    "消协",
    "骚扰",
    "别再打",
    "不要再打",
    "拉黑",
    "报警",
    "退订",
    "律师函",
)

DEFAULT_HIGH_INTENT_PAGES: Tuple[str, ...] = (
    "loan_calculator",
    "apply_form",
    "quote",
    "precheck",
    "pricing",
)

DEFAULT_HIGH_INTENT_EVENTS: Tuple[str, ...] = (
    "app.loan_simulated",
    "miniapp.active_high_intent",
    "app.quote_viewed",
)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_iso_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _to_utc(value)
    if isinstance(value, str) and value.strip():
        try:
            return _to_utc(datetime.fromisoformat(value.strip()))
        except ValueError:
            return None
    return None


def _extract_text_for_risk_scan(envelope: RSIEventEnvelope) -> str:
    chunks: List[str] = []
    for source in (envelope.facts or {}, envelope.extra or {}):
        if isinstance(source, dict):
            for key in ("text", "message", "transcript", "content", "reason"):
                val = source.get(key)
                if isinstance(val, str) and val.strip():
                    chunks.append(val.strip())
    if isinstance(envelope.summary, str) and envelope.summary.strip():
        chunks.append(envelope.summary.strip())
    return "\n".join(chunks)


def find_high_risk_keyword(
    text: str,
    extra_keywords: Optional[List[str]] = None,
) -> Optional[str]:
    if not text:
        return None
    lower_text = text.lower()
    keywords: List[str] = list(DEFAULT_HIGH_RISK_KEYWORDS)
    if extra_keywords:
        for kw in extra_keywords:
            if isinstance(kw, str) and kw.strip() and kw.strip() not in keywords:
                keywords.append(kw.strip())
    for kw in keywords:
        if kw.lower() in lower_text:
            return kw
    return None


def evaluate_rule_gateway(
    envelope: RSIEventEnvelope,
    state: EntityMemoryState,
    policy_config: Optional[Dict[str, Any]] = None,
) -> RuleGatewayDecision:
    """Classify event with 0 LLM tokens and produce deterministic scheduling/cancel rules."""
    cfg = dict(policy_config or {})
    facts = envelope.facts or {}
    occ = _to_utc(envelope.occurred_at)
    hb = state.hard_boundaries if isinstance(state.hard_boundaries, dict) else {}
    lc = state.lifecycle if isinstance(state.lifecycle, dict) else {}

    # 1. Causal loop / receipt guard
    if envelope.category == "action_receipt" or envelope.source == "rsi_outbox":
        return RuleGatewayDecision(
            tier="L0_SILENT",
            matched_rule="causal_loop_guard",
        )

    # 2. High-risk keyword / explicit blacklist / erasure guard
    if envelope.category == "risk_boundary" and (
        facts.get("blacklisted") is True
        or envelope.event_type in ("risk.blacklisted", "risk.complaint", "risk.erasure_requested")
    ):
        return RuleGatewayDecision(
            tier="L1_REFLEX_BLACKLIST",
            matched_rule=f"explicit:{envelope.event_type}",
            cancel_pending_tasks=True,
            blacklist_entity=True,
            blacklist_reason=str(facts.get("reason") or envelope.summary or envelope.event_type),
        )

    should_scan_text = envelope.actor_type == "customer" or (
        envelope.category == "voice" and envelope.actor_type in ("customer", "system")
    )
    if should_scan_text:
        matched_kw = find_high_risk_keyword(
            _extract_text_for_risk_scan(envelope),
            extra_keywords=cfg.get("high_risk_keywords"),
        )
        if matched_kw:
            return RuleGatewayDecision(
                tier="L1_REFLEX_BLACKLIST",
                matched_rule=f"high_risk_keyword:{matched_kw}",
                cancel_pending_tasks=True,
                blacklist_entity=True,
                blacklist_reason=f"high_risk_keyword:{matched_kw}",
            )

    # 3. Already blacklisted / erasure / DND entity guard
    if hb.get("blacklisted") or hb.get("erasure_requested"):
        return RuleGatewayDecision(
            tier="L1_REFLEX_CANCEL",
            matched_rule="entity_blacklisted",
            cancel_pending_tasks=True,
        )

    if hb.get("dnd") and envelope.actor_type != "customer":
        return RuleGatewayDecision(
            tier="L1_REFLEX_CANCEL",
            matched_rule="entity_dnd",
            cancel_pending_tasks=True,
        )

    # 4. Terminal conversion guard
    if (
        lc.get("converted") is True
        or envelope.event_type in ("order.paid", "order.completed", "lifecycle.converted")
    ) and not bool(cfg.get("allow_post_conversion_followup", False)):
        return RuleGatewayDecision(
            tier="L1_REFLEX_CANCEL",
            matched_rule="entity_converted_cancel",
            cancel_pending_tasks=True,
        )

    # 5. WeChat friend added guard
    if envelope.category == "wechat" and (
        envelope.event_type in ("wechat.friend_added", "wechat.contact_added")
        or facts.get("wechat_added") is True
    ):
        if bool(cfg.get("wakeup_on_wechat_added", False)):
            return RuleGatewayDecision(
                tier="L2_COGNITIVE_WAKEUP",
                matched_rule="wechat_added_handover",
                cancel_pending_tasks=True,
                schedule_wakeup=True,
                wakeup_delay_ms=0,
                wakeup_priority=8,
            )
        return RuleGatewayDecision(
            tier="L1_REFLEX_CANCEL",
            matched_rule="wechat_added_cancel",
            cancel_pending_tasks=True,
            schedule_wakeup=False,
        )

    # 6. Human takeover backoff
    if (
        envelope.category == "human_touch"
        and envelope.event_type != "human.takeover_ended"
        and facts.get("takeover") is not False
    ):
        if envelope.event_type == "human.takeover_started" or envelope.actor_type == "human_rep":
            takeover_until = _parse_iso_dt(facts.get("takeover_until"))
            if takeover_until is None:
                mins = int(facts.get("takeover_minutes") or cfg.get("human_takeover_minutes", 30))
                takeover_until = occ + timedelta(minutes=mins)
            return RuleGatewayDecision(
                tier="L1_REFLEX_TAKEOVER",
                matched_rule="human_takeover_backoff",
                cancel_pending_tasks=True,
                set_takeover_until=takeover_until,
            )

    if envelope.event_type == "human.takeover_ended":
        return RuleGatewayDecision(
            tier="L2_COGNITIVE_WAKEUP",
            matched_rule="human_takeover_resumed",
            schedule_wakeup=True,
            wakeup_delay_ms=0,
            wakeup_priority=7,
        )

    active_takeover_until = _parse_iso_dt(hb.get("human_takeover_until"))
    if active_takeover_until is not None and active_takeover_until > occ:
        return RuleGatewayDecision(
            tier="L1_REFLEX_TAKEOVER",
            matched_rule="human_takeover_active",
            schedule_wakeup=False,
        )

    # 7. App / MiniApp active engagement priority boost vs passive silent ingest
    if envelope.category == "content_engagement":
        high_intent_events = tuple(cfg.get("high_intent_events") or DEFAULT_HIGH_INTENT_EVENTS)
        high_intent_pages = tuple(cfg.get("high_intent_pages") or DEFAULT_HIGH_INTENT_PAGES)
        min_boost_score = int(cfg.get("min_boost_score", 70))
        page = str(facts.get("page") or facts.get("content") or "").strip()
        raw_score = facts.get("score")
        is_high_intent = (
            envelope.event_type in high_intent_events
            or (page and page in high_intent_pages)
            or (isinstance(raw_score, (int, float)) and raw_score >= min_boost_score)
            or facts.get("boost") is True
        )
        if is_high_intent:
            return RuleGatewayDecision(
                tier="L2_PRIORITY_BOOST",
                matched_rule="active_engagement_boost",
                schedule_wakeup=True,
                wakeup_delay_ms=0,
                wakeup_priority=9,
                boost_existing_tasks=True,
            )
        return RuleGatewayDecision(
            tier="L0_SILENT",
            matched_rule="passive_content_silent",
        )

    # 8. Commitment auto-timer
    if (
        envelope.category == "commitment"
        and facts.get("due_at") is not None
        and str(facts.get("status") or "open") == "open"
    ):
        return RuleGatewayDecision(
            tier="L0_SILENT",
            matched_rule="commitment_timer_scheduled",
            auto_create_commitment_timer=True,
        )

    # 9. Customer WeChat / messaging inbound (3-level debounce preemption path)
    if envelope.category in ("wechat", "messaging") and envelope.actor_type == "customer":
        debounce_ms = int(cfg.get("wechat_debounce_ms", 2500))
        return RuleGatewayDecision(
            tier="L2_COGNITIVE_WAKEUP",
            matched_rule="customer_inbound_messaging_debounce",
            schedule_wakeup=True,
            wakeup_delay_ms=max(0, debounce_ms),
            wakeup_priority=8,
        )

    # 10. Voice call completed & other cognitive events
    if envelope.category == "voice" and (
        envelope.event_type in ("voice.call_ended", "voice.call_unanswered", "voice.call_answered")
        or "answered" in facts
        or "outcome" in facts
    ):
        return RuleGatewayDecision(
            tier="L2_COGNITIVE_WAKEUP",
            matched_rule="voice_call_completed",
            schedule_wakeup=True,
            wakeup_delay_ms=0,
            wakeup_priority=7,
        )

    if envelope.category in ("churn_signal", "approval", "role_handoff"):
        return RuleGatewayDecision(
            tier="L2_COGNITIVE_WAKEUP",
            matched_rule=f"cognitive_{envelope.category}",
            schedule_wakeup=True,
            wakeup_delay_ms=0,
            wakeup_priority=7,
        )

    # 11. Default L0 silent memory ingest
    return RuleGatewayDecision(
        tier="L0_SILENT",
        matched_rule="silent_memory_ingest",
    )
