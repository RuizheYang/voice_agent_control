"""Offline RSI Optimization Engine: attribution review, edit clustering & Shadow Mode."""

from __future__ import annotations

import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from app.rsi.call_planner import (
    CallPlanDecision,
    SipSignalSummary,
    build_call_plan_proposals,
    classify_sip_disposition,
)
from app.rsi.guardrails.outbox_guardrail import evaluate_outbox_guardrails
from app.rsi.guardrails.time_window import to_utc
from app.rsi.models import (
    EntityEventLog,
    EntityMemoryState,
    FollowupEntity,
    FollowupPolicy,
    RsiFeedbackRecord,
    RsiShadowEvalRun,
)
from app.rsi.rule_gateway import evaluate_rule_gateway
from app.rsi.schemas import ActionProposal, RSIEventEnvelope


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PolicyVersionMetrics(BaseModel):
    policy_id: str
    policy_version: str
    script_version: str
    total_records: int = 0
    conversion_total: int = 0
    converted_count: int = 0
    conversion_rate: float = 0.0
    call_total: int = 0
    call_connected: int = 0
    call_connect_rate: float = 0.0
    message_total: int = 0
    message_replied: int = 0
    message_reply_rate: float = 0.0
    human_edit_count: int = 0
    avg_edit_ratio: float = 0.0
    guardrail_block_count: int = 0
    top_guardrail_rules: Dict[str, int] = Field(default_factory=dict)


class HumanEditCluster(BaseModel):
    tag: str
    count: int
    avg_edit_ratio: float
    top_removed_phrases: List[str] = Field(default_factory=list)
    top_added_phrases: List[str] = Field(default_factory=list)
    sample_examples: List[Dict[str, str]] = Field(default_factory=list)


class OptimizationSuggestion(BaseModel):
    category: str
    priority: int = Field(default=5, ge=1, le=10)
    target_policy_id: str
    target_version: Optional[str] = None
    summary: str
    recommended_action: str
    evidence: Dict[str, Any] = Field(default_factory=dict)


class OfflineReviewReport(BaseModel):
    tenant_id: str
    window_days: int
    generated_at: datetime
    total_records: int
    by_version: List[PolicyVersionMetrics] = Field(default_factory=list)
    edit_clusters: List[HumanEditCluster] = Field(default_factory=list)
    suggestions: List[OptimizationSuggestion] = Field(default_factory=list)


class ShadowEvalRequest(BaseModel):
    tenant_id: str = Field(default="default", min_length=1, max_length=64)
    baseline_policy_id: str = Field(default="default", min_length=1, max_length=128)
    baseline_version: str = Field(default="1.0", max_length=32)
    baseline_policy_config: Optional[Dict[str, Any]] = None
    candidate_policy_id: Optional[str] = None
    candidate_version: str = Field(default="candidate", max_length=32)
    candidate_policy_config: Dict[str, Any] = Field(default_factory=dict)
    entity_ids: Optional[List[str]] = None
    sample_limit: int = Field(default=20, ge=1, le=200)
    probe_scheduled_at: Optional[datetime] = None
    probe_channel: str = Field(default="voice", max_length=64)
    probe_action_type: str = Field(default="schedule_call", max_length=64)


class ShadowSampleDiff(BaseModel):
    entity_id: str
    event_id: Optional[str] = None
    baseline_rule_tier: str
    candidate_rule_tier: str
    baseline_matched_rule: str
    candidate_matched_rule: str
    baseline_verdict: str
    baseline_verdict_rule: str
    baseline_scheduled_at: Optional[datetime] = None
    baseline_pre_call_sms: bool = False
    candidate_verdict: str
    candidate_verdict_rule: str
    candidate_scheduled_at: Optional[datetime] = None
    candidate_pre_call_sms: bool = False
    diverged: bool = False
    divergence_reasons: List[str] = Field(default_factory=list)


class ShadowEvalResponse(BaseModel):
    eval_id: str
    tenant_id: str
    baseline_policy_id: str
    baseline_version: str
    candidate_policy_id: str
    candidate_version: str
    sample_size: int
    diverged_count: int
    zero_outbound_enforced: bool = True
    summary: Dict[str, Any] = Field(default_factory=dict)
    samples: List[ShadowSampleDiff] = Field(default_factory=list)
    created_at: datetime


async def generate_offline_review_report(
    tenant_id: str = "default",
    *,
    policy_id: Optional[str] = None,
    window_days: int = 30,
    now: Optional[datetime] = None,
) -> OfflineReviewReport:
    """Aggregate outcomes, edit diffs & guardrail blocks into optimization suggestions."""
    check_now = to_utc(now) if now is not None else _utc_now()
    cutoff = check_now - timedelta(days=max(1, int(window_days)))

    qs = RsiFeedbackRecord.filter(tenant_id=tenant_id, occurred_at__gte=cutoff)
    if policy_id:
        qs = qs.filter(policy_id=policy_id)
    rows = await qs.order_by("occurred_at", "id").all()

    grouped: Dict[Tuple[str, str, str], List[RsiFeedbackRecord]] = defaultdict(list)
    for r in rows:
        grouped[(r.policy_id, r.policy_version, r.script_version)].append(r)

    metrics_list: List[PolicyVersionMetrics] = []
    for (pid, pver, sver), items in sorted(grouped.items()):
        conv_items = [x for x in items if x.feedback_type == "conversion_outcome"]
        conv_ok = [
            x
            for x in conv_items
            if str(x.outcome or "").lower()
            in ("converted", "won", "approved", "paid", "success")
        ]
        call_items = [x for x in items if x.feedback_type == "call_connect"]
        call_ok = [
            x
            for x in call_items
            if str(x.outcome or "").lower()
            in ("connected", "answered", "rescheduled", "success")
        ]
        msg_items = [x for x in items if x.feedback_type == "message_reply"]
        msg_ok = [
            x
            for x in msg_items
            if str(x.outcome or "").lower() in ("replied", "engaged", "success")
        ]
        edits = [x for x in items if x.feedback_type == "human_edit"]
        edit_ratios = [
            float((x.human_edit_diff or {}).get("edit_ratio", 0.0)) for x in edits
        ]
        blocks = [x for x in items if x.feedback_type == "guardrail_block"]
        rule_counts = Counter(
            str(x.guardrail_rule or "unknown") for x in blocks
        )

        metrics_list.append(
            PolicyVersionMetrics(
                policy_id=pid,
                policy_version=pver,
                script_version=sver,
                total_records=len(items),
                conversion_total=len(conv_items),
                converted_count=len(conv_ok),
                conversion_rate=round(len(conv_ok) / len(conv_items), 4)
                if conv_items
                else 0.0,
                call_total=len(call_items),
                call_connected=len(call_ok),
                call_connect_rate=round(len(call_ok) / len(call_items), 4)
                if call_items
                else 0.0,
                message_total=len(msg_items),
                message_replied=len(msg_ok),
                message_reply_rate=round(len(msg_ok) / len(msg_items), 4)
                if msg_items
                else 0.0,
                human_edit_count=len(edits),
                avg_edit_ratio=round(sum(edit_ratios) / len(edit_ratios), 4)
                if edit_ratios
                else 0.0,
                guardrail_block_count=len(blocks),
                top_guardrail_rules=dict(rule_counts),
            )
        )

    # Cluster human_edit rows by semantic edit tags
    tag_buckets: Dict[str, List[RsiFeedbackRecord]] = defaultdict(list)
    for r in rows:
        if r.feedback_type != "human_edit":
            continue
        diff = dict(r.human_edit_diff or {})
        tags = list(diff.get("edit_tags") or [])
        if not tags and diff.get("changed"):
            tags = ["minor_polish"]
        for tag in tags:
            tag_buckets[str(tag)].append(r)

    edit_clusters: List[HumanEditCluster] = []
    for tag, bucket in sorted(
        tag_buckets.items(), key=lambda kv: (-len(kv[1]), kv[0])
    ):
        removed_counter: Counter[str] = Counter()
        added_counter: Counter[str] = Counter()
        ratios: List[float] = []
        examples: List[Dict[str, str]] = []

        for r in bucket:
            diff = dict(r.human_edit_diff or {})
            ratios.append(float(diff.get("edit_ratio", 0.0)))
            for seg in diff.get("removed_segments") or []:
                if isinstance(seg, str) and seg.strip():
                    removed_counter[seg.strip()] += 1
            for seg in diff.get("added_segments") or []:
                if isinstance(seg, str) and seg.strip():
                    added_counter[seg.strip()] += 1
            if len(examples) < 3 and r.original_text is not None and r.edited_text is not None:
                examples.append(
                    {
                        "original": r.original_text,
                        "edited": r.edited_text,
                        "script_version": r.script_version,
                    }
                )

        edit_clusters.append(
            HumanEditCluster(
                tag=tag,
                count=len(bucket),
                avg_edit_ratio=round(sum(ratios) / len(ratios), 4) if ratios else 0.0,
                top_removed_phrases=[p for p, _ in removed_counter.most_common(5)],
                top_added_phrases=[p for p, _ in added_counter.most_common(5)],
                sample_examples=examples,
            )
        )

    suggestions: List[OptimizationSuggestion] = []
    # 1) Compliance guardrail recommendations
    for m in metrics_list:
        if m.guardrail_block_count > 0:
            top_rule = max(m.top_guardrail_rules.items(), key=lambda kv: kv[1])[0]
            suggestions.append(
                OptimizationSuggestion(
                    category="compliance_guardrail",
                    priority=9,
                    target_policy_id=m.policy_id,
                    target_version=f"{m.policy_version}/{m.script_version}",
                    summary=(
                        f"{m.policy_id} ({m.policy_version}/{m.script_version}) 触发合规拦截 "
                        f"{m.guardrail_block_count} 次（高频规则: {top_rule}）"
                    ),
                    recommended_action="更新外呼与微信话术模板，移除承诺性高危用语并用影子模式回归验证拦截率清零。",
                    evidence={"top_guardrail_rules": m.top_guardrail_rules},
                )
            )

    # 2) Script tone recommendations from human edit clusters
    if edit_clusters:
        top_cluster = edit_clusters[0]
        suggestions.append(
            OptimizationSuggestion(
                category="script_tone",
                priority=8 if top_cluster.count >= 2 else 6,
                target_policy_id=policy_id or (metrics_list[0].policy_id if metrics_list else "default"),
                summary=(
                    f"人工改稿高频模式 '{top_cluster.tag}' 出现 {top_cluster.count} 次"
                    f"（平均改动率 {top_cluster.avg_edit_ratio:.0%}）"
                ),
                recommended_action=(
                    f"建议沉淀候选话术模板：精简/替换 {top_cluster.top_removed_phrases[:3]}，"
                    f"前置融入高频保留表达 {top_cluster.top_added_phrases[:3]}。"
                ),
                evidence={
                    "tag": top_cluster.tag,
                    "top_removed": top_cluster.top_removed_phrases[:3],
                    "top_added": top_cluster.top_added_phrases[:3],
                },
            )
        )

    # 3) Cadence timing suggestions for low call connect rate
    for m in metrics_list:
        if m.call_total >= 3 and m.call_connect_rate < 0.35:
            suggestions.append(
                OptimizationSuggestion(
                    category="cadence_timing",
                    priority=7,
                    target_policy_id=m.policy_id,
                    target_version=f"{m.policy_version}/{m.script_version}",
                    summary=(
                        f"策略 {m.policy_id} ({m.policy_version}) 接通率偏低: "
                        f"{m.call_connected}/{m.call_total} ({m.call_connect_rate:.1%})"
                    ),
                    recommended_action="建议开启倒班晚间补呼时段(19:00)与呼前5分钟短信预告以提升接通率。",
                    evidence={
                        "call_total": m.call_total,
                        "call_connected": m.call_connected,
                        "call_connect_rate": m.call_connect_rate,
                    },
                )
            )

    # 4) Conversion winner identification across >=2 variants
    with_conv = [m for m in metrics_list if m.conversion_total >= 2]
    if len(with_conv) >= 2:
        ordered = sorted(with_conv, key=lambda x: (-x.conversion_rate, -x.converted_count))
        best, worst = ordered[0], ordered[-1]
        if best.conversion_rate > worst.conversion_rate:
            lift = round(best.conversion_rate - worst.conversion_rate, 4)
            suggestions.append(
                OptimizationSuggestion(
                    category="conversion_winner",
                    priority=8,
                    target_policy_id=best.policy_id,
                    target_version=f"{best.policy_version}/{best.script_version}",
                    summary=(
                        f"转化胜出版本为 {best.policy_version}/{best.script_version} "
                        f"({best.conversion_rate:.1%})，较 {worst.policy_version}/{worst.script_version} "
                        f"({worst.conversion_rate:.1%}) 高 +{lift:.1%}"
                    ),
                    recommended_action="建议在确认风控拦截清零后，将流量灰度切换至胜出策略/话术版本。",
                    evidence={
                        "winner": f"{best.policy_version}/{best.script_version}",
                        "winner_rate": best.conversion_rate,
                        "runner_down": f"{worst.policy_version}/{worst.script_version}",
                        "runner_down_rate": worst.conversion_rate,
                    },
                )
            )

    return OfflineReviewReport(
        tenant_id=tenant_id,
        window_days=window_days,
        generated_at=check_now,
        total_records=len(rows),
        by_version=metrics_list,
        edit_clusters=edit_clusters,
        suggestions=suggestions,
    )


def _envelope_from_log_or_probe(
    entity: FollowupEntity,
    latest_event: Optional[EntityEventLog],
    check_now: datetime,
) -> RSIEventEnvelope:
    if latest_event is not None:
        return RSIEventEnvelope(
            tenant_id=entity.tenant_id,
            event_id=latest_event.event_id,
            entity_id=entity.entity_id,
            policy_id=latest_event.policy_id or entity.policy_id,
            event_type=latest_event.event_type,
            category=latest_event.category,  # type: ignore[arg-type]
            occurred_at=to_utc(latest_event.occurred_at),
            source=latest_event.source,
            actor_type=latest_event.actor_type,  # type: ignore[arg-type]
            summary=latest_event.summary or "",
            facts=dict(latest_event.facts or {}),
            extra=dict(latest_event.extra or {}),
        )
    return RSIEventEnvelope(
        tenant_id=entity.tenant_id,
        event_id=f"shadow_probe_{entity.entity_id}",
        entity_id=entity.entity_id,
        policy_id=entity.policy_id,
        event_type="voice.call_ended",
        category="voice",
        occurred_at=check_now,
        facts={"sip_code": 408, "hangup_cause": "no_answer"},
    )


def _evaluate_policy_proposal_in_memory(
    *,
    entity: FollowupEntity,
    envelope: RSIEventEnvelope,
    cfg: Dict[str, Any],
    policy_id: str,
    probe_scheduled_at: Optional[datetime],
    probe_channel: str,
    probe_action_type: str,
    check_now: datetime,
) -> Tuple[Optional[datetime], bool, ActionProposal]:
    facts = dict(envelope.facts or {})
    disp = classify_sip_disposition(facts, summary=envelope.summary or "")
    signals = SipSignalSummary(
        disposition=disp if disp != "unknown" else "no_answer",
        sip_code=facts.get("sip_code") if isinstance(facts.get("sip_code"), int) else None,
        customer_city=facts.get("city"),
        phone_number=facts.get("phone_number"),
    )

    draft_text = str(cfg.get("candidate_draft_text") or cfg.get("default_opening_script") or "您好，这里是专属顾问按约跟进您的方案进度～")
    if probe_channel != "voice":
        sched_at = to_utc(probe_scheduled_at) if probe_scheduled_at else check_now
        prop = ActionProposal(
            tenant_id=entity.tenant_id,
            entity_id=entity.entity_id,
            action_type=probe_action_type or "send_message",
            channel=probe_channel,
            policy_id=policy_id,
            scheduled_at=sched_at,
            payload={"text": draft_text},
        )
        return sched_at, False, prop

    raw_req_dt = probe_scheduled_at
    iso_override = raw_req_dt.isoformat() if raw_req_dt is not None else None
    decision = CallPlanDecision(
        decision="schedule_call",
        scheduled_at_iso=iso_override,
        delay_minutes=120 if iso_override is None else None,
        opening_script=draft_text,
        send_pre_call_sms=bool(cfg.get("enable_pre_call_sms", True)),
    )
    resolved_dt, proposals = build_call_plan_proposals(
        tenant_id=entity.tenant_id,
        entity_id=entity.entity_id,
        decision=decision,
        signals=signals,
        policy_id=policy_id,
        policy_config=cfg,
        now=check_now,
    )
    voice_prop = proposals[-1]
    has_sms = any(p.channel == "sms" for p in proposals)
    # If caller specified explicit probe_scheduled_at, evaluate guardrail against that raw timestamp
    # so working_window differences between baseline and candidate surface directly!
    if probe_scheduled_at is not None:
        voice_prop = voice_prop.model_copy(update={"scheduled_at": to_utc(probe_scheduled_at)})
    return resolved_dt, has_sms, voice_prop


async def run_shadow_evaluation(
    req: ShadowEvalRequest,
    *,
    now: Optional[datetime] = None,
) -> ShadowEvalResponse:
    """Replay historical entities under baseline vs candidate policies with ZERO outbound writes."""
    check_now = to_utc(now) if now is not None else _utc_now()
    baseline_row = await FollowupPolicy.get_or_none(
        tenant_id=req.tenant_id, policy_id=req.baseline_policy_id
    )
    baseline_cfg = dict(
        req.baseline_policy_config
        if req.baseline_policy_config is not None
        else (baseline_row.config or {} if baseline_row else {})
    )
    candidate_policy_id = req.candidate_policy_id or req.baseline_policy_id
    candidate_cfg = {**baseline_cfg, **dict(req.candidate_policy_config or {})}

    if req.entity_ids:
        entities = (
            await FollowupEntity.filter(
                tenant_id=req.tenant_id, entity_id__in=req.entity_ids
            )
            .order_by("id")
            .all()
        )
    else:
        entities = (
            await FollowupEntity.filter(tenant_id=req.tenant_id)
            .order_by("-id")
            .limit(req.sample_limit)
            .all()
        )

    samples: List[ShadowSampleDiff] = []
    baseline_verdict_counts: Counter[str] = Counter()
    candidate_verdict_counts: Counter[str] = Counter()
    diverged_count = 0

    for ent in entities:
        state = await EntityMemoryState.get_or_none(
            tenant_id=req.tenant_id, entity_id=ent.entity_id
        )
        if state is None:
            state = EntityMemoryState(
                tenant_id=req.tenant_id,
                entity_id=ent.entity_id,
                hard_boundaries={},
                commitments=[],
                lifecycle={"stage": "new", "converted": False},
                ownership={},
                channel_stats={},
            )

        latest_ev = (
            await EntityEventLog.filter(
                tenant_id=req.tenant_id, entity_id=ent.entity_id
            )
            .order_by("-occurred_at", "-id")
            .first()
        )
        envelope = _envelope_from_log_or_probe(ent, latest_ev, check_now)

        base_rule = evaluate_rule_gateway(envelope, state, baseline_cfg)
        cand_rule = evaluate_rule_gateway(envelope, state, candidate_cfg)

        base_dt, base_sms, base_prop = _evaluate_policy_proposal_in_memory(
            entity=ent,
            envelope=envelope,
            cfg=baseline_cfg,
            policy_id=req.baseline_policy_id,
            probe_scheduled_at=req.probe_scheduled_at,
            probe_channel=req.probe_channel,
            probe_action_type=req.probe_action_type,
            check_now=check_now,
        )
        cand_dt, cand_sms, cand_prop = _evaluate_policy_proposal_in_memory(
            entity=ent,
            envelope=envelope,
            cfg=candidate_cfg,
            policy_id=candidate_policy_id,
            probe_scheduled_at=req.probe_scheduled_at,
            probe_channel=req.probe_channel,
            probe_action_type=req.probe_action_type,
            check_now=check_now,
        )

        # Pure read-only guardrail checks (never inserts ActionOutbox)
        base_verdict = await evaluate_outbox_guardrails(
            base_prop, ent, state, baseline_cfg, now=check_now
        )
        cand_verdict = await evaluate_outbox_guardrails(
            cand_prop, ent, state, candidate_cfg, now=check_now
        )

        baseline_verdict_counts[base_verdict.decision] += 1
        candidate_verdict_counts[cand_verdict.decision] += 1

        reasons: List[str] = []
        if base_rule.tier != cand_rule.tier or base_rule.matched_rule != cand_rule.matched_rule:
            reasons.append(f"rule_gateway:{base_rule.tier}->{cand_rule.tier}")
        if base_verdict.decision != cand_verdict.decision or base_verdict.rule_id != cand_verdict.rule_id:
            reasons.append(
                f"guardrail:{base_verdict.decision}({base_verdict.rule_id})->{cand_verdict.decision}({cand_verdict.rule_id})"
            )
        if base_sms != cand_sms:
            reasons.append(f"pre_call_sms:{base_sms}->{cand_sms}")
        if base_dt != cand_dt:
            reasons.append("scheduled_at_changed")

        diverged = len(reasons) > 0
        if diverged:
            diverged_count += 1

        samples.append(
            ShadowSampleDiff(
                entity_id=ent.entity_id,
                event_id=envelope.event_id,
                baseline_rule_tier=base_rule.tier,
                candidate_rule_tier=cand_rule.tier,
                baseline_matched_rule=base_rule.matched_rule,
                candidate_matched_rule=cand_rule.matched_rule,
                baseline_verdict=base_verdict.decision,
                baseline_verdict_rule=base_verdict.rule_id,
                baseline_scheduled_at=base_dt,
                baseline_pre_call_sms=base_sms,
                candidate_verdict=cand_verdict.decision,
                candidate_verdict_rule=cand_verdict.rule_id,
                candidate_scheduled_at=cand_dt,
                candidate_pre_call_sms=cand_sms,
                diverged=diverged,
                divergence_reasons=reasons,
            )
        )

    blocked_by_cand = candidate_verdict_counts.get("blocked", 0)
    blocked_by_base = baseline_verdict_counts.get("blocked", 0)
    if blocked_by_cand > blocked_by_base:
        recommendation = "needs_review_new_blocks"
    elif diverged_count > 0:
        recommendation = "promote_candidate_ready"
    else:
        recommendation = "no_behavioral_change"

    summary = {
        "sample_size": len(samples),
        "diverged_count": diverged_count,
        "baseline_verdicts": dict(baseline_verdict_counts),
        "candidate_verdicts": dict(candidate_verdict_counts),
        "recommendation": recommendation,
        "zero_outbound_enforced": True,
    }
    eval_id = f"shadow_{uuid.uuid4().hex[:16]}"
    run_row = await RsiShadowEvalRun.create(
        tenant_id=req.tenant_id,
        eval_id=eval_id,
        baseline_policy_id=req.baseline_policy_id,
        baseline_version=req.baseline_version,
        candidate_policy_id=candidate_policy_id,
        candidate_version=req.candidate_version,
        sample_size=len(samples),
        summary=summary,
        samples=[s.model_dump(mode="json") for s in samples],
    )

    return ShadowEvalResponse(
        eval_id=eval_id,
        tenant_id=req.tenant_id,
        baseline_policy_id=req.baseline_policy_id,
        baseline_version=req.baseline_version,
        candidate_policy_id=candidate_policy_id,
        candidate_version=req.candidate_version,
        sample_size=len(samples),
        diverged_count=diverged_count,
        zero_outbound_enforced=True,
        summary=summary,
        samples=samples,
        created_at=to_utc(run_row.create_time),
    )
