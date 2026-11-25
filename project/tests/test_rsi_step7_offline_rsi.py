from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest
import pytest_asyncio
from tortoise import Tortoise

from app.main import app
from app.rsi.feedback import (
    FeedbackRecordRequest,
    compute_human_edit_diff,
    record_feedback,
    sync_guardrail_blocks_to_feedback,
)
from app.rsi.memory_reducer import ingest_event
from app.rsi.models import (
    ActionExecutionResult,
    ActionOutbox,
    EntityMemoryState,
    FollowupEntity,
    FollowupPolicy,
    RsiFeedbackRecord,
    RsiShadowEvalRun,
)
from app.rsi.offline_optimizer import (
    ShadowEvalRequest,
    generate_offline_review_report,
    run_shadow_evaluation,
)
from app.rsi.schemas import RSIEventEnvelope

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def setup_rsi_step7_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["app.rsi.models"]},
    )
    await Tortoise.generate_schemas()
    yield
    await Tortoise.close_connections()


async def test_human_edit_diff_auto_computation_and_persistence():
    """compute_human_edit_diff captures semantic tags and record_feedback backfills Outbox metadata."""
    diff = compute_human_edit_diff(
        "现在保证马上放款，今天必须过来签合同！",
        "您好，我们是不押车方案，仅登记绿本，方便时随时沟通细节～",
    )
    assert diff.changed is True
    assert 0.0 < diff.similarity < 1.0
    assert diff.edit_ratio > 0.5
    assert "compliance_or_tone_softened" in diff.edit_tags
    assert "politeness_added" in diff.edit_tags
    assert "clarity_enhanced" in diff.edit_tags
    assert diff.removed_segments and diff.added_segments
    assert "ai_draft" in diff.unified_diff and "human_edited" in diff.unified_diff

    await FollowupEntity.create(
        tenant_id="t7", entity_id="ent_diff_1", policy_id="af_v1", status="active"
    )
    outbox = await ActionOutbox.create(
        tenant_id="t7",
        entity_id="ent_diff_1",
        idempotency_key="ob_diff_1",
        action_type="send_message",
        channel="wechat",
        adapter_name="crm_passive",
        permission_level="L0",
        status="succeeded",
        scheduled_at=datetime(2026, 11, 24, 6, 0, tzinfo=timezone.utc),
        policy_id="af_v1",
        policy_version="1.2",
        payload={"text": "现在保证马上放款，快办！", "script_version": "s_v2"},
    )

    rec = await record_feedback(
        FeedbackRecordRequest(
            tenant_id="t7",
            entity_id="ent_diff_1",
            feedback_type="human_edit",
            outbox_id=outbox.id,
            edited_text="您好，我们是不押车方案，方便时随时沟通细节～",
            reviewer_id="sales_rep_07",
        )
    )
    assert rec.policy_id == "af_v1"
    assert rec.policy_version == "1.2"
    assert rec.script_version == "s_v2"
    assert rec.channel == "wechat"
    assert rec.original_text == "现在保证马上放款，快办！"
    assert rec.human_edit_diff["changed"] is True
    assert "compliance_or_tone_softened" in rec.human_edit_diff["edit_tags"]


async def test_conversion_attribution_and_guardrail_block_sync():
    """Syncing guardrail_blocked Outbox rows into feedback is strictly idempotent."""
    await FollowupEntity.create(
        tenant_id="t_sync", entity_id="ent_blk_1", policy_id="pol_a", status="active"
    )
    now = datetime(2026, 11, 24, 6, 0, tzinfo=timezone.utc)
    blocked = await ActionOutbox.create(
        tenant_id="t_sync",
        entity_id="ent_blk_1",
        idempotency_key="ob_blk_1",
        action_type="send_message",
        channel="wechat",
        adapter_name="crm_passive",
        permission_level="L0",
        status="guardrail_blocked",
        scheduled_at=now,
        verdict_rule="outbound_forbidden_word:保证放款",
        verdict_reason="Blocked promise language",
        policy_id="pol_a",
        policy_version="1.0",
        payload={"text": "保证放款当天拿车", "script_version": "s_v1"},
        finished_at=now,
    )

    inserted1 = await sync_guardrail_blocks_to_feedback(tenant_id="t_sync")
    inserted2 = await sync_guardrail_blocks_to_feedback(tenant_id="t_sync")
    assert inserted1 == 1
    assert inserted2 == 0

    fb = await RsiFeedbackRecord.get(tenant_id="t_sync", outbox_id=blocked.id)
    assert fb.feedback_type == "guardrail_block"
    assert fb.outcome == "blocked"
    assert fb.guardrail_rule == "outbound_forbidden_word:保证放款"
    assert fb.original_text == "保证放款当天拿车"


async def test_offline_review_report_aggregation_clustering_and_suggestions():
    """Aggregate conversion/connect rates, human-edit clusters, and actionable suggestions."""
    now = datetime(2026, 11, 24, 6, 0, tzinfo=timezone.utc)
    await FollowupEntity.create(
        tenant_id="t_rpt", entity_id="ent_r1", policy_id="af_pol", status="active"
    )

    # Variant 1.0/s1: 1/3 converted, 1/4 call connected, 2 softening edits, 1 block
    for i, out in enumerate(["converted", "lost", "lost"]):
        await record_feedback(
            FeedbackRecordRequest(
                tenant_id="t_rpt",
                entity_id="ent_r1",
                feedback_type="conversion_outcome",
                policy_id="af_pol",
                policy_version="1.0",
                script_version="s1",
                outcome=out,
                occurred_at=now,
            )
        )
    for i, out in enumerate(["connected", "no_answer", "no_answer", "no_answer"]):
        await record_feedback(
            FeedbackRecordRequest(
                tenant_id="t_rpt",
                entity_id="ent_r1",
                feedback_type="call_connect",
                policy_id="af_pol",
                policy_version="1.0",
                script_version="s1",
                channel="voice",
                outcome=out,
                occurred_at=now,
            )
        )
    await record_feedback(
        FeedbackRecordRequest(
            tenant_id="t_rpt",
            entity_id="ent_r1",
            feedback_type="guardrail_block",
            policy_id="af_pol",
            policy_version="1.0",
            script_version="s1",
            guardrail_rule="outbound_forbidden_word:绝对低息",
            outcome="blocked",
            occurred_at=now,
        )
    )
    for orig, edited in [
        ("绝对低息保证放款，马上发资料！", "年化费率透明，方便时随时沟通方案～"),
        ("必须今天交资料，稳过审核！", "方便时发一下行驶证，帮您先做初评～"),
    ]:
        await record_feedback(
            FeedbackRecordRequest(
                tenant_id="t_rpt",
                entity_id="ent_r1",
                feedback_type="human_edit",
                policy_id="af_pol",
                policy_version="1.0",
                script_version="s1",
                original_text=orig,
                edited_text=edited,
                occurred_at=now,
            )
        )

    # Variant 2.0/s2: 3/4 converted, 3/4 call connected
    for out in ["converted", "converted", "converted", "lost"]:
        await record_feedback(
            FeedbackRecordRequest(
                tenant_id="t_rpt",
                entity_id="ent_r1",
                feedback_type="conversion_outcome",
                policy_id="af_pol",
                policy_version="2.0",
                script_version="s2",
                outcome=out,
                occurred_at=now,
            )
        )
    for out in ["connected", "connected", "connected", "no_answer"]:
        await record_feedback(
            FeedbackRecordRequest(
                tenant_id="t_rpt",
                entity_id="ent_r1",
                feedback_type="call_connect",
                policy_id="af_pol",
                policy_version="2.0",
                script_version="s2",
                channel="voice",
                outcome=out,
                occurred_at=now,
            )
        )

    report = await generate_offline_review_report(
        tenant_id="t_rpt", policy_id="af_pol", window_days=30, now=now
    )
    assert len(report.by_version) == 2
    m_v1, m_v2 = report.by_version[0], report.by_version[1]
    assert m_v1.policy_version == "1.0"
    assert m_v1.conversion_rate == pytest.approx(0.3333, abs=0.001)
    assert m_v1.call_connect_rate == pytest.approx(0.25, abs=0.001)
    assert m_v1.human_edit_count == 2
    assert m_v1.guardrail_block_count == 1

    assert m_v2.policy_version == "2.0"
    assert m_v2.conversion_rate == pytest.approx(0.75, abs=0.001)
    assert m_v2.call_connect_rate == pytest.approx(0.75, abs=0.001)

    cluster_tags = {c.tag for c in report.edit_clusters}
    assert "compliance_or_tone_softened" in cluster_tags
    assert "politeness_added" in cluster_tags

    sug_cats = {s.category for s in report.suggestions}
    assert {
        "compliance_guardrail",
        "script_tone",
        "cadence_timing",
        "conversion_winner",
    }.issubset(sug_cats)


async def test_shadow_eval_zero_outbound_and_policy_divergence_detection():
    """Shadow evaluation replays candidate policy safely with 0 ActionOutbox side effects."""
    tenant = "t_shadow"
    now = datetime(2026, 11, 24, 6, 0, tzinfo=timezone.utc)  # 14:00 CST

    # Entity 1: wechat.friend_added event triggers rule_tier divergence
    await ingest_event(
        RSIEventEnvelope(
            tenant_id=tenant,
            event_id="sh_ev_1",
            entity_id="ent_sh_1",
            event_type="wechat.friend_added",
            category="wechat",
            actor_type="system",
            occurred_at=now,
            facts={"wechat_added": True},
        )
    )
    # Entity 2: voice no_answer event tests window & pre_call_sms divergence at 08:30 CST
    await ingest_event(
        RSIEventEnvelope(
            tenant_id=tenant,
            event_id="sh_ev_2",
            entity_id="ent_sh_2",
            event_type="voice.call_ended",
            category="voice",
            occurred_at=now,
            facts={"sip_code": 408, "hangup_cause": "no_answer", "city": "杭州"},
        )
    )

    outbox_before = await ActionOutbox.all().count()
    result_before = await ActionExecutionResult.all().count()

    # Probe 08:30 CST (00:30 UTC): blocked/deferred under 09:00 baseline, allowed under 08:00 candidate
    probe_dt = datetime(2026, 11, 25, 0, 30, tzinfo=timezone.utc)
    eval_res = await run_shadow_evaluation(
        ShadowEvalRequest(
            tenant_id=tenant,
            baseline_policy_id="af_base",
            baseline_version="1.0",
            baseline_policy_config={
                "working_window": {"work_start_time": "09:00:00", "work_end_time": "21:00:00", "timezone": "Asia/Shanghai"},
                "wakeup_on_wechat_added": False,
                "enable_pre_call_sms": False,
            },
            candidate_version="2.0",
            candidate_policy_config={
                "working_window": {"work_start_time": "08:00:00", "work_end_time": "21:00:00", "timezone": "Asia/Shanghai"},
                "wakeup_on_wechat_added": True,
                "enable_pre_call_sms": True,
            },
            entity_ids=["ent_sh_1", "ent_sh_2"],
            probe_scheduled_at=probe_dt,
        ),
        now=now,
    )

    assert eval_res.zero_outbound_enforced is True
    assert eval_res.sample_size == 2
    assert eval_res.diverged_count == 2
    by_ent = {s.entity_id: s for s in eval_res.samples}

    s1 = by_ent["ent_sh_1"]
    assert s1.baseline_rule_tier == "L1_REFLEX_CANCEL"
    assert s1.candidate_rule_tier == "L2_COGNITIVE_WAKEUP"
    assert s1.diverged is True

    s2 = by_ent["ent_sh_2"]
    assert s2.baseline_verdict == "deferred"
    assert s2.candidate_verdict == "allowed"
    assert s2.baseline_pre_call_sms is False
    assert s2.candidate_pre_call_sms is True

    # Zero Outbox side effects invariant!
    assert await ActionOutbox.all().count() == outbox_before
    assert await ActionExecutionResult.all().count() == result_before
    # Run persisted for audit
    saved = await RsiShadowEvalRun.get(eval_id=eval_res.eval_id)
    assert saved.sample_size == 2


async def test_shadow_eval_respects_hard_boundaries_and_forbidden_words():
    """Shadow evaluation catches blacklisted entities and forbidden candidate copy with zero outbound."""
    await FollowupEntity.create(
        tenant_id="t_safe", entity_id="ent_bl", policy_id="default", status="active"
    )
    await EntityMemoryState.create(
        tenant_id="t_safe",
        entity_id="ent_bl",
        hard_boundaries={"blacklisted": True, "blacklist_reason": "complaint"},
    )
    await FollowupEntity.create(
        tenant_id="t_safe", entity_id="ent_clean", policy_id="default", status="active"
    )

    now = datetime(2026, 11, 24, 6, 0, tzinfo=timezone.utc)
    res = await run_shadow_evaluation(
        ShadowEvalRequest(
            tenant_id="t_safe",
            baseline_policy_id="default",
            baseline_policy_config={"default_opening_script": "您好，为您介绍车贷方案"},
            candidate_policy_config={"candidate_draft_text": "我们这边百分百通过审核！"},
            entity_ids=["ent_bl", "ent_clean"],
        ),
        now=now,
    )

    by_ent = {s.entity_id: s for s in res.samples}
    assert by_ent["ent_bl"].candidate_verdict == "blocked"
    assert by_ent["ent_bl"].candidate_verdict_rule == "boundary:blacklisted_or_closed"
    assert by_ent["ent_clean"].baseline_verdict == "allowed"
    assert by_ent["ent_clean"].candidate_verdict == "blocked"
    assert "outbound_forbidden_word:百分百通过" in by_ent["ent_clean"].candidate_verdict_rule
    assert res.summary["recommendation"] == "needs_review_new_blocks"
    assert await ActionOutbox.filter(tenant_id="t_safe").count() == 0


async def test_rsi_feedback_http_api_endpoints():
    """Verify POST /v1/rsi/feedback, GET /v1/rsi/reports, and POST /v1/rsi/shadow-eval."""
    await FollowupEntity.create(
        tenant_id="t_api7", entity_id="ent_api_1", policy_id="pol_api", status="active"
    )
    await FollowupPolicy.create(
        tenant_id="t_api7",
        policy_id="pol_api",
        version="1.0",
        config={"enable_pre_call_sms": False},
    )
    await ActionOutbox.create(
        tenant_id="t_api7",
        entity_id="ent_api_1",
        idempotency_key="ob_api_blk",
        action_type="send_message",
        channel="wechat",
        adapter_name="crm_passive",
        permission_level="L0",
        status="guardrail_blocked",
        scheduled_at=datetime(2026, 11, 24, 6, 0, tzinfo=timezone.utc),
        verdict_rule="outbound_forbidden_word:稳过",
        policy_id="pol_api",
        policy_version="1.0",
        payload={"text": "稳过审批"},
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        # 1) POST /v1/rsi/feedback (human_edit)
        r_post = await client.post(
            "/v1/rsi/feedback",
            json={
                "tenant_id": "t_api7",
                "entity_id": "ent_api_1",
                "feedback_type": "human_edit",
                "policy_id": "pol_api",
                "policy_version": "1.0",
                "script_version": "s1",
                "original_text": "绝对低息稳过！",
                "edited_text": "您好，具体年化利率透明公开，方便发我行驶证看看～",
            },
        )
        assert r_post.status_code == 200
        body_post = r_post.json()
        assert body_post["human_edit_diff"]["changed"] is True

        # 2) GET /v1/rsi/reports?sync_blocks=true
        r_rep = await client.get(
            "/v1/rsi/reports",
            params={"tenant_id": "t_api7", "policy_id": "pol_api", "sync_blocks": "true"},
        )
        assert r_rep.status_code == 200
        rep_json = r_rep.json()
        assert rep_json["total_records"] == 2
        assert sum(v["guardrail_block_count"] for v in rep_json["by_version"]) == 1
        assert sum(v["human_edit_count"] for v in rep_json["by_version"]) == 1

        # 3) POST /v1/rsi/shadow-eval
        r_sh = await client.post(
            "/v1/rsi/shadow-eval",
            json={
                "tenant_id": "t_api7",
                "baseline_policy_id": "pol_api",
                "candidate_version": "2.0",
                "candidate_policy_config": {"enable_pre_call_sms": True},
                "entity_ids": ["ent_api_1"],
            },
        )
        assert r_sh.status_code == 200
        sh_json = r_sh.json()
        assert sh_json["zero_outbound_enforced"] is True
        assert sh_json["sample_size"] == 1
        assert sh_json["samples"][0]["candidate_pre_call_sms"] is True
