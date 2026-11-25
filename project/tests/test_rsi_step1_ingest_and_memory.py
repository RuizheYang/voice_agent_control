from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from tortoise import Tortoise

from app.main import app
from app.rsi.memory_reducer import get_entity_state, ingest_event, replay_entity_state
from app.rsi.models import EntityEventLog, EntityMemoryState, FollowupEntity
from app.rsi.schemas import EntityKeyRef, RSIEventEnvelope

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def setup_rsi_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["app.rsi.models"]},
    )
    await Tortoise.generate_schemas()
    yield
    await Tortoise.close_connections()


def _ev(
    event_id: str,
    event_type: str,
    category: str,
    occurred_at: datetime,
    *,
    tenant_id: str = "default",
    entity_id: str | None = None,
    entity_keys: list[dict] | None = None,
    actor_type: str = "system",
    actor_id: str | None = None,
    summary: str | None = None,
    facts: dict | None = None,
    extra: dict | None = None,
    policy_id: str | None = None,
) -> RSIEventEnvelope:
    return RSIEventEnvelope(
        event_id=event_id,
        tenant_id=tenant_id,
        event_type=event_type,
        category=category,  # type: ignore[arg-type]
        occurred_at=occurred_at,
        entity_id=entity_id,
        entity_keys=[EntityKeyRef(**k) for k in (entity_keys or [])],
        actor_type=actor_type,  # type: ignore[arg-type]
        actor_id=actor_id,
        summary=summary,
        facts=facts or {},
        extra=extra or {},
        policy_id=policy_id,
    )


async def test_all_16_categories_ingest_and_l2_projection():
    """All 16 fact categories persist in L1 with extra payloads and project into typed L2 slots."""
    base = datetime(2026, 11, 20, 8, 0, 0, tzinfo=timezone.utc)
    eid = "cust_16_cat"
    events = [
        _ev("e1", "voice.call_ended", "voice", base + timedelta(minutes=1), entity_id=eid, actor_type="system", facts={"answered": True, "outcome": "answered", "hangup_code": "200"}, extra={"call_trace_id": "tr_1"}),
        _ev("e2", "wechat.friend_added", "wechat", base + timedelta(minutes=2), entity_id=eid, actor_type="customer", facts={"wechat_added": True}, extra={"external_userid": "wm_1"}),
        _ev("e3", "sms.delivered", "messaging", base + timedelta(minutes=3), entity_id=eid, actor_type="system", facts={"channel": "sms"}, extra={"tpl": "pre_call"}),
        _ev("e4", "app.page_view", "content_engagement", base + timedelta(minutes=4), entity_id=eid, actor_type="customer", facts={"page": "loan_calculator", "score": 85}, extra={"duration_sec": 42}),
        _ev("e5", "moments.liked", "private_domain", base + timedelta(minutes=5), entity_id=eid, actor_type="customer", summary="点赞车贷海报", facts={"post_id": "p_1"}),
        _ev("e6", "churn.idle_7d", "churn_signal", base + timedelta(minutes=6), entity_id=eid, actor_type="system", facts={"risk_level": "medium"}),
        _ev("e7", "license.precheck_passed", "fulfillment", base + timedelta(minutes=7), entity_id=eid, actor_type="agent", facts={"status": "precheck_passed", "vin_ok": True}),
        _ev("e8", "order.paid", "finance_order", base + timedelta(minutes=8), entity_id=eid, actor_type="crm", facts={"order_id": "ord_100", "status": "paid", "amount": 5000, "currency": "CNY"}),
        _ev("e9", "risk.flag_added", "risk_boundary", base + timedelta(minutes=9), entity_id=eid, actor_type="crm", facts={"dnd": False, "risk_flag": "sensitive_rate"}),
        _ev("e10", "human.rep_called", "human_touch", base + timedelta(minutes=10), entity_id=eid, actor_type="human_rep", actor_id="rep_07", facts={"takeover_minutes": 15}),
        _ev("e11", "approval.requested", "approval", base + timedelta(minutes=11), entity_id=eid, actor_type="agent", facts={"approval_id": "apr_1", "status": "pending", "tool_name": "tool_submit_loan"}),
        _ev("e12", "role.handoff", "role_handoff", base + timedelta(minutes=12), entity_id=eid, actor_type="crm", facts={"from_role": "ai_precall", "to_role": "finance_specialist", "to_owner_id": "spec_88"}),
        _ev("e13", "commitment.created", "commitment", base + timedelta(minutes=13), entity_id=eid, actor_type="customer", summary="周六下午带驾驶证到店", facts={"commitment_id": "cm_1", "due_at": "2026-11-28T06:00:00+00:00", "status": "open"}),
        _ev("e14", "lifecycle.stage_changed", "lifecycle", base + timedelta(minutes=14), entity_id=eid, actor_type="crm", facts={"stage": "underwriting"}),
        _ev("e15", "campaign.interest_rate_drop", "business_broadcast", base + timedelta(minutes=15), entity_id=eid, actor_type="crm", summary="11月低息活动", facts={"campaign_id": "cmp_nov"}),
        _ev("e16", "adapter.receipt", "action_receipt", base + timedelta(minutes=16), entity_id=eid, actor_type="system", facts={"action_id": "act_9", "ok": True}),
    ]

    for ev in events:
        res = await ingest_event(ev)
        assert res.duplicate is False

    l1_rows = await EntityEventLog.filter(tenant_id="default", entity_id=eid).order_by("id").all()
    assert len(l1_rows) == 16
    assert l1_rows[0].extra == {"call_trace_id": "tr_1"}
    assert l1_rows[1].extra == {"external_userid": "wm_1"}

    state = await get_entity_state("default", eid)
    assert state is not None
    assert state.projection_version == 16
    assert state.status == "converted"
    assert state.channel_stats["wechat_added"] is True
    assert state.channel_stats["by_channel"]["voice"]["answered_count"] == 1
    assert state.channel_stats["by_channel"]["sms"]["count"] == 1
    assert len(state.channel_stats["content_signals"]) == 1
    assert len(state.channel_stats["private_domain_events"]) == 1
    assert len(state.channel_stats["broadcasts"]) == 1
    assert state.channel_stats["last_receipt"]["event_id"] == "e16"
    assert state.lifecycle["churn_risk"] == "medium"
    assert state.lifecycle["fulfillment_status"] == "precheck_passed"
    assert state.lifecycle["orders"]["ord_100"]["status"] == "paid"
    assert state.lifecycle["converted"] is True
    assert state.lifecycle["stage"] == "underwriting"
    assert "sensitive_rate" in state.hard_boundaries["risk_flags"]
    assert state.hard_boundaries["human_takeover_until"] is not None
    assert state.ownership["current_role"] == "finance_specialist"
    assert state.ownership["owner_id"] == "spec_88"
    assert state.ownership["pending_approvals"]["apr_1"]["status"] == "pending"
    assert len(state.commitments) == 1
    assert state.commitments[0]["commitment_id"] == "cm_1"


async def test_strict_idempotency_and_tenant_isolation():
    """Same (tenant_id, event_id) is idempotent; same event_id in another tenant is isolated."""
    now = datetime(2026, 11, 20, 9, 0, 0, tzinfo=timezone.utc)
    ev1 = _ev(
        "dup_ev_1",
        "voice.call_ended",
        "voice",
        now,
        tenant_id="tenant_a",
        entity_id="ent_shared_name",
        facts={"answered": False, "outcome": "no_answer"},
    )
    r1 = await ingest_event(ev1)
    assert r1.duplicate is False
    assert r1.created_entity is True

    r2 = await ingest_event(ev1)
    assert r2.duplicate is True
    assert r2.created_entity is False
    assert r2.state_version == r1.state_version

    state_a = await get_entity_state("tenant_a", "ent_shared_name")
    assert state_a is not None
    assert state_a.channel_stats["by_channel"]["voice"]["count"] == 1
    assert state_a.channel_stats["by_channel"]["voice"]["no_answer_streak"] == 1

    # Different tenant with exact same event_id and entity_id must be isolated
    ev_b = _ev(
        "dup_ev_1",
        "voice.call_ended",
        "voice",
        now,
        tenant_id="tenant_b",
        entity_id="ent_shared_name",
        facts={"answered": True, "outcome": "answered"},
    )
    rb = await ingest_event(ev_b)
    assert rb.duplicate is False
    assert rb.created_entity is True

    state_b = await get_entity_state("tenant_b", "ent_shared_name")
    assert state_b is not None
    assert state_b.channel_stats["by_channel"]["voice"]["answered_count"] == 1
    assert await get_entity_state("tenant_b", "nonexistent") is None


async def test_cross_channel_identity_resolution():
    """Chained cross-channel aliases (phone -> wechat -> crm_id) resolve to one canonical entity."""
    t0 = datetime(2026, 11, 20, 10, 0, 0, tzinfo=timezone.utc)

    r1 = await ingest_event(
        _ev(
            "id_1",
            "voice.call_ended",
            "voice",
            t0,
            entity_keys=[{"key_type": "phone_hash", "key_value": "ph_999"}],
            facts={"answered": False},
        )
    )
    r2 = await ingest_event(
        _ev(
            "id_2",
            "wechat.friend_added",
            "wechat",
            t0 + timedelta(minutes=5),
            entity_keys=[
                {"key_type": "phone_hash", "key_value": "ph_999"},
                {"key_type": "wechat_external_id", "key_value": "wx_888"},
            ],
            facts={"wechat_added": True},
        )
    )
    r3 = await ingest_event(
        _ev(
            "id_3",
            "order.created",
            "finance_order",
            t0 + timedelta(minutes=10),
            entity_keys=[
                {"key_type": "wechat_external_id", "key_value": "wx_888"},
                {"key_type": "crm_customer_id", "key_value": "crm_777"},
            ],
            facts={"order_id": "ord_777", "status": "pending", "amount": 120000},
        )
    )

    assert r1.entity_id == r2.entity_id == r3.entity_id
    state = await get_entity_state("default", r1.entity_id)
    assert state is not None
    alias_pairs = {(a.key_type, a.key_value) for a in state.aliases}
    assert alias_pairs == {
        ("phone_hash", "ph_999"),
        ("wechat_external_id", "wx_888"),
        ("crm_customer_id", "crm_777"),
    }


async def test_entity_merge_and_redirect():
    """entity.merged migrates aliases & L1 history to target entity and redirects future events."""
    t0 = datetime(2026, 11, 20, 11, 0, 0, tzinfo=timezone.utc)

    await ingest_event(
        _ev(
            "m_1",
            "voice.call_ended",
            "voice",
            t0,
            entity_id="ent_a",
            entity_keys=[{"key_type": "phone_hash", "key_value": "ph_merge_1"}],
            facts={"answered": True},
        )
    )
    await ingest_event(
        _ev(
            "m_2",
            "order.paid",
            "finance_order",
            t0 + timedelta(minutes=5),
            entity_id="ent_b",
            entity_keys=[{"key_type": "crm_customer_id", "key_value": "crm_merge_b"}],
            facts={"order_id": "ord_m", "status": "paid", "amount": 9000},
        )
    )

    merge_res = await ingest_event(
        _ev(
            "m_merge",
            "entity.merged",
            "lifecycle",
            t0 + timedelta(minutes=6),
            entity_id="ent_a",
            facts={"target_entity_id": "ent_b", "stage": "merged"},
        )
    )
    assert merge_res.entity_id == "ent_b"

    row_a = await FollowupEntity.get(tenant_id="default", entity_id="ent_a")
    assert row_a.status == "merged"
    assert row_a.merged_into_entity_id == "ent_b"

    state_b = await get_entity_state("default", "ent_b")
    assert state_b is not None
    assert state_b.channel_stats["by_channel"]["voice"]["answered_count"] == 1
    assert state_b.lifecycle["orders"]["ord_m"]["status"] == "paid"
    assert state_b.lifecycle["converted"] is True
    assert {(a.key_type, a.key_value) for a in state_b.aliases} == {
        ("phone_hash", "ph_merge_1"),
        ("crm_customer_id", "crm_merge_b"),
    }

    # Subsequent event targeting old entity_id transparently redirects to ent_b
    follow_res = await ingest_event(
        _ev(
            "m_4",
            "wechat.msg_received",
            "wechat",
            t0 + timedelta(minutes=10),
            entity_id="ent_a",
            actor_type="customer",
            facts={"text": "材料已发"},
        )
    )
    assert follow_res.entity_id == "ent_b"
    assert follow_res.inbound_msg_seq == 1


async def test_out_of_order_occurred_at_lww_protection():
    """Late stale events never overwrite newer L2 order, lifecycle, or human takeover slots."""
    eid = "ent_ooo"
    t11 = datetime(2026, 11, 20, 11, 0, 0, tzinfo=timezone.utc)
    t12 = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)
    t12_30 = datetime(2026, 11, 20, 12, 30, 0, tzinfo=timezone.utc)

    # 1. Newer order.paid before late order.created
    await ingest_event(_ev("ooo_1", "order.paid", "finance_order", t12, entity_id=eid, facts={"order_id": "o1", "status": "paid"}))
    await ingest_event(_ev("ooo_2", "order.created", "finance_order", t11, entity_id=eid, facts={"order_id": "o1", "status": "created"}))

    # 2. Newer lifecycle.stage="qualified" before stale "new"
    await ingest_event(_ev("ooo_3", "lifecycle.stage", "lifecycle", t12, entity_id=eid, facts={"stage": "qualified"}))
    await ingest_event(_ev("ooo_4", "lifecycle.stage", "lifecycle", t11, entity_id=eid, facts={"stage": "new"}))

    # 3. Newer human.takeover_ended (12:30) before stale human.takeover_started (12:00)
    await ingest_event(_ev("ooo_5", "human.takeover_ended", "human_touch", t12_30, entity_id=eid, actor_type="human_rep"))
    await ingest_event(_ev("ooo_6", "human.takeover_started", "human_touch", t12, entity_id=eid, actor_type="human_rep", facts={"takeover_minutes": 60}))

    state = await get_entity_state("default", eid)
    assert state is not None
    assert state.lifecycle["orders"]["o1"]["status"] == "paid"
    assert state.lifecycle["converted"] is True
    assert state.lifecycle["stage"] == "qualified"
    assert state.hard_boundaries["human_takeover_until"] is None


async def test_inbound_msg_seq_increments_only_for_customer_messaging():
    """Only customer wechat/messaging events advance inbound_msg_seq for L3 preemption."""
    eid = "ent_seq"
    t0 = datetime(2026, 11, 20, 13, 0, 0, tzinfo=timezone.utc)

    r1 = await ingest_event(_ev("s1", "voice.call_ended", "voice", t0, entity_id=eid, actor_type="system", facts={"answered": True}))
    r2 = await ingest_event(_ev("s2", "wechat.msg_sent", "wechat", t0 + timedelta(seconds=1), entity_id=eid, actor_type="agent"))
    assert r1.inbound_msg_seq == 0
    assert r2.inbound_msg_seq == 0

    r3 = await ingest_event(_ev("s3", "wechat.msg_received", "wechat", t0 + timedelta(seconds=2), entity_id=eid, actor_type="customer", facts={"text": "在吗"}))
    r4 = await ingest_event(_ev("s4", "sms.replied", "messaging", t0 + timedelta(seconds=3), entity_id=eid, actor_type="customer", facts={"channel": "sms", "text": "明天打"}))
    assert r3.inbound_msg_seq == 1
    assert r4.inbound_msg_seq == 2


async def test_deterministic_full_replay():
    """Corrupting L2 state and running replay rebuilds exact L2 projection from L1 ledger."""
    eid = "ent_replay"
    t0 = datetime(2026, 11, 20, 14, 0, 0, tzinfo=timezone.utc)
    await ingest_event(_ev("rp_1", "voice.call_ended", "voice", t0, entity_id=eid, facts={"answered": False}))
    await ingest_event(_ev("rp_2", "risk.blacklisted", "risk_boundary", t0 + timedelta(minutes=1), entity_id=eid, facts={"blacklisted": True, "reason": "complaint"}))
    await ingest_event(_ev("rp_3", "commitment.created", "commitment", t0 + timedelta(minutes=2), entity_id=eid, facts={"commitment_id": "c_rp", "description": "周日回访"}))

    before = await get_entity_state("default", eid)
    assert before is not None

    # Deliberately wipe L2 state in DB
    await EntityMemoryState.filter(tenant_id="default", entity_id=eid).update(
        hard_boundaries={},
        commitments=[],
        lifecycle={},
        ownership={},
        channel_stats={},
        slot_watermarks={},
    )
    corrupted = await get_entity_state("default", eid)
    assert corrupted is not None
    assert corrupted.hard_boundaries.get("blacklisted") is False  # wiped back to default view

    replay_res = await replay_entity_state("default", eid)
    assert replay_res is not None
    assert replay_res.replayed_events == 3
    assert replay_res.projection_version == before.projection_version + 1
    assert replay_res.state.status == "blacklisted"
    assert replay_res.state.hard_boundaries == before.hard_boundaries
    assert replay_res.state.commitments == before.commitments
    assert replay_res.state.channel_stats == before.channel_stats


async def test_http_api_single_batch_state_timeline_and_validation():
    """HTTP router exposes single ingest, batch ingest, state, timeline, replay, and 422 validation."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Validation error when neither entity_id nor entity_keys is present
        bad_resp = await client.post(
            "/v1/events",
            json={
                "event_id": "bad_1",
                "event_type": "voice.call_ended",
                "category": "voice",
                "occurred_at": "2026-11-20T15:00:00Z",
            },
        )
        assert bad_resp.status_code == 422

        # Single event ingest
        single_resp = await client.post(
            "/v1/events",
            json={
                "event_id": "http_1",
                "tenant_id": "tenant_http",
                "event_type": "voice.call_ended",
                "category": "voice",
                "entity_keys": [{"key_type": "phone_hash", "key_value": "ph_http_1"}],
                "occurred_at": "2026-11-20T15:00:00Z",
                "facts": {"answered": True},
                "extra": {"sip_trunk": "hz_01"},
            },
        )
        assert single_resp.status_code == 200
        entity_id = single_resp.json()["entity_id"]
        assert single_resp.json()["duplicate"] is False

        # Batch ingest (1 new + 1 duplicate)
        batch_resp = await client.post(
            "/v1/events/batch",
            json={
                "events": [
                    {
                        "event_id": "http_1",
                        "tenant_id": "tenant_http",
                        "event_type": "voice.call_ended",
                        "category": "voice",
                        "entity_id": entity_id,
                        "occurred_at": "2026-11-20T15:00:00Z",
                    },
                    {
                        "event_id": "http_2",
                        "tenant_id": "tenant_http",
                        "event_type": "wechat.msg_received",
                        "category": "wechat",
                        "entity_id": entity_id,
                        "actor_type": "customer",
                        "occurred_at": "2026-11-20T15:05:00Z",
                        "facts": {"text": "你好"},
                    },
                ]
            },
        )
        assert batch_resp.status_code == 200
        batch_json = batch_resp.json()
        assert batch_json["accepted"] == 1
        assert batch_json["duplicates"] == 1
        assert batch_json["failed"] == 0

        # Query state & timeline & replay
        st_resp = await client.get(f"/v1/entities/{entity_id}/state", params={"tenant_id": "tenant_http"})
        assert st_resp.status_code == 200
        assert st_resp.json()["inbound_msg_seq"] == 1

        tl_resp = await client.get(f"/v1/entities/{entity_id}/timeline", params={"tenant_id": "tenant_http"})
        assert tl_resp.status_code == 200
        assert tl_resp.json()["total"] == 2
        assert tl_resp.json()["items"][0]["event_id"] == "http_2"

        rp_resp = await client.post(f"/v1/entities/{entity_id}/replay", params={"tenant_id": "tenant_http"})
        assert rp_resp.status_code == 200
        assert rp_resp.json()["replayed_events"] == 2

        missing_resp = await client.get("/v1/entities/does_not_exist/state")
        assert missing_resp.status_code == 404


async def test_step1_dead_code_removed():
    """Confirmed dead legacy artifacts deleted in Step 1 stay removed."""
    app_root = Path(__file__).resolve().parents[1] / "app"
    assert not (app_root / "apis" / "voice_agent.py").exists()
    assert not (app_root / ".DS_Store").exists()


async def test_alias_conflict_warning_and_policy_binding():
    """Explicit entity_id warns cleanly on alias collision and binds policy_id without hijacking."""
    t0 = datetime(2026, 11, 20, 16, 0, 0, tzinfo=timezone.utc)
    r1 = await ingest_event(
        _ev(
            "ac_1",
            "voice.call_ended",
            "voice",
            t0,
            entity_id="ent_owner_1",
            entity_keys=[{"key_type": "phone_hash", "key_value": "ph_owner_1"}],
            policy_id="auto_finance_v1",
        )
    )
    assert r1.warnings == []

    r2 = await ingest_event(
        _ev(
            "ac_2",
            "wechat.msg_received",
            "wechat",
            t0 + timedelta(minutes=1),
            entity_id="ent_owner_2",
            entity_keys=[{"key_type": "phone_hash", "key_value": "ph_owner_1"}],
            actor_type="customer",
        )
    )
    assert any("alias_conflict:phone_hash:ph_owner_1->existing:ent_owner_1" in w for w in r2.warnings)

    st1 = await get_entity_state("default", "ent_owner_1")
    st2 = await get_entity_state("default", "ent_owner_2")
    assert st1 is not None and st1.policy_id == "auto_finance_v1"
    assert st2 is not None and st2.aliases == []
