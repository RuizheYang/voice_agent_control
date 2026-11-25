from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio
from tortoise import Tortoise

from app.main import app
from app.rsi.adapters import AdapterRegistry, CrmPassiveAdapter, VoiceChannelAdapter, WebhookChannelAdapter
from app.rsi.adapters.base import query_action_results
from app.rsi.adapters.voice import VoicePriorityQueue
from app.rsi.guardrails import (
    dispatch_due_outbox_actions,
    list_pending_outbox_approvals,
    resolve_outbox_approval,
    set_kill_switch,
    submit_action_proposal,
)
from app.rsi.memory_reducer import get_entity_state, ingest_event
from app.rsi.models import ActionExecutionResult, ActionOutbox, FollowupEntity, FollowupPolicy, RsiTenantAuth
from app.rsi.schemas import ActionProposal, RSIEventEnvelope

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def setup_rsi_step3_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["app.rsi.models"]},
    )
    await Tortoise.generate_schemas()
    yield
    await Tortoise.close_connections()


async def _create_entity(entity_id: str, *, tenant_id: str = "default", policy_id: str = "default") -> FollowupEntity:
    return await FollowupEntity.create(
        tenant_id=tenant_id,
        entity_id=entity_id,
        policy_id=policy_id,
        status="active",
    )


async def test_outbox_guardrail_working_window_deferral_and_timezone_snap():
    """Proposals outside working hours defer to next valid opening + buffer, then dispatch when due."""
    await FollowupPolicy.create(
        tenant_id="default",
        policy_id="pol_win",
        config={
            "work_start_time": "09:00:00",
            "work_end_time": "21:00:00",
            "timezone": "Asia/Shanghai",
            "morning_buffer_minutes": 30,
        },
        is_active=True,
    )
    await _create_entity("ent_win", policy_id="pol_win")

    # 2026-11-23 14:30 UTC == 22:30 Asia/Shanghai (evening closed) -> snaps to 2026-11-24 09:30+08:00 == 01:30 UTC
    night_utc = datetime(2026, 11, 23, 14, 30, 0, tzinfo=timezone.utc)
    res_night = await submit_action_proposal(
        ActionProposal(
            entity_id="ent_win",
            action_type="schedule_call",
            channel="voice",
            payload={"phone_number": "13800138001", "opening_script": "您好，回访一下"},
        ),
        now=night_utc,
    )
    assert res_night.status == "deferred"
    assert res_night.verdict == "deferred"
    assert res_night.verdict_rule == "working_window:outside_hours"
    expected_utc = datetime(2026, 11, 24, 1, 30, 0, tzinfo=timezone.utc)
    assert res_night.scheduled_at.replace(tzinfo=timezone.utc) == expected_utc

    # Early morning 07:15 Asia/Shanghai == 2026-11-23 23:15 UTC -> snaps to same local day 09:30+08:00 (2026-11-24 01:30 UTC)
    early_utc = datetime(2026, 11, 23, 23, 15, 0, tzinfo=timezone.utc)
    res_early = await submit_action_proposal(
        ActionProposal(
            entity_id="ent_win",
            action_type="send_sms",
            channel="sms",
            payload={"sms_text": "稍后致电给您"},
        ),
        now=early_utc,
    )
    assert res_early.status == "deferred"
    assert res_early.scheduled_at.replace(tzinfo=timezone.utc) == expected_utc

    # Dispatch becomes allowed once clock enters working window (2026-11-24 01:35 UTC == 09:35 CST)
    dispatched = await dispatch_due_outbox_actions(now=datetime(2026, 11, 24, 1, 35, 0, tzinfo=timezone.utc))
    assert dispatched == 2
    assert (await ActionOutbox.get(id=res_night.outbox_id)).status == "succeeded"
    assert (await ActionOutbox.get(id=res_early.outbox_id)).status == "succeeded"


async def test_outbox_guardrail_cross_channel_24h_7d_and_per_channel_freq_caps():
    """Enforce cooldown, per-channel 24h cap, cross-channel 24h cap, and 7d hard cap."""
    await FollowupPolicy.create(
        tenant_id="default",
        policy_id="pol_freq",
        config={
            "window_enforced_channels": [],
            "max_touches_24h": 3,
            "max_touches_7d": 4,
            "channel_caps_24h": {"voice": 2},
            "min_cooldown_minutes": 10,
        },
        is_active=True,
    )
    await _create_entity("ent_freq", policy_id="pol_freq")

    t0 = datetime(2026, 11, 20, 2, 0, 0, tzinfo=timezone.utc)  # 10:00 CST
    r1 = await submit_action_proposal(
        ActionProposal(entity_id="ent_freq", policy_id="pol_freq", action_type="call", channel="voice"),
        now=t0,
    )
    assert r1.status == "succeeded"

    # Cooldown block (< 10 min)
    r_cool = await submit_action_proposal(
        ActionProposal(entity_id="ent_freq", policy_id="pol_freq", action_type="call", channel="voice"),
        now=t0 + timedelta(minutes=2),
    )
    assert r_cool.verdict == "deferred"
    assert r_cool.verdict_rule == "freq_cap:cooldown"
    assert r_cool.scheduled_at.replace(tzinfo=timezone.utc) == t0 + timedelta(minutes=10)

    # Voice #2 succeeds after cooldown
    r2 = await submit_action_proposal(
        ActionProposal(entity_id="ent_freq", policy_id="pol_freq", action_type="call", channel="voice"),
        now=t0 + timedelta(minutes=15),
    )
    assert r2.status == "succeeded"

    # Voice #3 hits per-channel 24h cap (voice: 2)
    r_ch_cap = await submit_action_proposal(
        ActionProposal(entity_id="ent_freq", policy_id="pol_freq", action_type="call", channel="voice"),
        now=t0 + timedelta(minutes=30),
    )
    assert r_ch_cap.verdict == "deferred"
    assert r_ch_cap.verdict_rule == "freq_cap:channel_24h:voice"

    # SMS #1 succeeds (cross-channel 24h = 3)
    r3 = await submit_action_proposal(
        ActionProposal(entity_id="ent_freq", policy_id="pol_freq", action_type="sms", channel="sms"),
        now=t0 + timedelta(minutes=45),
    )
    assert r3.status == "succeeded"

    # WeChat hits cross-channel 24h cap (3)
    r_24h_cap = await submit_action_proposal(
        ActionProposal(entity_id="ent_freq", policy_id="pol_freq", action_type="wechat_msg", channel="wechat"),
        now=t0 + timedelta(minutes=60),
    )
    assert r_24h_cap.verdict == "deferred"
    assert r_24h_cap.verdict_rule == "freq_cap:cross_channel_24h"

    # Two days later: 4th touch succeeds, 5th hits 7d hard block
    t_day3 = t0 + timedelta(days=2)
    r4 = await submit_action_proposal(
        ActionProposal(entity_id="ent_freq", policy_id="pol_freq", action_type="wechat_msg", channel="wechat"),
        now=t_day3,
    )
    assert r4.status == "succeeded"

    r_7d_block = await submit_action_proposal(
        ActionProposal(entity_id="ent_freq", policy_id="pol_freq", action_type="wechat_msg", channel="wechat"),
        now=t_day3 + timedelta(minutes=20),
    )
    assert r_7d_block.status == "guardrail_blocked"
    assert r_7d_block.verdict == "blocked"
    assert r_7d_block.verdict_rule == "freq_cap:cross_channel_7d"


async def test_outbox_guardrail_forbidden_words_blacklist_and_human_takeover():
    """Block forbidden outbound promises, DND/blacklisted entities, and defer during human takeover."""
    now = datetime(2026, 11, 23, 3, 0, 0, tzinfo=timezone.utc)
    await _create_entity("ent_comp")

    # Forbidden word scan
    r_bad = await submit_action_proposal(
        ActionProposal(
            entity_id="ent_comp",
            action_type="send_wechat",
            channel="wechat",
            payload={"message": "放心，我们有内部关系保证放款"},
        ),
        now=now,
    )
    assert r_bad.status == "guardrail_blocked"
    assert r_bad.verdict_rule == "outbound_forbidden_word:保证放款"

    # Human takeover active -> deferred until takeover ends
    await ingest_event(
        RSIEventEnvelope(
            event_id="tk_1",
            event_type="human.takeover_started",
            category="human_touch",
            entity_id="ent_comp",
            occurred_at=now,
            actor_type="human_rep",
            actor_id="rep_9",
            facts={"takeover_minutes": 40},
        )
    )
    r_tk = await submit_action_proposal(
        ActionProposal(
            entity_id="ent_comp",
            action_type="send_wechat",
            channel="wechat",
            payload={"message": "常规回访"},
        ),
        now=now + timedelta(minutes=5),
    )
    assert r_tk.status == "deferred"
    assert r_tk.verdict_rule == "boundary:human_takeover_active"
    assert r_tk.scheduled_at.replace(tzinfo=timezone.utc) == now + timedelta(minutes=40)

    # DND boundary blocks outbound
    await ingest_event(
        RSIEventEnvelope(
            event_id="dnd_1",
            event_type="risk.dnd_set",
            category="risk_boundary",
            entity_id="ent_comp",
            occurred_at=now + timedelta(minutes=6),
            facts={"dnd": True},
        )
    )
    r_dnd = await submit_action_proposal(
        ActionProposal(
            entity_id="ent_comp",
            action_type="send_wechat",
            channel="wechat",
            payload={"message": "正常消息"},
        ),
        now=now + timedelta(minutes=7),
    )
    assert r_dnd.status == "guardrail_blocked"
    assert r_dnd.verdict_rule == "boundary:dnd"


async def test_outbox_guardrail_kill_switch_tenant_and_channel_pause():
    """Channel and tenant kill switches defer affected channels cleanly until resumed."""
    now = datetime(2026, 11, 23, 3, 0, 0, tzinfo=timezone.utc)
    await _create_entity("ent_ks", tenant_id="t_ks")

    await set_kill_switch("t_ks", "channel", "sms", True, reason="sms_gateway_maintenance")
    r_sms = await submit_action_proposal(
        ActionProposal(tenant_id="t_ks", entity_id="ent_ks", action_type="send_sms", channel="sms"),
        now=now,
    )
    assert r_sms.status == "deferred"
    assert r_sms.verdict_rule == "kill_switch:channel:sms"

    # Unpaused channel still succeeds
    r_wx = await submit_action_proposal(
        ActionProposal(tenant_id="t_ks", entity_id="ent_ks", action_type="send_wechat", channel="wechat"),
        now=now,
    )
    assert r_wx.status == "succeeded"

    # Tenant pause defers wechat as well
    await set_kill_switch("t_ks", "tenant", "t_ks", True, reason="tenant_pause")
    r_wx2 = await submit_action_proposal(
        ActionProposal(tenant_id="t_ks", entity_id="ent_ks", action_type="send_wechat", channel="wechat"),
        now=now + timedelta(minutes=1),
    )
    assert r_wx2.status == "deferred"
    assert r_wx2.verdict_rule == "kill_switch:tenant:t_ks"

    # Resume switches -> deferred actions dispatch cleanly once due
    await set_kill_switch("t_ks", "channel", "sms", False)
    await set_kill_switch("t_ks", "tenant", "t_ks", False)
    dispatched = await dispatch_due_outbox_actions(now=now + timedelta(minutes=10))
    assert dispatched == 2


async def test_outbox_permission_l3_blocked_and_l2_deferred_approval_lifecycle():
    """L3 actions hard-block regardless of proposal level; L2 actions suspend and resume on approval."""
    now = datetime(2026, 11, 23, 3, 0, 0, tzinfo=timezone.utc)
    await FollowupPolicy.create(
        tenant_id="default",
        policy_id="pol_perm",
        config={
            "window_enforced_channels": [],
            "action_permissions": {
                "forbidden_transfer": "L3",
                "offer_special_rate": "L2",
            },
        },
        is_active=True,
    )
    await _create_entity("ent_perm", policy_id="pol_perm")

    # Agent tries passing L0 for a policy-L3 action -> policy overrides upward and blocks
    r_l3 = await submit_action_proposal(
        ActionProposal(
            entity_id="ent_perm",
            policy_id="pol_perm",
            action_type="forbidden_transfer",
            channel="crm",
            permission_level="L0",
        ),
        now=now,
    )
    assert r_l3.status == "guardrail_blocked"
    assert r_l3.verdict_rule == "permission:L3_forbidden"

    # L2 action pauses in awaiting_approval
    r_l2 = await submit_action_proposal(
        ActionProposal(
            entity_id="ent_perm",
            policy_id="pol_perm",
            action_type="offer_special_rate",
            channel="wechat",
            payload={"rate": "2.8%"},
        ),
        now=now,
    )
    assert r_l2.status == "awaiting_approval"
    assert r_l2.verdict == "awaiting_approval"

    pending = await list_pending_outbox_approvals(entity_id="ent_perm")
    assert len(pending) == 1
    assert pending[0]["outbox_id"] == r_l2.outbox_id

    # Approve -> dispatches via edge adapter and records ownership approval in L2
    approved_res = await resolve_outbox_approval(
        r_l2.outbox_id,
        approved=True,
        reviewer="risk_mgr",
        comment="verified customer profile",
        now=now + timedelta(minutes=1),
    )
    assert approved_res.already_resolved is False
    assert approved_res.approved is True
    assert approved_res.status == "succeeded"
    assert approved_res.execution_status == "pending_crm"

    # Idempotent second call
    dup_res = await resolve_outbox_approval(r_l2.outbox_id, approved=False)
    assert dup_res.already_resolved is True

    state = await get_entity_state("default", "ent_perm")
    assert state is not None
    assert state.ownership["pending_approvals"][f"outbox_{r_l2.outbox_id}"]["status"] == "approved"

    # Rejection flow
    r_l2_rej = await submit_action_proposal(
        ActionProposal(
            entity_id="ent_perm",
            policy_id="pol_perm",
            action_type="offer_special_rate",
            channel="wechat",
            payload={"rate": "1.9%"},
        ),
        now=now + timedelta(minutes=2),
    )
    rej_res = await resolve_outbox_approval(
        r_l2_rej.outbox_id,
        approved=False,
        reviewer="risk_mgr",
        comment="rate too low",
        now=now + timedelta(minutes=3),
    )
    assert rej_res.approved is False
    assert rej_res.status == "cancelled"


async def test_crm_passive_adapter_store_query_and_ack_api():
    """CrmPassiveAdapter writes ActionResultStore for CRM HTTP query and ack."""
    now = datetime(2026, 11, 23, 4, 0, 0, tzinfo=timezone.utc)
    await _create_entity("ent_crm", tenant_id="t_crm")

    res = await submit_action_proposal(
        ActionProposal(
            tenant_id="t_crm",
            entity_id="ent_crm",
            action_type="send_wechat_card",
            channel="wechat",
            payload={"template_id": "tpl_car_01", "hint": "发送精评预约卡片"},
        ),
        now=now,
    )
    assert res.status == "succeeded"
    assert res.execution_status == "pending_crm"
    assert res.provider_ref == f"crm_outbox_{res.outbox_id}"

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        q_resp = await client.get(
            "/v1/outbox/results",
            params={"tenant_id": "t_crm", "entity_id": "ent_crm"},
        )
        assert q_resp.status_code == 200
        q_data = q_resp.json()
        assert q_data["total"] == 1
        item = q_data["items"][0]
        assert item["execution_status"] == "pending_crm"
        assert item["proposal_snapshot"]["payload"]["template_id"] == "tpl_car_01"
        assert item["crm_acked_at"] is None

        ack_resp = await client.post(
            f"/v1/outbox/results/{item['id']}/ack",
            params={"tenant_id": "t_crm"},
        )
        assert ack_resp.status_code == 200
        assert ack_resp.json()["crm_acked_at"] is not None


async def test_voice_adapter_city_matching_gateway_and_priority_queue():
    """VoiceChannelAdapter matches city caller number and indexes into VoicePriorityQueue."""
    now = datetime(2026, 11, 23, 3, 0, 0, tzinfo=timezone.utc)  # 11:00 CST (in working window)
    await FollowupPolicy.create(
        tenant_id="default",
        policy_id="pol_voice",
        config={
            "city_caller_map": {
                "杭州": {"caller_number": "0571-88880001", "line_id": "line_hz_1"}
            }
        },
        is_active=True,
    )
    await _create_entity("ent_voice", policy_id="pol_voice")

    mock_redis = MagicMock()
    mock_redis.eval = AsyncMock(return_value=1)
    vq = VoicePriorityQueue(mock_redis, interval_ms=500)

    reg = AdapterRegistry()
    reg.register(CrmPassiveAdapter())
    reg.register(WebhookChannelAdapter())
    reg.register(VoiceChannelAdapter(priority_queue=vq))

    res = await submit_action_proposal(
        ActionProposal(
            entity_id="ent_voice",
            policy_id="pol_voice",
            action_type="outbound_call",
            channel="voice",
            priority=9,
            payload={
                "city": "杭州市",
                "phone_number": "13800138000",
                "opening_script": "您好，这里是杭州汽车金融中心",
            },
        ),
        adapter_registry=reg,
        now=now,
    )
    assert res.status == "succeeded"
    assert res.execution_status == "succeeded"
    assert res.provider_ref and res.provider_ref.startswith("mock_rsi_call_")
    mock_redis.eval.assert_awaited_once()

    stored = await query_action_results("default", entity_id="ent_voice")
    assert stored.total == 1
    resp_json = stored.items[0].adapter_response
    assert resp_json["city_caller"]["city_matched"] is True
    assert resp_json["city_caller"]["caller_number"] == "0571-88880001"
    assert resp_json["city_caller"]["line_id"] == "line_hz_1"
    assert resp_json["dispatch_payload"]["opening_script"] == "您好，这里是杭州汽车金融中心"


async def test_rule_gateway_withdraws_pending_outbox_on_blacklist_and_admin_http_routes():
    """High-risk customer keyword auto-cancels pending/awaiting_approval outbox rows; admin routes work end-to-end."""
    now = datetime(2026, 11, 23, 3, 0, 0, tzinfo=timezone.utc)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Create policy via admin API
        pol_resp = await client.post(
            "/v1/policies",
            json={
                "tenant_id": "t_admin",
                "policy_id": "pol_admin",
                "name": "Admin Test Policy",
                "config": {
                    "action_permissions": {"high_risk_offer": "L2"},
                    "window_enforced_channels": [],
                },
            },
        )
        assert pol_resp.status_code == 200
        assert pol_resp.json()["policy_id"] == "pol_admin"

        get_pol = await client.get("/v1/policies/pol_admin", params={"tenant_id": "t_admin"})
        assert get_pol.status_code == 200

        # 2. Register tenant auth
        auth_resp = await client.post(
            "/v1/auth/register",
            json={
                "tenant_id": "t_admin",
                "app_id": "crm_prod",
                "app_secret": "sec_123",
                "name": "CRM Prod",
                "allowed_policies": ["pol_admin"],
            },
        )
        assert auth_resp.status_code == 200
        assert await RsiTenantAuth.filter(tenant_id="t_admin", app_id="crm_prod").exists()

        # 3. Create entity & two pending outbox items (future scheduled + awaiting_approval)
        await _create_entity("ent_withdraw", tenant_id="t_admin", policy_id="pol_admin")
        ob_sched = await submit_action_proposal(
            ActionProposal(
                tenant_id="t_admin",
                entity_id="ent_withdraw",
                policy_id="pol_admin",
                action_type="followup_call",
                channel="voice",
                scheduled_at=now + timedelta(hours=2),
            ),
            now=now,
        )
        ob_appr = await submit_action_proposal(
            ActionProposal(
                tenant_id="t_admin",
                entity_id="ent_withdraw",
                policy_id="pol_admin",
                action_type="high_risk_offer",
                channel="wechat",
            ),
            now=now,
        )
        assert ob_sched.status == "scheduled"
        assert ob_appr.status == "awaiting_approval"

        pending_http = await client.get("/v1/approvals/pending", params={"tenant_id": "t_admin"})
        assert pending_http.status_code == 200
        assert pending_http.json()["total"] == 1

        # 4. Customer triggers high-risk keyword -> Step 2 + Step 3 automatically cancel pending tasks & outbox
        ev_res = await ingest_event(
            RSIEventEnvelope(
                event_id="wd_kw_1",
                tenant_id="t_admin",
                event_type="wechat.msg_received",
                category="wechat",
                entity_id="ent_withdraw",
                actor_type="customer",
                occurred_at=now + timedelta(minutes=1),
                facts={"text": "不要再打，我要去12315投诉你们"},
            )
        )
        assert ev_res.rule_tier == "L1_REFLEX_BLACKLIST"
        assert ev_res.cancelled_task_count >= 2
        assert (await ActionOutbox.get(id=ob_sched.outbox_id)).status == "cancelled"
        assert (await ActionOutbox.get(id=ob_appr.outbox_id)).status == "cancelled"

        # 5. Test kill switch HTTP routes
        p_resp = await client.post(
            "/v1/control/pause",
            json={"tenant_id": "t_admin", "scope_type": "channel", "target_id": "voice", "reason": "ops"},
        )
        assert p_resp.status_code == 200 and p_resp.json()["paused"] is True

        st_resp = await client.get("/v1/control/status", params={"tenant_id": "t_admin"})
        assert st_resp.status_code == 200 and st_resp.json()["total"] == 1

        r_resp = await client.post(
            "/v1/control/resume",
            json={"tenant_id": "t_admin", "scope_type": "channel", "target_id": "voice"},
        )
        assert r_resp.status_code == 200 and r_resp.json()["paused"] is False
