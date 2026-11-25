from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from tortoise import Tortoise

from app.rsi.coalescer import message_coalescer
from app.rsi.memory_reducer import get_entity_state, ingest_event
from app.rsi.models import EntityEventLog, FollowupEntity, FollowupPolicy, RsiTask
from app.rsi.schemas import RSIEventEnvelope
from app.rsi.timers import schedule_timer, schedule_watchdog
from app.rsi.worker import (
    RsiInboxWorker,
    list_dead_letter_tasks,
    requeue_dead_letter_task,
)

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def setup_rsi_step2_db():
    message_coalescer.reset()
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["app.rsi.models"]},
    )
    await Tortoise.generate_schemas()
    yield
    message_coalescer.reset()
    await Tortoise.close_connections()


def _ev(
    event_id: str,
    event_type: str,
    category: str,
    occurred_at: datetime,
    *,
    tenant_id: str = "default",
    entity_id: str = "ent_s2",
    actor_type: str = "system",
    actor_id: str | None = None,
    source: str = "external",
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
        actor_type=actor_type,  # type: ignore[arg-type]
        actor_id=actor_id,
        source=source,
        summary=summary,
        facts=facts or {},
        extra=extra or {},
        policy_id=policy_id,
    )


async def test_rule_gateway_zero_llm_blacklist_and_cancel_on_high_risk_keyword():
    """High-risk customer keywords trigger 0-LLM blacklist + cancel all queued tasks."""
    now = datetime.now(timezone.utc)
    eid = "ent_risk_1"
    await FollowupPolicy.create(
        tenant_id="default",
        policy_id="pol_risk",
        config={"high_risk_keywords": ["找律师"]},
        is_active=True,
    )

    r1 = await ingest_event(
        _ev(
            "rk_1",
            "voice.call_ended",
            "voice",
            now,
            entity_id=eid,
            policy_id="pol_risk",
            facts={"answered": False, "outcome": "no_answer"},
        )
    )
    assert r1.rule_tier == "L2_COGNITIVE_WAKEUP"
    assert r1.scheduled_task_id is not None

    # Customer uses default high-risk keyword
    r2 = await ingest_event(
        _ev(
            "rk_2",
            "wechat.msg_received",
            "wechat",
            now + timedelta(seconds=10),
            entity_id=eid,
            actor_type="customer",
            facts={"text": "别再打了，我要去工信部投诉你们"},
        )
    )
    assert r2.rule_tier == "L1_REFLEX_BLACKLIST"
    assert r2.matched_rule.startswith("high_risk_keyword:")
    assert r2.scheduled_task_id is None
    assert r2.cancelled_task_count >= 1

    state = await get_entity_state("default", eid)
    assert state is not None
    assert state.status == "blacklisted"
    assert state.hard_boundaries["blacklisted"] is True

    prev_task = await RsiTask.get(id=r1.scheduled_task_id)
    assert prev_task.status == "cancelled"

    # Subsequent event on blacklisted entity stays blocked at L1 without wakeup
    r3 = await ingest_event(
        _ev(
            "rk_3",
            "app.loan_simulated",
            "content_engagement",
            now + timedelta(seconds=20),
            entity_id=eid,
            actor_type="customer",
            facts={"score": 99},
        )
    )
    assert r3.rule_tier == "L1_REFLEX_CANCEL"
    assert r3.matched_rule == "entity_blacklisted"
    assert r3.scheduled_task_id is None

    # Custom policy keyword test
    r_custom = await ingest_event(
        _ev(
            "rk_custom",
            "wechat.msg_received",
            "wechat",
            now,
            entity_id="ent_risk_custom",
            policy_id="pol_risk",
            actor_type="customer",
            facts={"text": "我准备找律师处理"},
        )
    )
    assert r_custom.rule_tier == "L1_REFLEX_BLACKLIST"
    assert r_custom.matched_rule == "high_risk_keyword:找律师"


async def test_rule_gateway_converted_and_wechat_added_zero_llm_cancel():
    """order.paid and wechat.friend_added (by policy) cancel pending tasks with 0 LLM."""
    now = datetime.now(timezone.utc)

    # 1. Conversion cancels pending tasks
    e_conv = "ent_conv_cancel"
    r1 = await ingest_event(
        _ev("cv_1", "voice.call_ended", "voice", now, entity_id=e_conv, facts={"answered": True})
    )
    assert r1.scheduled_task_id is not None

    r2 = await ingest_event(
        _ev(
            "cv_2",
            "order.paid",
            "finance_order",
            now + timedelta(seconds=5),
            entity_id=e_conv,
            facts={"order_id": "ord_1", "status": "paid", "amount": 8000},
        )
    )
    assert r2.rule_tier == "L1_REFLEX_CANCEL"
    assert r2.matched_rule == "entity_converted_cancel"
    assert r2.cancelled_task_count == 1
    assert r2.scheduled_task_id is None
    assert (await RsiTask.get(id=r1.scheduled_task_id)).status == "cancelled"

    # 2. WeChat friend added: default cancels queued chase; policy flag enables handover wakeup
    e_wx = "ent_wx_default"
    rw1 = await ingest_event(
        _ev("wx_1", "voice.call_ended", "voice", now, entity_id=e_wx, facts={"answered": True})
    )
    assert rw1.scheduled_task_id is not None

    rw2 = await ingest_event(
        _ev("wx_2", "wechat.friend_added", "wechat", now + timedelta(seconds=2), entity_id=e_wx, facts={"wechat_added": True})
    )
    assert rw2.rule_tier == "L1_REFLEX_CANCEL"
    assert rw2.matched_rule == "wechat_added_cancel"
    assert rw2.cancelled_task_count == 1
    assert rw2.scheduled_task_id is None

    await FollowupPolicy.create(
        tenant_id="default",
        policy_id="pol_wx_wakeup",
        config={"wakeup_on_wechat_added": True},
        is_active=True,
    )
    rw3 = await ingest_event(
        _ev(
            "wx_3",
            "wechat.friend_added",
            "wechat",
            now,
            entity_id="ent_wx_wakeup",
            policy_id="pol_wx_wakeup",
            facts={"wechat_added": True},
        )
    )
    assert rw3.rule_tier == "L2_COGNITIVE_WAKEUP"
    assert rw3.matched_rule == "wechat_added_handover"
    assert rw3.scheduled_task_id is not None


async def test_rule_gateway_human_takeover_backoff_and_resume():
    """Human takeover pauses automatic wakeups; human.takeover_ended resumes AI wakeup."""
    now = datetime.now(timezone.utc)
    eid = "ent_takeover"

    r_voice = await ingest_event(
        _ev("ht_0", "voice.call_ended", "voice", now, entity_id=eid, facts={"answered": True})
    )
    assert r_voice.scheduled_task_id is not None

    r_take = await ingest_event(
        _ev(
            "ht_1",
            "human.takeover_started",
            "human_touch",
            now + timedelta(seconds=5),
            entity_id=eid,
            actor_type="human_rep",
            actor_id="rep_01",
            facts={"takeover_minutes": 45},
        )
    )
    assert r_take.rule_tier == "L1_REFLEX_TAKEOVER"
    assert r_take.matched_rule == "human_takeover_backoff"
    assert r_take.cancelled_task_count == 1
    assert (await RsiTask.get(id=r_voice.scheduled_task_id)).status == "cancelled"

    # Incoming customer event while inside takeover window must back off
    r_mid = await ingest_event(
        _ev(
            "ht_2",
            "wechat.msg_received",
            "wechat",
            now + timedelta(minutes=10),
            entity_id=eid,
            actor_type="customer",
            facts={"text": "人工老师刚跟我说了报价"},
        )
    )
    assert r_mid.rule_tier == "L1_REFLEX_TAKEOVER"
    assert r_mid.matched_rule == "human_takeover_active"
    assert r_mid.scheduled_task_id is None

    # Ending human takeover wakes cognitive loop
    r_end = await ingest_event(
        _ev(
            "ht_3",
            "human.takeover_ended",
            "human_touch",
            now + timedelta(minutes=15),
            entity_id=eid,
            actor_type="human_rep",
            actor_id="rep_01",
        )
    )
    assert r_end.rule_tier == "L2_COGNITIVE_WAKEUP"
    assert r_end.matched_rule == "human_takeover_resumed"
    assert r_end.scheduled_task_id is not None


async def test_rule_gateway_active_engagement_priority_boost_vs_passive_silent():
    """High-intent engagement boosts future queued tasks ahead of low-priority ones; passive is silent."""
    now = datetime.now(timezone.utc)
    eid = "ent_boost"

    low_task = await schedule_timer(
        "default",
        eid,
        "cognitive_wakeup",
        now + timedelta(hours=2),
        payload={"reason": "regular_followup"},
        priority=3,
    )
    other_task = await schedule_timer(
        "default",
        "ent_other_normal",
        "cognitive_wakeup",
        now - timedelta(minutes=1),
        payload={"reason": "due_earlier_normal"},
        priority=5,
    )

    # Passive page view -> L0_SILENT, does not boost or wake
    r_passive = await ingest_event(
        _ev(
            "ce_1",
            "app.page_view",
            "content_engagement",
            now,
            entity_id=eid,
            actor_type="customer",
            facts={"page": "about_us", "score": 15},
        )
    )
    assert r_passive.rule_tier == "L0_SILENT"
    assert r_passive.matched_rule == "passive_content_silent"
    assert (await RsiTask.get(id=low_task.id)).priority == 3

    # Active loan simulator event -> L2_PRIORITY_BOOST
    r_hot = await ingest_event(
        _ev(
            "ce_2",
            "app.loan_simulated",
            "content_engagement",
            now + timedelta(seconds=1),
            entity_id=eid,
            actor_type="customer",
            facts={"page": "loan_calculator", "score": 92},
        )
    )
    assert r_hot.rule_tier == "L2_PRIORITY_BOOST"
    assert r_hot.matched_rule == "active_engagement_boost"

    boosted = await RsiTask.get(id=low_task.id)
    assert boosted.priority == 9
    assert boosted.payload.get("boosted") is True

    # Worker claims priority=9 boosted task BEFORE priority=5 other_task
    claimed_order: list[str] = []

    async def handler(ctx):
        claimed_order.append(ctx["entity_id"])
        return {"ok": True}

    worker = RsiInboxWorker(cognitive_handler=handler, clock=lambda: datetime.now(timezone.utc) + timedelta(seconds=2))
    assert await worker.run_once() is True
    assert await worker.run_once() is True
    assert claimed_order == [eid, "ent_other_normal"]
    assert other_task.id is not None


async def test_causal_loop_guard_action_receipt_is_silent():
    """Outbound action receipts / rsi_outbox events never re-trigger cognitive wakeups."""
    now = datetime.now(timezone.utc)
    r1 = await ingest_event(
        _ev(
            "loop_1",
            "adapter.receipt",
            "action_receipt",
            now,
            entity_id="ent_loop",
            source="rsi_outbox",
            facts={"action_id": "a_1", "ok": True},
        )
    )
    assert r1.rule_tier == "L0_SILENT"
    assert r1.matched_rule == "causal_loop_guard"
    assert r1.scheduled_task_id is None


async def test_wechat_3_level_coalescing_debounce_preemption_and_presend_guard():
    """Verify Level 1 debounce merge, Level 2 in-flight preemption, and Level 3 optimistic seq guard."""
    now = datetime.now(timezone.utc)

    # --- Level 1: Debounce Coalescing ---
    await FollowupPolicy.create(
        tenant_id="default",
        policy_id="pol_wx_l1",
        config={"wechat_debounce_ms": 2000},
        is_active=True,
    )
    eid_l1 = "ent_wx_l1"
    r1 = await ingest_event(_ev("l1_m1", "wechat.msg_received", "wechat", now, entity_id=eid_l1, policy_id="pol_wx_l1", actor_type="customer", facts={"text": "在吗"}))
    r2 = await ingest_event(_ev("l1_m2", "wechat.msg_received", "wechat", now + timedelta(milliseconds=200), entity_id=eid_l1, actor_type="customer", facts={"text": "21年帕萨特"}))
    r3 = await ingest_event(_ev("l1_m3", "wechat.msg_received", "wechat", now + timedelta(milliseconds=400), entity_id=eid_l1, actor_type="customer", facts={"text": "能贷多少"}))

    assert r1.scheduled_task_id == r2.scheduled_task_id == r3.scheduled_task_id
    deb_task = await RsiTask.get(id=r1.scheduled_task_id)
    assert deb_task.payload["event_ids"] == ["l1_m1", "l1_m2", "l1_m3"]
    assert deb_task.payload["latest_msg_seq"] == 3

    batched_events: list[list[str]] = []

    async def l1_handler(ctx):
        batched_events.append([e["event_id"] for e in ctx["events"]])
        return {"replied": True}

    worker_l1 = RsiInboxWorker(
        cognitive_handler=l1_handler,
        clock=lambda: datetime.now(timezone.utc) + timedelta(seconds=10),
    )
    assert await worker_l1.run_once() is True
    assert batched_events == [["l1_m1", "l1_m2", "l1_m3"]]
    assert await RsiTask.filter(tenant_id="default", entity_id=eid_l1, status="succeeded").count() == 1

    # --- Level 2: In-Flight Preemption ---
    await FollowupPolicy.create(
        tenant_id="default",
        policy_id="pol_wx_l2",
        config={"wechat_debounce_ms": 0},
        is_active=True,
    )
    eid_l2 = "ent_wx_l2"
    await ingest_event(
        _ev("l2_m1", "wechat.msg_received", "wechat", now, entity_id=eid_l2, policy_id="pol_wx_l2", actor_type="customer", facts={"text": "第一句"})
    )

    run1_started = asyncio.Event()
    observed_batches: list[list[str]] = []

    async def preemptible_handler(ctx):
        ev_ids = [e["event_id"] for e in ctx["events"]]
        if ev_ids == ["l2_m1"]:
            run1_started.set()
            await asyncio.sleep(5)  # Simulate slow LLM inference interrupted by Level 2
        observed_batches.append(ev_ids)
        return {"merged_count": len(ev_ids)}

    worker_l2 = RsiInboxWorker(
        cognitive_handler=preemptible_handler,
        clock=lambda: datetime.now(timezone.utc) + timedelta(seconds=2),
    )
    inflight_worker_coro = asyncio.create_task(worker_l2.run_once())
    await asyncio.wait_for(run1_started.wait(), timeout=2.0)

    # Customer sends second short message mid-inference -> Level 2 interrupts first run
    r_preempt = await ingest_event(
        _ev("l2_m2", "wechat.msg_received", "wechat", now + timedelta(seconds=1), entity_id=eid_l2, actor_type="customer", facts={"text": "还有行驶证"})
    )
    assert r_preempt.inbound_msg_seq == 2
    assert await inflight_worker_coro is True

    superseded = await RsiTask.filter(tenant_id="default", entity_id=eid_l2, status="superseded").all()
    assert len(superseded) == 1
    assert superseded[0].result["status"] == "preempted"

    # Next run picks up BOTH unconsumed messages together and succeeds
    assert await worker_l2.run_once() is True
    assert observed_batches == [["l2_m1", "l2_m2"]]
    assert await EntityEventLog.filter(entity_id=eid_l2, ingest_status="processed").count() == 2

    # --- Level 3: Pre-Send Optimistic msg_seq Guard ---
    eid_l3 = "ent_wx_l3"
    r_l3 = await ingest_event(
        _ev("l3_m1", "wechat.msg_received", "wechat", now, entity_id=eid_l3, policy_id="pol_wx_l2", actor_type="customer", facts={"text": "发消息前竞态测试"})
    )

    async def stale_race_handler(ctx):
        # Simulate unseen concurrent DB update advancing inbound_msg_seq before send
        await FollowupEntity.filter(tenant_id="default", entity_id=eid_l3).update(inbound_msg_seq=99)
        return {"draft": "stale reply"}

    worker_l3 = RsiInboxWorker(
        cognitive_handler=stale_race_handler,
        clock=lambda: datetime.now(timezone.utc) + timedelta(seconds=2),
    )
    assert await worker_l3.run_once() is True

    l3_task = await RsiTask.get(id=r_l3.scheduled_task_id)
    assert l3_task.status == "superseded"
    assert l3_task.result["status"] == "discarded_stale"
    # Event stays ingested and a fresh wakeup task was automatically ensured
    assert await EntityEventLog.filter(entity_id=eid_l3, event_id="l3_m1", ingest_status="ingested").exists()
    assert await RsiTask.filter(tenant_id="default", entity_id=eid_l3, task_type="cognitive_wakeup", status="pending").exists()


async def test_commitment_auto_timer_and_watchdog_anti_leak():
    """Commitment creates due timer; stalled stage fires watchdog recovery while progressed stage skips."""
    now = datetime.now(timezone.utc)
    eid = "ent_timer_wd"
    due = now + timedelta(hours=2)

    r_cm = await ingest_event(
        _ev(
            "cm_1",
            "commitment.created",
            "commitment",
            now,
            entity_id=eid,
            actor_type="customer",
            summary="两小时后看方案",
            facts={"commitment_id": "c_10", "due_at": due.isoformat(), "status": "open"},
        )
    )
    assert r_cm.matched_rule == "commitment_timer_scheduled"
    assert r_cm.scheduled_task_id is not None

    cm_task = await RsiTask.get(id=r_cm.scheduled_task_id)
    assert cm_task.task_type == "timer_commitment"

    # Set lifecycle stage to "precheck" and mount a watchdog
    await ingest_event(
        _ev("stg_1", "lifecycle.stage_changed", "lifecycle", now + timedelta(seconds=1), entity_id=eid, facts={"stage": "precheck"})
    )
    wd_stalled = await schedule_watchdog("default", eid, expected_stage="precheck", delay_seconds=300)

    # Another entity progresses past precheck -> its watchdog must skip cleanly
    eid_ok = "ent_wd_progressed"
    await ingest_event(
        _ev("stg_ok_1", "lifecycle.stage_changed", "lifecycle", now, entity_id=eid_ok, facts={"stage": "precheck"})
    )
    wd_ok = await schedule_watchdog("default", eid_ok, expected_stage="precheck", delay_seconds=300)
    await ingest_event(
        _ev("stg_ok_2", "lifecycle.stage_changed", "lifecycle", now + timedelta(seconds=10), entity_id=eid_ok, facts={"stage": "underwriting"})
    )

    # Advance clock past commitment & watchdogs (note: run_once picks by priority desc, run_at asc)
    future_now = now + timedelta(hours=3)
    processed: list[str] = []

    async def timer_handler(ctx):
        processed.append(f"{ctx['entity_id']}:{ctx['task_type']}")
        return {"ok": True}

    worker = RsiInboxWorker(cognitive_handler=timer_handler, clock=lambda: future_now)
    while await worker.run_once():
        pass

    wd_stalled_row = await RsiTask.get(id=wd_stalled.id)
    wd_ok_row = await RsiTask.get(id=wd_ok.id)
    assert wd_stalled_row.result["triggered"] is True
    assert wd_ok_row.result["triggered"] is False
    assert wd_ok_row.result["skipped"] is True
    assert f"{eid}:timer_commitment" in processed
    assert f"{eid}:cognitive_wakeup" in processed  # recovery wakeup spawned by stalled watchdog


async def test_worker_retry_exponential_backoff_and_dlq_requeue():
    """Task retries with 2^n exponential backoff, enters dead_letter, and replays on requeue."""
    t0 = datetime(2026, 11, 23, 10, 0, 0, tzinfo=timezone.utc)
    fake_now = [t0]

    await FollowupEntity.create(tenant_id="default", entity_id="ent_dlq")
    task = await schedule_timer(
        "default",
        "ent_dlq",
        "cognitive_wakeup",
        t0,
        payload={"reason": "dlq_test"},
        max_retries=2,
    )

    should_fail = [True]

    async def flaky_handler(ctx):
        if should_fail[0]:
            raise RuntimeError("transient_llm_timeout")
        return {"recovered": True}

    worker = RsiInboxWorker(cognitive_handler=flaky_handler, clock=lambda: fake_now[0])

    # Attempt 1 fails -> retry #1 (+2s)
    assert await worker.run_once() is True
    t1 = await RsiTask.get(id=task.id)
    assert t1.status == "retry"
    assert t1.retry_count == 1
    assert t1.run_at.replace(tzinfo=timezone.utc) == t0 + timedelta(seconds=2)

    # Too early (+1s) -> skipped
    fake_now[0] = t0 + timedelta(seconds=1)
    assert await worker.run_once() is False

    # Attempt 2 fails -> retry #2 (+4s from current clock)
    fake_now[0] = t0 + timedelta(seconds=2)
    assert await worker.run_once() is True
    t2 = await RsiTask.get(id=task.id)
    assert t2.status == "retry"
    assert t2.retry_count == 2
    assert t2.run_at.replace(tzinfo=timezone.utc) == fake_now[0] + timedelta(seconds=4)

    # Attempt 3 fails -> exhausts max_retries=2 -> dead_letter
    fake_now[0] = t0 + timedelta(seconds=6)
    assert await worker.run_once() is True
    t3 = await RsiTask.get(id=task.id)
    assert t3.status == "dead_letter"
    assert "RuntimeError: transient_llm_timeout" in (t3.last_error or "")

    dlq_items = await list_dead_letter_tasks(tenant_id="default")
    assert any(item["task_id"] == task.id for item in dlq_items)

    # Manual requeue succeeds once handler recovers
    should_fail[0] = False
    requeued = await requeue_dead_letter_task(task.id)
    assert requeued is not None and requeued.status == "pending"

    fake_now[0] = datetime.now(timezone.utc) + timedelta(seconds=1)
    assert await worker.run_once() is True
    t_final = await RsiTask.get(id=task.id)
    assert t_final.status == "succeeded"
    assert t_final.result["output"] == {"recovered": True}
