from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest
import pytest_asyncio
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from tortoise import Tortoise

from app.rsi.adapters import AdapterRegistry, CrmPassiveAdapter, VoiceChannelAdapter
from app.rsi.adapters.voice.gateway import BaseVoiceGateway, VoiceGatewayResult
from app.rsi.call_planner import (
    CallPlannerAgent,
    classify_sip_disposition,
)
from app.rsi.memory_reducer import ingest_event
from app.rsi.models import (
    ActionOutbox,
    EntityEventLog,
    EntityMemoryState,
    FollowupEntity,
    FollowupPolicy,
    RsiTask,
)
from app.rsi.schemas import RSIEventEnvelope
from app.rsi.steward import StewardAgent
from app.rsi.subflows import SubFlowRegistry
from app.rsi.worker import RsiInboxWorker

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def setup_rsi_step5_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["app.rsi.models"]},
    )
    await Tortoise.generate_schemas()
    yield
    await Tortoise.close_connections()


class _RecordingVoiceGateway(BaseVoiceGateway):
    mode = "recording"

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    async def submit_call(self, request_ctx: Dict[str, Any]) -> VoiceGatewayResult:
        self.calls.append(dict(request_ctx))
        return VoiceGatewayResult(
            status="accepted",
            code="accepted",
            provider_call_id=f"mock_{request_ctx.get('call_id')}",
            dispatch_payload=dict(request_ctx.get("payload") or {}),
        )


async def _create_entity(
    entity_id: str,
    *,
    tenant_id: str = "default",
    policy_id: str = "default",
) -> FollowupEntity:
    return await FollowupEntity.create(
        tenant_id=tenant_id,
        entity_id=entity_id,
        policy_id=policy_id,
        status="active",
    )


async def test_sip_disposition_classification_and_differentiated_scheduling():
    """Classify SIP hangup codes accurately and run 0-LLM fast-path for invalid numbers."""
    assert classify_sip_disposition({"sip_code": 486}) == "instant_reject"
    assert (
        classify_sip_disposition(
            {"sip_code": 603, "answered": True, "duration_sec": 2, "hangup_cause": "秒挂拒接"}
        )
        == "instant_reject"
    )
    assert classify_sip_disposition({"sip_code": 480}) == "powered_off"
    assert classify_sip_disposition({"sip_code": 408}) == "no_answer"
    assert classify_sip_disposition({"sip_code": 404}) == "invalid_number"
    assert classify_sip_disposition({"hangup_cause": "voicemail"}) == "voicemail"
    assert (
        classify_sip_disposition({"answered": True, "duration_sec": 45})
        == "connected"
    )

    llm_calls = 0

    async def should_not_be_called(
        messages: list[ModelMessage], info: AgentInfo
    ) -> ModelResponse:
        nonlocal llm_calls
        llm_calls += 1
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={"decision": "schedule_call"},
                )
            ]
        )

    now = datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc)  # 14:00 CST

    # Entity A: 404 + WeChat added -> 0-LLM switch_to_wechat
    await _create_entity("ent_invalid_wx")
    await EntityMemoryState.create(
        tenant_id="default",
        entity_id="ent_invalid_wx",
        channel_stats={"wechat_added": True},
    )
    planner = CallPlannerAgent(model=FunctionModel(should_not_be_called))
    res_wx = await planner.plan_followup_call(
        "default",
        "ent_invalid_wx",
        trigger_events=[
            {
                "event_id": "v_404_wx",
                "category": "voice",
                "occurred_at": now.isoformat(),
                "facts": {"sip_code": 404, "hangup_cause": "unallocated"},
            }
        ],
        now=now,
    )
    assert llm_calls == 0
    assert res_wx.used_llm is False
    assert res_wx.signals.disposition == "invalid_number"
    assert res_wx.decision.decision == "switch_to_wechat"
    assert len(res_wx.outbox_receipts) == 1
    outbox_wx = await ActionOutbox.filter(tenant_id="default", entity_id="ent_invalid_wx").all()
    assert len(outbox_wx) == 1 and outbox_wx[0].channel == "wechat"

    # Entity B: 404 + no WeChat -> 0-LLM close_followup
    await _create_entity("ent_invalid_close")
    res_close = await planner.plan_followup_call(
        "default",
        "ent_invalid_close",
        trigger_events=[
            {
                "event_id": "v_404_close",
                "category": "voice",
                "occurred_at": now.isoformat(),
                "facts": {"sip_code": 404, "hangup_cause": "空号"},
            }
        ],
        now=now,
    )
    assert llm_calls == 0
    assert res_close.used_llm is False
    assert res_close.decision.decision == "close_followup"
    assert await ActionOutbox.filter(tenant_id="default", entity_id="ent_invalid_close").count() == 0


async def test_verbal_reschedule_exact_timestamp_parsing_and_compound_pre_call_sms():
    """Verbal reschedule parses into exact ISO timestamp + T-5m SMS & city-tagged voice Outbox."""
    await FollowupPolicy.create(
        tenant_id="default",
        policy_id="pol_hz",
        config={
            "working_window": {
                "work_start_time": "09:00:00",
                "work_end_time": "21:00:00",
                "timezone": "Asia/Shanghai",
            },
            "city_caller_map": {
                "杭州": {"caller_number": "0571-88886666", "line_id": "hz_01"}
            },
        },
    )
    await _create_entity("ent_resched_1", policy_id="pol_hz")

    async def resched_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={
                        "decision": "schedule_call",
                        "scheduled_at_iso": "2026-11-24T18:30:00+08:00",
                        "priority": 8,
                        "opening_script": "<think>gentle recall</think>张先生您好，按约定晚上六点半联系您确认方案～",
                        "send_pre_call_sms": True,
                        "pre_call_sms_text": "您好，专属顾问将在5分钟后致电，请留意0571来电。",
                        "verbal_reschedule_detected": True,
                        "verbal_reschedule_quote": "明天下午下班六点半再打",
                        "rationale": "客户口头明确改约次日18:30",
                    },
                )
            ]
        )

    now = datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc)  # 14:00 CST
    planner = CallPlannerAgent(model=FunctionModel(resched_fn))
    res = await planner.plan_followup_call(
        "default",
        "ent_resched_1",
        trigger_events=[
            {
                "event_id": "v_resched_1",
                "category": "voice",
                "occurred_at": now.isoformat(),
                "summary": "今天开会，明天下午下班六点半再打",
                "facts": {
                    "outcome": "connected",
                    "answered": True,
                    "duration_sec": 28,
                    "city": "杭州",
                    "phone_number": "13800138000",
                },
            }
        ],
        now=now,
        dispatch_actions=False,
    )

    expected_voice_utc = datetime(2026, 11, 24, 10, 30, tzinfo=timezone.utc)
    expected_sms_utc = datetime(2026, 11, 24, 10, 25, tzinfo=timezone.utc)
    assert res.resolved_scheduled_at == expected_voice_utc
    assert len(res.outbox_receipts) == 2

    rows = await ActionOutbox.filter(tenant_id="default", entity_id="ent_resched_1").order_by("scheduled_at").all()
    assert len(rows) == 2
    sms_row, voice_row = rows[0], rows[1]
    assert sms_row.channel == "sms"
    assert sms_row.action_type == "send_pre_call_sms"
    assert sms_row.scheduled_at == expected_sms_utc
    assert sms_row.payload["notice_lead_minutes"] == 5

    assert voice_row.channel == "voice"
    assert voice_row.action_type == "schedule_call"
    assert voice_row.scheduled_at == expected_voice_utc
    assert voice_row.payload["city"] == "杭州"
    assert "<think>" not in voice_row.payload["opening_script"]
    assert voice_row.payload["pre_call_sms_attached"] is True
    assert sms_row.correlation_id == voice_row.correlation_id


async def test_shift_worker_repeated_daytime_no_answer_switches_to_evening_call():
    """Repeated daytime unanswered calls auto-shift redials to 19:00 local evening window."""
    await _create_entity("ent_shift_1")
    day1_morning = datetime(2026, 11, 22, 2, 30, tzinfo=timezone.utc)  # 10:30 CST
    day1_afternoon = datetime(2026, 11, 23, 7, 0, tzinfo=timezone.utc)  # 15:00 CST

    await EntityEventLog.create(
        tenant_id="default",
        event_id="ev_sw_1",
        entity_id="ent_shift_1",
        event_type="voice.call_unanswered",
        category="voice",
        occurred_at=day1_morning,
        facts={"sip_code": 408, "hangup_cause": "no_answer"},
    )
    await EntityEventLog.create(
        tenant_id="default",
        event_id="ev_sw_2",
        entity_id="ent_shift_1",
        event_type="voice.call_unanswered",
        category="voice",
        occurred_at=day1_afternoon,
        facts={"sip_code": 480, "hangup_cause": "powered_off"},
    )

    async def shift_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={
                        "decision": "schedule_call",
                        "delay_minutes": 90,
                        "priority": 6,
                        "opening_script": "您好，白天怕打扰您工作，晚上给您简短沟通下方案。",
                    },
                )
            ]
        )

    now = datetime(2026, 11, 23, 8, 0, tzinfo=timezone.utc)  # 16:00 CST
    planner = CallPlannerAgent(model=FunctionModel(shift_fn))
    res = await planner.plan_followup_call("default", "ent_shift_1", now=now, dispatch_actions=False)

    assert res.signals.daytime_miss_count == 2
    assert res.signals.is_shift_worker_candidate is True
    assert res.decision.shift_to_evening is True
    # 19:00 Asia/Shanghai == 11:00 UTC on 2026-11-23
    assert res.resolved_scheduled_at == datetime(2026, 11, 23, 11, 0, tzinfo=timezone.utc)
    voice_out = await ActionOutbox.get(tenant_id="default", entity_id="ent_shift_1", channel="voice")
    assert voice_out.scheduled_at == datetime(2026, 11, 23, 11, 0, tzinfo=timezone.utc)
    assert voice_out.payload["shift_to_evening"] is True


async def test_active_engagement_boost_triggers_immediate_high_priority_callback():
    """High-intent App telemetry boosts priority>=9 and dispatches immediate city-matched voice call."""
    await FollowupPolicy.create(
        tenant_id="default",
        policy_id="pol_city",
        config={
            "city_caller_map": {
                "杭州": {"caller_number": "0571-88990011", "line_id": "hz_vip"}
            }
        },
    )
    await _create_entity("ent_boost_1", policy_id="pol_city")

    gw = _RecordingVoiceGateway()
    adapter_reg = AdapterRegistry()
    adapter_reg.register(CrmPassiveAdapter())
    adapter_reg.register(VoiceChannelAdapter(gateway=gw))

    async def boost_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={
                        "decision": "schedule_call",
                        "priority": 7,
                        "opening_script": "您好！看到您刚在App测算了车贷额度，马上为您核对最优惠方案！",
                        "send_pre_call_sms": True,
                    },
                )
            ]
        )

    now = datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc)  # 14:00 CST
    planner = CallPlannerAgent(model=FunctionModel(boost_fn), adapter_registry=adapter_reg)
    res = await planner.plan_followup_call(
        "default",
        "ent_boost_1",
        trigger_events=[
            {
                "event_id": "ev_app_1",
                "event_type": "app.loan_simulated",
                "category": "content_engagement",
                "occurred_at": now.isoformat(),
                "facts": {
                    "page": "loan_calculator",
                    "boost": True,
                    "city": "杭州",
                    "phone_number": "13800001111",
                },
            }
        ],
        now=now,
        submit_to_outbox=True,
        dispatch_actions=True,
    )

    assert res.signals.active_engagement_boost is True
    assert res.resolved_scheduled_at == now
    assert len(gw.calls) == 1
    assert gw.calls[0]["city_caller"] == {
        "matched_city": "杭州",
        "caller_number": "0571-88990011",
        "line_id": "hz_vip",
        "city_matched": True,
    }
    voice_out = await ActionOutbox.get(tenant_id="default", entity_id="ent_boost_1", channel="voice")
    assert voice_out.priority >= 9
    assert voice_out.status == "succeeded"


async def test_steward_routes_wechat_nudge_escalate_human_wait_timer_and_blacklist_noop():
    """StewardAgent executes wechat_nudge, escalate_human, wait_and_monitor, and 0-LLM blacklist noop."""
    now = datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc)

    # 1) send_wechat_nudge + <think> stripping
    await _create_entity("ent_st_wx")
    steward_wx = StewardAgent(
        model=FunctionModel(
            lambda msgs, info: ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={
                            "route": "send_wechat_nudge",
                            "wechat_message": "<think>soft touch</think>您好，方案测算已备好，方便时发我看下～",
                            "rationale": "<reasoning>check</reasoning>温和提醒",
                            "priority": 6,
                        },
                    )
                ]
            )
        )
    )
    res_wx = await steward_wx.handle_cognitive_batch(
        {"tenant_id": "default", "entity_id": "ent_st_wx", "events": []}, now=now
    )
    assert res_wx["route"] == "send_wechat_nudge"
    wx_out = await ActionOutbox.get(tenant_id="default", entity_id="ent_st_wx")
    assert wx_out.channel == "wechat"
    assert wx_out.payload["text"] == "您好，方案测算已备好，方便时发我看下～"

    # 2) escalate_human
    await _create_entity("ent_st_hum")
    steward_hum = StewardAgent(
        model=FunctionModel(
            lambda msgs, info: ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={
                            "route": "escalate_human",
                            "handoff_reason": "客户要求资深经理直接对接大额授信",
                            "priority": 9,
                        },
                    )
                ]
            )
        )
    )
    res_hum = await steward_hum.handle_cognitive_batch(
        {"tenant_id": "default", "entity_id": "ent_st_hum", "events": []}, now=now
    )
    assert res_hum["route"] == "escalate_human"
    hum_out = await ActionOutbox.get(tenant_id="default", entity_id="ent_st_hum")
    assert hum_out.channel == "crm_handoff"
    assert hum_out.action_type == "escalate_human"

    # 3) wait_and_monitor -> creates timer_self_check RsiTask
    await _create_entity("ent_st_wait")
    steward_wait = StewardAgent(
        model=FunctionModel(
            lambda msgs, info: ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={
                            "route": "wait_and_monitor",
                            "next_check_minutes": 45,
                            "rationale": "等待客户查看方案",
                        },
                    )
                ]
            )
        )
    )
    res_wait = await steward_wait.handle_cognitive_batch(
        {"tenant_id": "default", "entity_id": "ent_st_wait", "events": []}, now=now
    )
    assert res_wait["route"] == "wait_and_monitor"
    assert res_wait["scheduled_task_id"] is not None
    t_wait = await RsiTask.get(id=res_wait["scheduled_task_id"])
    assert t_wait.task_type == "timer_self_check"
    assert t_wait.run_at == now + timedelta(minutes=45)

    # 4) Blacklisted -> 0-LLM noop
    await _create_entity("ent_st_bl")
    await EntityMemoryState.create(
        tenant_id="default",
        entity_id="ent_st_bl",
        hard_boundaries={"blacklisted": True},
    )
    llm_called = 0

    def spy_model(msgs, info):
        nonlocal llm_called
        llm_called += 1
        return ModelResponse(parts=[])

    steward_bl = StewardAgent(model=FunctionModel(spy_model))
    res_bl = await steward_bl.handle_cognitive_batch(
        {"tenant_id": "default", "entity_id": "ent_st_bl", "events": []}, now=now
    )
    assert llm_called == 0
    assert res_bl["used_llm"] is False
    assert res_bl["route"] == "noop"
    assert "entity_blacklisted" in res_bl["notes"]


async def test_steward_delegates_to_subflow_and_advances_stage_on_typed_conclusion():
    """Steward delegates inbound message to subflow, saves typed conclusion, and promotes stage."""
    await _create_entity("ent_st_sf")
    reg = SubFlowRegistry()
    reg.register_yaml(
        """
subflow_id: qual_step
description: Initial qualification subflow
initial_node_id: collect_qual
nodes:
  collect_qual:
    prompt: Extract car_age and city
    output_fields:
      - name: car_age
        type: int
        required: true
      - name: city
        type: str
        required: true
    completion_reply: 初审资料已登记完毕！
"""
    )

    steward_calls = 0

    async def steward_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        nonlocal steward_calls
        steward_calls += 1
        if steward_calls == 1:
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={
                            "route": "delegate_subflow",
                            "target_subflow_id": "qual_step",
                            "priority": 7,
                            "rationale": "委派资质初审子状态机收集车辆信息",
                        },
                    )
                ]
            )
        # Hop 1 after subflow completed
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={
                        "route": "send_wechat_nudge",
                        "wechat_message": "太棒了，初审信息已收齐，接下来请拍一张行驶证照片发给我做预审哦～",
                        "priority": 7,
                        "rationale": "资质初审已完成，推进至行驶证预审引导",
                    },
                )
            ]
        )

    async def subflow_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={"car_age": 3, "city": "杭州"},
                )
            ]
        )

    now = datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc)
    steward = StewardAgent(
        model=FunctionModel(steward_fn),
        subflow_model=FunctionModel(subflow_fn),
        registry=reg,
    )
    res = await steward.handle_cognitive_batch(
        {
            "tenant_id": "default",
            "entity_id": "ent_st_sf",
            "events": [
                {
                    "event_id": "wx_1",
                    "category": "wechat",
                    "actor_type": "customer",
                    "facts": {"text": "我的车开了3年，在杭州"},
                }
            ],
        },
        now=now,
    )

    assert steward_calls == 2
    assert res["route"] == "delegate_subflow"
    assert res["subflow_result"]["status"] == "completed"
    assert res["subflow_result"]["conclusion"] == {"car_age": 3, "city": "杭州"}
    assert res["follow_up_decision"]["route"] == "send_wechat_nudge"

    mem = await EntityMemoryState.get(tenant_id="default", entity_id="ent_st_sf")
    assert mem.lifecycle["completed_subflows"]["qual_step"] == {
        "car_age": 3,
        "city": "杭州",
    }
    out_rows = await ActionOutbox.filter(tenant_id="default", entity_id="ent_st_sf").order_by("id").all()
    assert len(out_rows) == 2
    assert out_rows[0].payload["text"] == "初审资料已登记完毕！"
    assert "行驶证照片" in out_rows[1].payload["text"]


async def test_steward_delegates_voice_event_to_call_planner_via_worker_end_to_end():
    """Full end-to-end: ingest_event(voice) -> RsiInboxWorker(steward) -> Steward -> CallPlanner -> Outbox."""
    await FollowupPolicy.create(
        tenant_id="default",
        policy_id="default",
        config={
            "city_caller_map": {"上海": "021-60008888"},
        },
    )

    # Deterministic future daytime window so wall-clock test hour never flakes
    future_now = datetime(2099, 1, 1, 6, 0, tzinfo=timezone.utc)  # 14:00 CST

    await ingest_event(
        RSIEventEnvelope(
            tenant_id="default",
            event_id="ev_e2e_voice_1",
            entity_id="ent_e2e_1",
            event_type="voice.call_unanswered",
            category="voice",
            occurred_at=datetime(2099, 1, 1, 5, 55, tzinfo=timezone.utc),
            summary="客户说正在开车，下午4点半再打",
            facts={
                "sip_code": 486,
                "hangup_cause": "busy",
                "city": "上海",
                "phone_number": "13900002222",
            },
        )
    )

    steward_model = FunctionModel(
        lambda msgs, info: ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={
                        "route": "delegate_call_planner",
                        "rationale": "呼损且有口头改约线索，转交CallPlanner动态排期",
                    },
                )
            ]
        )
    )
    planner_model = FunctionModel(
        lambda msgs, info: ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={
                        "decision": "schedule_call",
                        "scheduled_at_iso": "2099-01-01T16:30:00+08:00",
                        "priority": 8,
                        "opening_script": "您好，刚看您开车不便，四点半按时回电确认车贷额度～",
                        "send_pre_call_sms": True,
                        "verbal_reschedule_detected": True,
                        "verbal_reschedule_quote": "下午4点半再打",
                    },
                )
            ]
        )
    )

    steward = StewardAgent(
        model=steward_model,
        call_planner=CallPlannerAgent(model=planner_model),
        dispatch_actions=False,
    )
    worker = RsiInboxWorker(steward=steward, clock=lambda: future_now)

    ran = await worker.run_once()
    assert ran is True

    task = await RsiTask.get(tenant_id="default", entity_id="ent_e2e_1", task_type="cognitive_wakeup")
    assert task.status == "succeeded"
    assert task.result["output"]["route"] == "delegate_call_planner"

    rows = await ActionOutbox.filter(tenant_id="default", entity_id="ent_e2e_1").order_by("scheduled_at").all()
    assert len(rows) == 2
    assert rows[0].channel == "sms"
    assert rows[0].scheduled_at == datetime(2099, 1, 1, 8, 25, tzinfo=timezone.utc)
    assert rows[1].channel == "voice"
    assert rows[1].scheduled_at == datetime(2099, 1, 1, 8, 30, tzinfo=timezone.utc)
    assert rows[1].payload["city"] == "上海"


async def test_steward_and_call_planner_respect_outbox_guardrails():
    """Steward/CallPlanner cannot bypass forbidden-word blocks or quiet-hour window snapping."""
    await _create_entity("ent_guard_1")

    # 1) Steward proposes forbidden phrase "保证放款" -> Outbox guardrail blocks it!
    day_now = datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc)  # 14:00 CST
    steward = StewardAgent(
        model=FunctionModel(
            lambda msgs, info: ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={
                            "route": "send_wechat_nudge",
                            "wechat_message": "您好，我们这边保证放款，今天就能拿钱！",
                        },
                    )
                ]
            )
        )
    )
    res = await steward.handle_cognitive_batch(
        {"tenant_id": "default", "entity_id": "ent_guard_1", "events": []},
        now=day_now,
    )
    assert res["outbox_receipts"][0]["verdict"] == "blocked"
    assert "forbidden_word" in res["outbox_receipts"][0]["verdict_rule"]
    blocked_out = await ActionOutbox.get(tenant_id="default", entity_id="ent_guard_1")
    assert blocked_out.status == "guardrail_blocked"

    # 2) Outside quiet hours (22:30 CST) immediate request snaps cleanly to next morning 09:30 CST
    await _create_entity("ent_guard_2")
    night_now = datetime(2026, 11, 23, 14, 30, tzinfo=timezone.utc)  # 22:30 CST
    planner = CallPlannerAgent(
        model=FunctionModel(
            lambda msgs, info: ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={
                            "decision": "schedule_call",
                            "delay_minutes": 0,
                            "opening_script": "合规外呼开场白",
                        },
                    )
                ]
            )
        )
    )
    plan_res = await planner.plan_followup_call(
        "default",
        "ent_guard_2",
        now=night_now,
        submit_to_outbox=True,
        dispatch_actions=False,
    )
    # 2026-11-24 09:30 CST (01:30 UTC) — voice scheduled_at >= 09:30 CST (01:30 UTC)
    expected_morning_utc = datetime(2026, 11, 24, 1, 30, tzinfo=timezone.utc)
    assert plan_res.resolved_scheduled_at == expected_morning_utc
    voice_out = await ActionOutbox.get(tenant_id="default", entity_id="ent_guard_2", channel="voice")
    assert voice_out.status == "scheduled"
    assert voice_out.scheduled_at == expected_morning_utc
