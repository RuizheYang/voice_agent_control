from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import httpx
import pytest
import pytest_asyncio
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from tortoise import Tortoise

from app.rsi.adapters import AdapterRegistry, CrmPassiveAdapter, VoiceChannelAdapter
from app.rsi.adapters.base import ack_action_result, query_action_results
from app.rsi.adapters.voice.gateway import BaseVoiceGateway, VoiceGatewayResult
from app.rsi.call_planner import CallPlannerAgent
from app.rsi.memory_reducer import ingest_event
from app.rsi.models import (
    ActionOutbox,
    EntityMemoryState,
    FollowupEntity,
    RsiTask,
    RsiTenantAuth,
)
from app.rsi.schemas import RSIEventEnvelope
from app.rsi.steward import StewardAgent
from app.rsi.subflows import GlobalQATopicSpec, SubFlowRegistry, SubFlowRuntime
from app.rsi.subflows.vehicle_license_precheck import (
    Qwen3VlOcrClient,
    VehicleLicensePrecheckHandler,
    calculate_car_age_years,
    ensure_auto_finance_policy,
    generate_precheck_code,
    load_auto_finance_plugin,
    normalize_and_validate_vin,
)
from app.rsi.worker import RsiInboxWorker

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def setup_rsi_step6_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["app.rsi.models"]},
    )
    await Tortoise.generate_schemas()
    yield
    await Tortoise.close_connections()


class _RecordingVoiceGateway(BaseVoiceGateway):
    mode = "recording_af"

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    async def submit_call(self, request_ctx: Dict[str, Any]) -> VoiceGatewayResult:
        self.calls.append(dict(request_ctx))
        return VoiceGatewayResult(
            status="accepted",
            code="accepted",
            provider_call_id=f"af_call_{request_ctx.get('call_id')}",
            dispatch_payload=dict(request_ctx.get("payload") or {}),
        )


async def test_vin_validation_car_age_and_qwen3_vl_ocr_client():
    """Validate ISO 3779 VIN rules, car-age calculation, and Qwen3-VL-32B OCR client."""
    # Valid VIN with spaces/lowercase normalizes cleanly
    ok, clean, reason = normalize_and_validate_vin("lsv-123456 78901234")
    assert ok is True
    assert clean == "LSV12345678901234"
    assert reason == "ok"

    # Rejects wrong length and forbidden letters I, O, Q
    assert normalize_and_validate_vin("LSV12345")[0] is False
    ok_ioq, _, r_ioq = normalize_and_validate_vin("LSV12345I78901234")
    assert ok_ioq is False and "forbidden" in r_ioq

    # Car age calculation supports ISO & Chinese date formats
    ref = date(2026, 11, 24)
    assert calculate_car_age_years("2023-05-24", reference_date=ref) == pytest.approx(3.5, abs=0.05)
    assert calculate_car_age_years("2021年11月24日", reference_date=ref) == pytest.approx(5.0, abs=0.02)

    code1 = generate_precheck_code("LSV12345678901234", "ent_1")
    code2 = generate_precheck_code("LSV12345678901234", "ent_1")
    assert code1 == code2 and code1.startswith("AFPC-901234-")

    # Qwen3-VL-32B HTTP MockTransport (including <think> stripping)
    captured: List[Dict[str, Any]] = []

    def qwen_mock(request: httpx.Request) -> httpx.Response:
        captured.append(
            {
                "url": str(request.url),
                "auth": request.headers.get("authorization"),
                "body": json.loads(request.content.decode("utf-8")),
            }
        )
        content_json = json.dumps(
            {
                "vin": "LSV12345678901234",
                "plate_no": "浙A99881",
                "owner_name": "张三",
                "brand_model": "大众帕萨特",
                "registration_date": "2023-06-01",
                "use_character": "非营运",
                "confidence": 0.98,
            },
            ensure_ascii=False,
        )
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": f"<think>extract vin from crop</think>```json\n{content_json}\n```"
                        }
                    }
                ]
            },
        )

    client = Qwen3VlOcrClient(
        base_url="https://vlm.internal/v1",
        api_key="vlm_test_key",
        model_name="Qwen3-VL-32B",
        http_transport=httpx.MockTransport(qwen_mock),
    )
    ocr = await client.extract_vehicle_license("https://cdn.example/vl_front.jpg")
    assert len(captured) == 1
    assert captured[0]["url"] == "https://vlm.internal/v1/chat/completions"
    assert captured[0]["auth"] == "Bearer vlm_test_key"
    assert captured[0]["body"]["model"] == "Qwen3-VL-32B"
    assert ocr.vin == "LSV12345678901234"
    assert ocr.plate_no == "浙A99881"
    assert ocr.registration_date == "2023-06-01"
    assert ocr.provider == "Qwen3-VL-32B+ocr"


async def test_vehicle_license_precheck_step_locking_age_rejection_and_qa_jump_back():
    """Lock node on bad VIN, support global QA jump-back, and reject over-age cars deterministically."""
    await FollowupEntity.create(
        tenant_id="default", entity_id="ent_vl_lock", policy_id="default", status="active"
    )
    reg = SubFlowRegistry()
    handler = VehicleLicensePrecheckHandler(
        max_car_age_years=10.0,
        reference_date_fn=lambda: date(2026, 11, 24),
        qa_topics=[
            GlobalQATopicSpec(
                topic_id="pledge_certificate",
                keywords=["押证", "押车"],
                answer_template="仅押车辆登记证（绿本），不押车。",
                resume_prompt="我们继续行驶证核验～",
            )
        ],
    )
    reg.register(handler)
    rt = SubFlowRuntime(registry=reg)

    # 1) Start with 0-LLM fixed opening
    r0 = await rt.start_subflow("default", "ent_vl_lock", "auto_vehicle_license_precheck")
    assert r0["used_fixed_opening"] is True and r0["used_llm"] is False

    # 2) Bad short VIN locks on collect_license_image
    r1 = await rt.advance_entity_subflow(
        "default",
        "ent_vl_lock",
        incoming_text="VIN是 LSV12345 注册日期 2014-01-01",
        dispatch_actions=False,
    )
    assert r1["status"] == "in_progress"
    assert r1["node_id"] == "collect_license_image"
    assert "格式有误" in r1["reply_text"]

    # 3) Mid-step off-topic QA jumps to Global QA and restores collect_license_image
    r2 = await rt.advance_entity_subflow(
        "default",
        "ent_vl_lock",
        incoming_text="请问办理需要押车吗？",
        dispatch_actions=False,
    )
    assert r2["status"] == "qa_answered"
    assert r2["node_id"] == "collect_license_image"
    assert "不押车" in r2["reply_text"]
    assert r2["qa_jump_back"]["return_node_id"] == "collect_license_image"

    # 4) Valid 17-char VIN + >10yr registration date -> completes with precheck_passed=False
    r3 = await rt.advance_entity_subflow(
        "default",
        "ent_vl_lock",
        incoming_text="完整VIN是 LSV12345678901234，注册日期 2014-01-01",
        dispatch_actions=False,
    )
    assert r3["status"] == "completed"
    assert r3["conclusion"]["precheck_passed"] is False
    assert r3["conclusion"]["reject_reason"] == "car_age_exceeds_limit"
    assert r3["conclusion"]["car_age_years"] > 10.0


async def test_auto_finance_yaml_schema_and_mcp_spec_integrity():
    """Load auto_finance_rsi.yaml via plugin loader and verify subflows, QA topics & MCP proxy."""
    await RsiTenantAuth.create(
        tenant_id="t_af",
        app_id="auto_finance_mcp_key",
        app_secret="mcp_secret_xyz",
        name="Auto Finance MCP",
        is_active=True,
    )
    reg = SubFlowRegistry()
    meta = load_auto_finance_plugin(registry=reg)

    assert meta["domain_id"] == "auto_finance"
    assert meta["policy_id"] == "auto_finance_v1"
    assert set(meta["registered_subflow_ids"]) == {
        "auto_qual_precheck",
        "auto_vehicle_license_precheck",
        "auto_vehicle_submission",
        "auto_detailed_appraisal",
    }
    assert len(meta["global_qa_topics"]) == 4

    for sid in meta["registered_subflow_ids"]:
        h = reg.get(sid)
        assert len(h.get_qa_topics()) == 4

    assert len(meta["mcp_servers"]) == 1
    mcp_headers = await meta["mcp_servers"][0].build_resolved_headers(tenant_id="t_af")
    assert mcp_headers["Authorization"] == "Bearer mcp_secret_xyz"


async def test_auto_finance_full_lifecycle_e2e_zero_kernel_changes():
    """End-to-end Auto Finance 7-stage customer lifecycle without modifying any kernel module."""
    tenant_id = "t_auto_fin"
    entity_id = "ent_af_e2e"

    # Register API secret for quote HTTP tool inside CredentialProxy (RsiTenantAuth)
    await RsiTenantAuth.create(
        tenant_id=tenant_id,
        app_id="auto_finance_api_key",
        app_secret="af_secret_key_888",
        name="Auto Finance Quote API",
        is_active=True,
    )

    quote_http_requests: List[Dict[str, Any]] = []

    def combined_http_mock(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        body = json.loads(request.content.decode("utf-8")) if request.content else {}
        if "/chat/completions" in url:
            ocr_json = json.dumps(
                {
                    "vin": "LSV12345678901234",
                    "plate_no": "浙A99881",
                    "owner_name": "李明",
                    "brand_model": "宝马325Li",
                    "registration_date": "2023-06-01",
                    "use_character": "非营运",
                    "confidence": 0.99,
                },
                ensure_ascii=False,
            )
            return httpx.Response(
                200,
                json={"choices": [{"message": {"content": ocr_json}}]},
            )
        if "/v1/quote/estimate" in url:
            quote_http_requests.append(
                {
                    "url": url,
                    "auth": request.headers.get("authorization"),
                    "body": body,
                }
            )
            return httpx.Response(
                200,
                json={"estimated_limit_wan": 16, "annual_rate_pct": 5.88},
            )
        return httpx.Response(404, json={"error": "not_found"})

    transport = httpx.MockTransport(combined_http_mock)
    vision_client = Qwen3VlOcrClient(
        base_url="https://vlm.autofinance.internal/v1",
        api_key="vlm_key_123",
        http_transport=transport,
    )

    af_registry = SubFlowRegistry()
    plugin_meta = load_auto_finance_plugin(
        registry=af_registry,
        vision_client=vision_client,
        http_transport=transport,
        reference_date_fn=lambda: date(2026, 11, 24),
    )
    await ensure_auto_finance_policy(tenant_id=tenant_id, plugin_meta=plugin_meta)

    gw = _RecordingVoiceGateway()
    adapter_reg = AdapterRegistry()
    adapter_reg.register(CrmPassiveAdapter())
    adapter_reg.register(VoiceChannelAdapter(gateway=gw))

    # Contextual FunctionModel for Top Steward
    async def steward_brain(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        user_prompts = [
            str(p.content)
            for m in messages
            for p in getattr(m, "parts", [])
            if getattr(p, "part_kind", "") == "user-prompt"
        ]
        ctx_data = json.loads(user_prompts[-1])
        task_type = ctx_data.get("task_type")
        events = ctx_data.get("events") or []
        active_sf = ctx_data.get("active_subflow_state") or {}

        # Hand-back promotion after subflow completion
        if task_type == "subflow_completed" and events:
            done_sid = (events[0].get("facts") or {}).get("completed_subflow_id")
            if done_sid == "auto_qual_precheck":
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name=info.output_tools[0].name,
                            args={
                                "route": "delegate_subflow",
                                "target_subflow_id": "auto_vehicle_license_precheck",
                                "priority": 8,
                                "rationale": "初审完成，推进至多模态行驶证预审",
                            },
                        )
                    ]
                )
            if done_sid == "auto_vehicle_license_precheck":
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name=info.output_tools[0].name,
                            args={
                                "route": "delegate_subflow",
                                "target_subflow_id": "auto_vehicle_submission",
                                "priority": 8,
                                "rationale": "行驶证预审通过，进入车辆提报试算",
                            },
                        )
                    ]
                )
            if done_sid == "auto_vehicle_submission":
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name=info.output_tools[0].name,
                            args={
                                "route": "delegate_subflow",
                                "target_subflow_id": "auto_detailed_appraisal",
                                "priority": 8,
                                "rationale": "提报完成，预约线下精评",
                            },
                        )
                    ]
                )
            if done_sid == "auto_detailed_appraisal":
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name=info.output_tools[0].name,
                            args={
                                "route": "send_wechat_nudge",
                                "wechat_message": "太棒了，全套资料与精评档期均已确认锁定，期待周末在门店为您提供服务！",
                                "priority": 7,
                                "trigger_memory_dream": True,
                                "rationale": "精评预约完毕，发送最终确认并触发L3沉淀",
                            },
                        )
                    ]
                )

        categories = {e.get("category") for e in events}
        etypes = {e.get("event_type") for e in events}
        if "voice" in categories or "content_engagement" in categories:
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={"route": "delegate_call_planner", "priority": 8},
                    )
                ]
            )

        if "wechat.friend_added" in etypes:
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={
                            "route": "delegate_subflow",
                            "target_subflow_id": "auto_qual_precheck",
                            "priority": 8,
                            "rationale": "客户已加微，切入资质初审子状态机",
                        },
                    )
                ]
            )

        if active_sf.get("subflow_id") and active_sf.get("status") == "active":
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={
                            "route": "delegate_subflow",
                            "target_subflow_id": active_sf["subflow_id"],
                            "priority": 8,
                        },
                    )
                ]
            )

        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={"route": "noop"},
                )
            ]
        )

    # Contextual FunctionModel for CallPlanner
    async def planner_brain(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        user_prompts = [
            str(p.content)
            for m in messages
            for p in getattr(m, "parts", [])
            if getattr(p, "part_kind", "") == "user-prompt"
        ]
        ctx = json.loads(user_prompts[-1])
        sig = ctx.get("sip_signals") or {}
        if sig.get("active_engagement_boost"):
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={
                            "decision": "schedule_call",
                            "priority": 9,
                            "opening_script": "您好，看您刚在App试算了车辆方案，马上回电为您解答！",
                        },
                    )
                ]
            )
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={
                        "decision": "schedule_call",
                        "scheduled_at_iso": "2099-01-02T16:30:00+08:00",
                        "priority": 8,
                        "opening_script": "李先生您好，按您昨天约的时间回电跟进车贷初审～",
                        "send_pre_call_sms": True,
                        "verbal_reschedule_detected": True,
                        "verbal_reschedule_quote": "明天下午4点半再打",
                    },
                )
            ]
        )

    # Contextual FunctionModel for YAML SubFlows (with HTTP quote tool support)
    async def yaml_subflow_brain(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        user_prompts = [
            str(p.content)
            for m in messages
            for p in getattr(m, "parts", [])
            if getattr(p, "part_kind", "") == "user-prompt"
        ]
        latest_prompt = user_prompts[-1] if user_prompts else ""

        tool_names = {t.name for t in info.function_tools}
        if "calculate_auto_loan_quote" in tool_names:
            if len(messages) == 1:
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name="calculate_auto_loan_quote",
                            args={
                                "vin": "LSV12345678901234",
                                "car_age_years": 3.48,
                                "mileage_wan_km": 4.5,
                                "loan_term_months": 36,
                            },
                        )
                    ]
                )
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={
                            "mileage_wan_km": 4.5,
                            "loan_term_months": 36,
                            "estimated_limit_wan": 16,
                        },
                    )
                ]
            )

        schema_props = (info.output_tools[0].parameters_json_schema or {}).get("properties", {})
        if "desired_amount_wan" in schema_props:
            if "18" in latest_prompt or "杭州" in latest_prompt:
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name=info.output_tools[0].name,
                            args={"city": "杭州", "is_owner": True, "desired_amount_wan": 18},
                        )
                    ]
                )
            return ModelResponse(
                parts=[
                    TextPart("您好！很高兴微信上为您服务～请先告诉我车辆在哪个城市、是否本人名下、期望贷款多少万？")
                ]
            )

        if "门店" in latest_prompt or "store" in latest_prompt or "周六" in latest_prompt:
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={
                            "appraisal_date": "2099-01-06",
                            "appraisal_mode": "store",
                            "appraisal_city": "杭州",
                        },
                    )
                ]
            )
        return ModelResponse(
            parts=[TextPart("请问您方便哪天精评？更倾向到店（store）还是上门（onsite）？")]
        )

    dream_model = FunctionModel(
        lambda msgs, info: ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={
                        "customer_profile_notes": {"city": "杭州", "vehicle": "宝马325Li"},
                        "rolling_digest": "客户已完成资质初审、行驶证预审、提报试算及门店精评预约。",
                        "active_guidance": "周末门店精评按时提醒。",
                    },
                )
            ]
        )
    )

    # Fixed future daytime UTC clock inside 09:00-21:00 CST so CI never hits night window flakiness
    daytime_clock = datetime(2099, 1, 1, 6, 0, tzinfo=timezone.utc)  # 14:00 CST

    steward = StewardAgent(
        model=FunctionModel(steward_brain),
        call_planner=CallPlannerAgent(
            model=FunctionModel(planner_brain), adapter_registry=adapter_reg
        ),
        subflow_runtime=SubFlowRuntime(registry=af_registry),
        subflow_model=FunctionModel(yaml_subflow_brain),
        registry=af_registry,
        adapter_registry=adapter_reg,
        dispatch_actions=True,
    )
    worker = RsiInboxWorker(
        steward=steward,
        dream_model=dream_model,
        clock=lambda: daytime_clock,
    )

    async def _drain_worker() -> int:
        cnt = 0
        while await worker.run_once():
            cnt += 1
        return cnt

    # --- Stage 1: Outbound voice verbal reschedule -> T-5m SMS + Voice Outbox ---
    await FollowupEntity.create(
        tenant_id=tenant_id,
        entity_id=entity_id,
        policy_id="auto_finance_v1",
        status="active",
    )
    await ingest_event(
        RSIEventEnvelope(
            tenant_id=tenant_id,
            event_id="af_ev_1_voice",
            entity_id=entity_id,
            policy_id="auto_finance_v1",
            event_type="voice.call_ended",
            category="voice",
            occurred_at=daytime_clock,
            summary="客户说正在忙，明天下午4点半再打",
            facts={
                "outcome": "connected",
                "answered": True,
                "duration_sec": 22,
                "city": "杭州",
                "phone_number": "13800138999",
            },
        )
    )
    assert await _drain_worker() >= 1
    out_stage1 = await ActionOutbox.filter(tenant_id=tenant_id, entity_id=entity_id).order_by("scheduled_at").all()
    assert len(out_stage1) == 2
    assert out_stage1[0].channel == "sms"
    assert out_stage1[1].channel == "voice"
    assert out_stage1[0].scheduled_at == datetime(2099, 1, 2, 8, 25, tzinfo=timezone.utc)
    assert out_stage1[1].scheduled_at == datetime(2099, 1, 2, 8, 30, tzinfo=timezone.utc)

    # --- Stage 2: App high-intent active engagement -> priority>=9 immediate callback + city line ---
    await ingest_event(
        RSIEventEnvelope(
            tenant_id=tenant_id,
            event_id="af_ev_2_app",
            entity_id=entity_id,
            policy_id="auto_finance_v1",
            event_type="app.loan_simulated",
            category="content_engagement",
            occurred_at=daytime_clock,
            summary="客户在App计算18万车贷方案",
            facts={"page": "loan_calculator", "boost": True, "city": "杭州", "phone_number": "13800138999"},
        )
    )
    assert await _drain_worker() >= 1
    assert len(gw.calls) == 1
    assert gw.calls[0]["city_caller"]["caller_number"] == "0571-88886666"

    # --- Stage 3: WeChat friend added -> wakes Steward -> starts auto_qual_precheck (0-LLM opening) ---
    await ingest_event(
        RSIEventEnvelope(
            tenant_id=tenant_id,
            event_id="af_ev_3_wx_add",
            entity_id=entity_id,
            policy_id="auto_finance_v1",
            event_type="wechat.friend_added",
            category="wechat",
            actor_type="system",
            occurred_at=daytime_clock,
            summary="客户已通过企微好友",
            facts={"wechat_added": True},
        )
    )
    assert await _drain_worker() >= 1
    mem = await EntityMemoryState.get(tenant_id=tenant_id, entity_id=entity_id)
    assert mem.lifecycle["active_subflow_state"]["subflow_id"] == "auto_qual_precheck"

    # --- Stage 4: Complete auto_qual_precheck -> Steward promotes to auto_vehicle_license_precheck ---
    await ingest_event(
        RSIEventEnvelope(
            tenant_id=tenant_id,
            event_id="af_ev_4_qual",
            entity_id=entity_id,
            policy_id="auto_finance_v1",
            event_type="wechat.message",
            category="wechat",
            actor_type="customer",
            occurred_at=daytime_clock,
            facts={"text": "车在杭州，是我本人名下的，大概想贷18万"},
        )
    )
    assert await _drain_worker() >= 1
    mem = await EntityMemoryState.get(tenant_id=tenant_id, entity_id=entity_id)
    assert mem.lifecycle["completed_subflows"]["auto_qual_precheck"] == {
        "city": "杭州",
        "is_owner": True,
        "desired_amount_wan": 18,
    }
    assert mem.lifecycle["active_subflow_state"]["subflow_id"] == "auto_vehicle_license_precheck"

    # --- Stage 5a: Off-topic QA interrupt ("押证/费率") -> jump back to collect_license_image ---
    await ingest_event(
        RSIEventEnvelope(
            tenant_id=tenant_id,
            event_id="af_ev_5a_qa",
            entity_id=entity_id,
            policy_id="auto_finance_v1",
            event_type="wechat.message",
            category="wechat",
            actor_type="customer",
            occurred_at=daytime_clock,
            facts={"text": "请问这个需要押证吗，会把车扣下吗？"},
        )
    )
    assert await _drain_worker() >= 1
    mem = await EntityMemoryState.get(tenant_id=tenant_id, entity_id=entity_id)
    snap5a = mem.lifecycle["active_subflow_state"]
    assert snap5a["subflow_id"] == "auto_vehicle_license_precheck"
    assert snap5a["current_node_id"] == "collect_license_image"
    assert len(snap5a["extra"]["qa_history"]) == 1

    # --- Stage 5b: Customer sends license photo -> Qwen3-VL OCR -> VIN & precheck_code -> promotes ---
    await ingest_event(
        RSIEventEnvelope(
            tenant_id=tenant_id,
            event_id="af_ev_5b_photo",
            entity_id=entity_id,
            policy_id="auto_finance_v1",
            event_type="wechat.message",
            category="wechat",
            actor_type="customer",
            occurred_at=daytime_clock,
            facts={
                "text": "行驶证照片发您了",
                "media_urls": ["https://cdn.example/license_e2e.jpg"],
            },
        )
    )
    assert await _drain_worker() >= 1
    mem = await EntityMemoryState.get(tenant_id=tenant_id, entity_id=entity_id)
    vl_conclusion = mem.lifecycle["completed_subflows"]["auto_vehicle_license_precheck"]
    assert vl_conclusion["precheck_passed"] is True
    assert vl_conclusion["vin"] == "LSV12345678901234"
    assert vl_conclusion["precheck_code"].startswith("AFPC-901234-")
    assert vl_conclusion["car_age_years"] < 10.0
    assert mem.lifecycle["active_subflow_state"]["subflow_id"] == "auto_vehicle_submission"

    # --- Stage 6a: Vehicle submission calls calculate_auto_loan_quote HTTP tool -> promotes ---
    await ingest_event(
        RSIEventEnvelope(
            tenant_id=tenant_id,
            event_id="af_ev_6a_submit",
            entity_id=entity_id,
            policy_id="auto_finance_v1",
            event_type="wechat.message",
            category="wechat",
            actor_type="customer",
            occurred_at=daytime_clock,
            facts={"text": "表显开了4.5万公里，想分36期"},
        )
    )
    assert await _drain_worker() >= 1
    assert len(quote_http_requests) == 1
    assert quote_http_requests[0]["auth"] == "Bearer af_secret_key_888"
    mem = await EntityMemoryState.get(tenant_id=tenant_id, entity_id=entity_id)
    assert mem.lifecycle["completed_subflows"]["auto_vehicle_submission"] == {
        "mileage_wan_km": 4.5,
        "loan_term_months": 36,
        "estimated_limit_wan": 16,
    }
    assert mem.lifecycle["active_subflow_state"]["subflow_id"] == "auto_detailed_appraisal"

    # --- Stage 6b: Detailed appraisal confirmation + L3 memory dream ---
    await ingest_event(
        RSIEventEnvelope(
            tenant_id=tenant_id,
            event_id="af_ev_6b_appraisal",
            entity_id=entity_id,
            policy_id="auto_finance_v1",
            event_type="wechat.message",
            category="wechat",
            actor_type="customer",
            occurred_at=daytime_clock,
            facts={"text": "那约这周六去杭州门店精评"},
        )
    )
    assert await _drain_worker() >= 1

    mem = await EntityMemoryState.get(tenant_id=tenant_id, entity_id=entity_id)
    assert set(mem.lifecycle["completed_subflows"].keys()) == {
        "auto_qual_precheck",
        "auto_vehicle_license_precheck",
        "auto_vehicle_submission",
        "auto_detailed_appraisal",
    }

    # --- Stage 7: Upstream CRM queries and acknowledges ActionExecutionResult trail ---
    crm_results = await query_action_results(tenant_id=tenant_id, entity_id=entity_id, limit=50)
    assert crm_results.total >= 6
    first_id = crm_results.items[0].id
    acked = await ack_action_result(tenant_id=tenant_id, result_id=first_id)
    assert acked is not None and acked.crm_acked_at is not None
