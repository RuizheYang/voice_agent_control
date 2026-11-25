from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import httpx
import pytest
import pytest_asyncio
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from tortoise import Tortoise

from app.rsi.memory_dreamer import RsiMemoryDreamer, enqueue_memory_dream_task
from app.rsi.memory_reducer import ingest_event
from app.rsi.models import (
    ActionExecutionResult,
    ActionOutbox,
    EntityEventLog,
    EntityMemoryState,
    EntityWorkingMemory,
    FollowupEntity,
    RsiTask,
    RsiTenantAuth,
)
from app.rsi.schemas import ActionProposal, RSIEventEnvelope
from app.rsi.subflows import (
    BaseSubFlowHandler,
    GlobalQATopicSpec,
    McpToolServerSpec,
    RsiCredentialProxy,
    StepResult,
    SubFlowRegistry,
    SubFlowRunContext,
    SubFlowRuntime,
    SubFlowStateSnapshot,
    YamlSubFlowHandler,
    build_compact_context_index,
    sanitize_llm_output,
    trim_node_history,
)
from app.rsi.worker import RsiInboxWorker

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def setup_rsi_step4_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["app.rsi.models"]},
    )
    await Tortoise.generate_schemas()
    yield
    await Tortoise.close_connections()


async def _create_entity(entity_id: str, *, tenant_id: str = "default") -> FollowupEntity:
    return await FollowupEntity.create(
        tenant_id=tenant_id,
        entity_id=entity_id,
        policy_id="default",
        status="active",
    )


async def test_sanitize_llm_output_strips_thoughts_and_unclosed_tags():
    raw = (
        "<think>internal step 1\ncheck balance</think>\n\n"
        "您好，请问您的爱车是哪一年上牌的呢？"
        "<REASONING>hidden reasoning</reasoning>"
        "<internal>secret tag"
    )
    cleaned = sanitize_llm_output(raw)
    assert cleaned == "您好，请问您的爱车是哪一年上牌的呢？"

    # Also verifies multiple consecutive newlines collapse without losing formatting
    raw2 = "<thinking>abc</thinking>第一行\n\n\n\n第二行"
    assert sanitize_llm_output(raw2) == "第一行\n\n第二行"


async def test_pydantic_ai_union_output_locks_step_until_schema_complete():
    """Incomplete turns returning `str` keep the node locked; valid schema output advances."""
    yaml_cfg = """
subflow_id: qual_check
description: Generic 2-step intake flow
initial_node_id: ask_info
nodes:
  - node_id: ask_info
    prompt: Ask the user for car_age_years (int) and city (str).
    output_fields:
      - name: car_age_years
        type: int
        required: true
      - name: city
        type: str
        required: true
    next_node_id: confirm_info
  - node_id: confirm_info
    prompt: Confirm user appointment slot.
    fixed_opening: 信息已收到！请问您方便周几办理手续？
    output_fields:
      - name: preferred_day
        type: str
        required: true
    completion_reply: 预约已记录！
"""
    reg = SubFlowRegistry()
    reg.register_yaml(yaml_cfg)
    rt = SubFlowRuntime(registry=reg)
    await _create_entity("ent_lock_1")

    call_count = 0

    async def lock_model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        nonlocal call_count
        call_count += 1
        assert len(info.output_tools) >= 1
        # Turn 1: only car_age mentioned, city missing -> model asks follow-up text with <think>
        if call_count == 1:
            return ModelResponse(
                parts=[
                    TextPart("<think>missing city</think>收到车龄，请问车辆目前在哪个城市？")
                ]
            )
        # Turn 2: both slots complete -> model invokes output tool
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={"car_age_years": 3, "city": "杭州"},
                )
            ]
        )

    fmodel = FunctionModel(lock_model_fn)

    # Turn 1: locked on ask_info
    res1 = await rt.advance_entity_subflow(
        "default",
        "ent_lock_1",
        subflow_id="qual_check",
        incoming_text="我的车开了3年",
        model=fmodel,
        dispatch_actions=False,
    )
    assert res1["status"] == "in_progress"
    assert res1["node_id"] == "ask_info"
    assert res1["reply_text"] == "收到车龄，请问车辆目前在哪个城市？"
    assert res1["collected_slots"] == {}

    # Turn 2: slots satisfied -> transitions to confirm_info and emits fixed opening
    res2 = await rt.advance_entity_subflow(
        "default",
        "ent_lock_1",
        incoming_text="在杭州",
        model=fmodel,
        dispatch_actions=False,
    )
    assert res2["status"] == "in_progress"
    assert res2["previous_node_id"] == "ask_info"
    assert res2["node_id"] == "confirm_info"
    assert res2["collected_slots"] == {"car_age_years": 3, "city": "杭州"}
    assert "信息已收到！请问您方便周几办理手续？" in res2["reply_text"]
    assert res2["used_fixed_opening"] is True


async def test_global_qa_call_stack_interrupt_and_jump_back():
    """Customer off-topic question pauses step logic, answers via QA stack, and resumes origin node."""
    yaml_cfg = """
subflow_id: license_check
initial_node_id: ask_plate
qa_topics:
  - topic_id: remote_plate_policy
    keywords: ["异地", "外地牌"]
    answer_template: 支持全国大多数城市异地牌照办理，不限制户籍所在地。
    resume_prompt: 您放心，我们继续刚才那一步～
nodes:
  ask_plate:
    prompt: Collect plate_no
    resume_hint: 请发一下您的车牌号或行驶证照片即可。
    output_fields:
      - name: plate_no
        type: str
        required: true
"""
    reg = SubFlowRegistry()
    reg.register_yaml(yaml_cfg)
    rt = SubFlowRuntime(registry=reg)
    await _create_entity("ent_qa_1")

    llm_calls = 0

    async def plate_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        nonlocal llm_calls
        llm_calls += 1
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={"plate_no": "浙A88888"},
                )
            ]
        )

    fmodel = FunctionModel(plate_fn)

    # First enter ask_plate with an initial message OR via start_subflow(emit_initial_opening=False)
    await rt.start_subflow(
        "default",
        "ent_qa_1",
        "license_check",
        emit_initial_opening=False,
    )

    # Interrupt with off-topic question -> should trigger QA jump-back without consuming LLM calls
    qa_res = await rt.advance_entity_subflow(
        "default",
        "ent_qa_1",
        incoming_text="请问外地牌可以办吗？",
        model=fmodel,
        dispatch_actions=False,
    )
    assert llm_calls == 0
    assert qa_res["status"] == "qa_answered"
    assert qa_res["node_id"] == "ask_plate"
    assert qa_res["used_llm"] is False
    assert "支持全国大多数城市异地牌照办理" in qa_res["reply_text"]
    assert "我们继续刚才那一步" in qa_res["reply_text"]
    assert qa_res["qa_jump_back"]["return_node_id"] == "ask_plate"
    assert qa_res["qa_jump_back"]["stack_depth_during_qa"] == 1
    assert qa_res["snapshot"]["qa_call_stack"] == []
    assert len(qa_res["snapshot"]["extra"]["qa_history"]) == 1

    # Now customer supplies the plate number -> flow resumes origin node and completes!
    done_res = await rt.advance_entity_subflow(
        "default",
        "ent_qa_1",
        incoming_text="浙A88888",
        model=fmodel,
        dispatch_actions=False,
    )
    assert llm_calls == 1
    assert done_res["status"] == "completed"
    assert done_res["conclusion"] == {"plate_no": "浙A88888"}


async def test_fixed_opening_zero_llm_and_node_history_isolation():
    """Initial entry emits fixed opening with 0 LLM calls; Node 2 isolates history from Node 1."""
    yaml_cfg = """
subflow_id: two_node_iso
initial_node_id: step_one
nodes:
  step_one:
    prompt: Collect name
    fixed_opening: 您好！请问怎么称呼您？
    max_history_turns: 4
    output_fields:
      - name: customer_name
        type: str
        required: true
    next_node_id: step_two
  step_two:
    prompt: Collect budget
    fixed_opening: 太好了！请问您的预算大概是多少？
    max_history_turns: 2
    output_fields:
      - name: budget_wan
        type: int
        required: true
    completion_reply: 已记下您的预算需求。
"""
    reg = SubFlowRegistry()
    reg.register_yaml(yaml_cfg)
    rt = SubFlowRuntime(registry=reg)
    await _create_entity("ent_iso_1")

    seen_prompts: List[str] = []

    async def iso_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        user_text = "\n".join(
            str(part.content)
            for msg in messages
            for part in getattr(msg, "parts", [])
            if hasattr(part, "content")
        )
        seen_prompts.append(user_text)
        if len(seen_prompts) == 1:
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=info.output_tools[0].name,
                        args={"customer_name": "张先生"},
                    )
                ]
            )
        if len(seen_prompts) == 2:
            # First turn inside step_two returns clarifying str (should NOT leak step_one's turns)
            return ModelResponse(parts=[TextPart("大概十几万区间都可以，您有具体数字吗？")])
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={"budget_wan": 18},
                )
            ]
        )

    fmodel = FunctionModel(iso_fn)

    # Start subflow -> 0 LLM fixed opening
    start_res = await rt.start_subflow(
        "default",
        "ent_iso_1",
        "two_node_iso",
        model=fmodel,
        emit_initial_opening=True,
    )
    assert start_res["used_fixed_opening"] is True
    assert start_res["used_llm"] is False
    assert start_res["reply_text"] == "您好！请问怎么称呼您？"
    assert len(seen_prompts) == 0

    # Complete step_one -> emits step_two fixed opening immediately
    step1_done = await rt.advance_entity_subflow(
        "default",
        "ent_iso_1",
        incoming_text="我姓张",
        model=fmodel,
        dispatch_actions=False,
    )
    assert step1_done["node_id"] == "step_two"
    assert step1_done["used_fixed_opening"] is True
    assert "请问您的预算大概是多少" in step1_done["reply_text"]

    # Turn in step_two (clarification): verify step_one message "我姓张" is NOT in step_two's node_history
    s2_turn1 = await rt.advance_entity_subflow(
        "default",
        "ent_iso_1",
        incoming_text="还没想太好",
        model=fmodel,
        dispatch_actions=False,
    )
    assert "我姓张" not in seen_prompts[1]
    assert "请问您的预算大概是多少" in seen_prompts[1]

    # Turn 2 in step_two completes and enforces max_history_turns=2 trimming
    s2_turn2 = await rt.advance_entity_subflow(
        "default",
        "ent_iso_1",
        incoming_text="大概18万",
        model=fmodel,
        dispatch_actions=False,
    )
    assert s2_turn2["status"] == "completed"
    assert s2_turn2["conclusion"] == {"customer_name": "张先生", "budget_wan": 18}
    # Directly verify trim_node_history helper bounds lists accurately
    assert len(trim_node_history([{"role": "user", "content": str(i)} for i in range(10)], 3)) == 3


async def test_yaml_subflow_http_tool_credential_proxy_and_mcp_spec():
    """HTTP tools execute via MockTransport with server-side CredentialProxy secret injection."""
    await _create_entity("ent_http_1", tenant_id="t_auto")
    await RsiTenantAuth.create(
        tenant_id="t_auto",
        app_id="vin_lookup_api",
        app_secret="super_secret_token_99",
        name="VIN API",
        is_active=True,
    )

    captured_requests: List[Dict[str, Any]] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(
            {
                "method": request.method,
                "url": str(request.url),
                "headers": dict(request.headers),
                "body": json.loads(request.content.decode("utf-8")) if request.content else {},
            }
        )
        return httpx.Response(200, json={"valid": True, "brand": "Volkswagen", "year": 2022})

    transport = httpx.MockTransport(mock_handler)

    yaml_cfg = """
subflow_id: vin_tool_flow
initial_node_id: check_vin
http_tools:
  - name: query_vin_info
    description: Verify VIN against enterprise registry
    method: POST
    url: https://api.example.internal/v1/vin/check
    auth_ref: vin_lookup_api
    auth_header_name: Authorization
    auth_scheme: Bearer
    params:
      - name: vin
        type: str
        required: true
        description: 17-character VIN
mcp_servers:
  - server_id: crm_mcp
    transport: streamable_http
    url: https://mcp.example.internal/sse
    auth_ref: vin_lookup_api
nodes:
  - node_id: check_vin
    prompt: Verify the VIN with query_vin_info tool, then return structured output.
    tools: ["query_vin_info"]
    output_fields:
      - name: vin
        type: str
        required: true
      - name: brand
        type: str
        required: true
    completion_reply: VIN核验通过。
"""
    handler = YamlSubFlowHandler.from_yaml_str(yaml_cfg, http_transport=transport)
    reg = SubFlowRegistry()
    reg.register(handler)
    rt = SubFlowRuntime(registry=reg)

    # Verify MCP server spec resolves secret cleanly
    mcp_headers = await handler.spec.mcp_servers[0].build_resolved_headers(tenant_id="t_auto")
    assert mcp_headers["Authorization"] == "Bearer super_secret_token_99"

    async def tool_calling_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        # Ensure secret never leaks into tool schemas visible to the LLM
        for ftool in info.function_tools:
            schema_json = json.dumps(ftool.parameters_json_schema)
            assert "super_secret_token_99" not in schema_json
            assert "Authorization" not in schema_json

        # Step 1: call HTTP tool; Step 2 (after tool result message arrives): emit output schema
        if len(messages) == 1:
            return ModelResponse(
                parts=[ToolCallPart(tool_name="query_vin_info", args={"vin": "LSV12345678901234"})]
            )
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={"vin": "LSV12345678901234", "brand": "Volkswagen"},
                )
            ]
        )

    res = await rt.advance_entity_subflow(
        "t_auto",
        "ent_http_1",
        subflow_id="vin_tool_flow",
        incoming_text="这是VIN LSV12345678901234",
        model=FunctionModel(tool_calling_fn),
        dispatch_actions=False,
    )

    assert res["status"] == "completed"
    assert res["conclusion"] == {"vin": "LSV12345678901234", "brand": "Volkswagen"}
    assert len(captured_requests) == 1
    assert captured_requests[0]["url"] == "https://api.example.internal/v1/vin/check"
    assert captured_requests[0]["headers"]["authorization"] == "Bearer super_secret_token_99"
    assert captured_requests[0]["body"] == {"vin": "LSV12345678901234"}


async def test_custom_python_handler_and_outbox_proposal_submission(tmp_path: Path):
    """Custom Python subflows can emit ActionProposals routed cleanly through Outbox Guardrail."""
    await _create_entity("ent_custom_1")

    class PreApprovalPythonFlow(BaseSubFlowHandler):
        subflow_id = "python_precheck"
        description = "Specialized Python subflow emitting an outbox proposal"
        initial_node_id = "evaluate"

        async def step(self, ctx: SubFlowRunContext) -> StepResult:
            return StepResult(
                status="completed",
                reply_text="预审已通过，正在发送结果通知。",
                updated_slots={"approved_limit_wan": 20},
                conclusion={"precheck_passed": True, "approved_limit_wan": 20},
                proposed_actions=[
                    ActionProposal(
                        tenant_id=ctx.tenant_id,
                        entity_id=ctx.entity_id,
                        idempotency_key=f"precheck:{ctx.entity_id}:pass",
                        channel="wechat",
                        action_type="send_message",
                        payload={"text": "恭喜您预审通过，初步额度20万"},
                        policy_id="default",
                        source_agent="subflow:python_precheck",
                    )
                ],
            )

    reg = SubFlowRegistry()
    reg.register(PreApprovalPythonFlow())

    # Also test directory YAML loading
    sample_yaml = tmp_path / "extra_flow.yaml"
    sample_yaml.write_text(
        """
subflow_id: dir_yaml_flow
nodes:
  first:
    prompt: Say hi
""",
        encoding="utf-8",
    )
    loaded_ids = reg.load_yaml_dir(tmp_path)
    assert loaded_ids == ["dir_yaml_flow"]
    assert {s["subflow_id"] for s in reg.list_specs()} == {"python_precheck", "dir_yaml_flow"}

    # Ensure CrmPassiveAdapter exists in default registry used by outbox guardrail (or use dispatch=False)
    rt = SubFlowRuntime(registry=reg)
    res = await rt.advance_entity_subflow(
        "default",
        "ent_custom_1",
        subflow_id="python_precheck",
        incoming_text="申请预审",
        dispatch_actions=True,
    )

    assert res["status"] == "completed"
    assert res["conclusion"] == {"precheck_passed": True, "approved_limit_wan": 20}
    assert len(res["action_receipts"]) == 1
    outbox_rows = await ActionOutbox.filter(tenant_id="default", entity_id="ent_custom_1").all()
    assert len(outbox_rows) == 1
    assert outbox_rows[0].channel == "wechat"
    exec_rows = await ActionExecutionResult.filter(tenant_id="default", entity_id="ent_custom_1").all()
    assert len(exec_rows) == 1


async def test_memory_dreamer_updates_l3_only_without_mutating_l1_or_l2():
    """RsiMemoryDreamer consumes memory_dream tasks via worker and writes strictly to L3."""
    now = datetime(2026, 11, 23, 10, 0, tzinfo=timezone.utc)
    # Ingest two L1 events to populate L1 + L2
    await ingest_event(
        RSIEventEnvelope(
            tenant_id="default",
            event_id="ev_dream_1",
            entity_id="ent_dream_1",
            event_type="call_completed",
            category="voice",
            occurred_at=now,
            summary="白天开会没空接电话，晚上8点后联系，觉得利息偏高",
            facts={"outcome": "connected", "disposition": "callback_requested"},
        )
    )
    await ingest_event(
        RSIEventEnvelope(
            tenant_id="default",
            event_id="ev_dream_2",
            entity_id="ent_dream_1",
            event_type="dnd_requested",
            category="risk_boundary",
            occurred_at=now,
            summary="不要中午打电话",
            facts={"dnd": True},
        )
    )

    l1_before_count = await EntityEventLog.filter(tenant_id="default", entity_id="ent_dream_1").count()
    l2_before = await EntityMemoryState.get(tenant_id="default", entity_id="ent_dream_1")
    hb_before = dict(l2_before.hard_boundaries)
    lc_before = dict(l2_before.lifecycle)
    pv_before = l2_before.projection_version

    async def dream_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name=info.output_tools[0].name,
                    args={
                        "customer_profile_notes": {
                            "occupation_hint": "白天经常开会",
                            "note": "<think>guess</think>关注月供压力",
                        },
                        "rolling_digest": "<think>raw</think>客户白天开会不便接听，关心利率问题并希望晚上联系。",
                        "active_guidance": "<reasoning>hidden</reasoning>晚上20:00后先通过微信发清晰费率测算表，再电话沟通。",
                        "detected_hesitations": ["利率偏高"],
                        "preferred_contact_windows": ["20:00-21:00"],
                    },
                )
            ]
        )

    await enqueue_memory_dream_task("default", "ent_dream_1", run_at=now, notify=False)
    worker = RsiInboxWorker(
        dream_model=FunctionModel(dream_fn),
        clock=lambda: now,
    )
    processed = 0
    while await worker.run_once():
        processed += 1
    assert processed >= 1

    # Verify task succeeded
    task = await RsiTask.get(tenant_id="default", entity_id="ent_dream_1", task_type="memory_dream")
    assert task.status == "succeeded"

    # Verify L3 populated & sanitized
    l3 = await EntityWorkingMemory.get(tenant_id="default", entity_id="ent_dream_1")
    assert l3.customer_profile_notes["occupation_hint"] == "白天经常开会"
    assert l3.customer_profile_notes["note"] == "关注月供压力"
    assert l3.customer_profile_notes["detected_hesitations"] == ["利率偏高"]
    assert l3.customer_profile_notes["preferred_contact_windows"] == ["20:00-21:00"]
    assert isinstance(l3.rolling_digest, list) and len(l3.rolling_digest) == 1
    assert "<think>" not in l3.rolling_digest[0]["digest"]
    assert l3.active_guidance == "晚上20:00后先通过微信发清晰费率测算表，再电话沟通。"
    assert l3.last_dreamed_at is not None

    # Verify L1 and L2 were strictly untouched!
    l1_after_count = await EntityEventLog.filter(tenant_id="default", entity_id="ent_dream_1").count()
    assert l1_after_count == l1_before_count
    l2_after = await EntityMemoryState.get(tenant_id="default", entity_id="ent_dream_1")
    assert l2_after.hard_boundaries == hb_before
    assert l2_after.lifecycle == lc_before
    assert l2_after.projection_version == pv_before


async def test_compact_context_index_combines_l2_and_l3_safely():
    """build_compact_context_index blends L2 + L3 + active subflow cleanly with <think> stripped."""
    snap = SubFlowStateSnapshot(
        subflow_id="qual_check",
        current_node_id="ask_city",
        collected_slots={"car_age_years": 2},
    )
    index = build_compact_context_index(
        l2_state={
            "lifecycle": {"stage": "engaged"},
            "ownership": {"current_role": "ai_steward"},
            "hard_boundaries": {"dnd": True, "blacklisted": False},
            "commitments": [{"title": "今晚回拨"}],
        },
        l3_notes={
            "rolling_digest": [{"digest": "<think>x</think>客户偏好晚上沟通"}],
            "active_guidance": "<internal>hide</internal>重点解释零违约金方案",
        },
        snapshot=snap,
    )
    assert "stage=engaged | owner=ai_steward" in index
    assert "hard_boundaries=dnd" in index
    assert "open_commitments=1" in index
    assert "subflow=qual_check:ask_city" in index
    assert "客户偏好晚上沟通" in index
    assert "重点解释零违约金方案" in index
    assert "<think>" not in index
    assert "<internal>" not in index
