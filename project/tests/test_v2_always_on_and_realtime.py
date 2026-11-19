from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import httpx
import pytest
import pytest_asyncio
from tortoise import Tortoise

from app.agents.approvals import create_deferred_approval_task, resolve_deferred_approval
from app.agents.contracts import BusinessDomainPack, DeclarativeHttpToolSpec, DeclarativeToolParamSpec
from app.agents.domain_registry import domain_pack_registry
from app.agents.inbox_worker import InboxWakeupWorker, enqueue_channel_inbox
from app.agents.memory_dreamer import MemoryDreamer
from app.agents.tool_bus import CredentialProxy, execute_tool_by_name
from app.apis.telephony import (
    LineType,
    LiveKitTelephonyGateway,
    PipecatTelephonyGateway,
    build_telephony_params,
    render_dynamic_agent_context,
    resolve_telephony_gateway,
)
from app.clue_handler.state import ClueState
from app.dbmodels import crud
from app.dbmodels.models import CallRecord, ClueInfo, ExecutionTask, OutboundStrategy
from app.main import app
from app.schemas.clue_create import NormalizedClue
from app.strategy.agent_driven.engine import AgentDrivenStrategyEngine
from app.strategy.agent_driven.models import (
    AgentStrategyConfig,
    MasterSummaryDecision,
    ScheduleActionType,
    StandardBusinessStatus,
)
from app.strategy.contracts.context import ClueContext
from app.utils.time_utils import get_now

pytestmark = pytest.mark.asyncio
CST = ZoneInfo("Asia/Shanghai")


class _DummyLock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class _DummyDQ:
    async def add_task(self, *args, **kwargs):
        pass

    async def remove_task(self, *args, **kwargs):
        pass


@pytest_asyncio.fixture(autouse=True)
async def setup_v2_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["app.dbmodels.models"]},
    )
    await Tortoise.generate_schemas()
    domain_pack_registry.load_yaml_cartridges(force=True)
    with patch("app.redis_client.redis_manager.redis_manager.get_lock", return_value=_DummyLock()), \
         patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue", return_value=_DummyDQ()), \
         patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock):
        yield
    domain_pack_registry.invalidate()
    await Tortoise.close_connections()


async def test_tier0_zero_llm_short_circuit_for_unanswered_calls():
    """验证 Tier 0 物理短路：未接通电话 0 次 LLM 调用，并保留连续未接切工作手机与熔断规则"""
    cfg = AgentStrategyConfig(evaluation_mode="single_pass", noanswer_num=4, max_call_num=10)
    mock_eval = MagicMock()
    mock_eval.evaluate_all = AsyncMock()
    mock_syn = MagicMock()
    mock_syn.synthesize = AsyncMock()

    engine = AgentDrivenStrategyEngine(config=cfg, evaluators=mock_eval, synthesizer=mock_syn)

    rec1 = {
        "answered": 0,
        "outcome": "no_answer",
        "duration_seconds": 0,
        "create_time": datetime(2026, 11, 19, 13, 30, 0, tzinfo=CST),
        "config_snapshot": {"aiCallContextList": []},
    }
    rec2 = {
        "answered": 0,
        "outcome": "no_answer",
        "duration_seconds": 0,
        "create_time": datetime(2026, 11, 19, 11, 0, 0, tzinfo=CST),
        "config_snapshot": {"aiCallContextList": []},
    }

    ctx = ClueContext(
        clue_id="clue_tier0_1",
        clue_intake_time=get_now() - timedelta(hours=4),
        call_records=[rec1, rec2],
        call_end_time=datetime(2026, 11, 19, 14, 0, 0, tzinfo=CST),
        total_call_count=2,
    )

    with patch("app.agents.supervisor.clue_supervisor_agent.run_single_pass_postcall", new_callable=AsyncMock) as mock_sup:
        res = await engine.calculate(ctx)
        mock_sup.assert_not_called()

    mock_eval.evaluate_all.assert_not_called()
    mock_syn.synthesize.assert_not_called()
    assert res.success is True
    assert res.use_work_phone is True
    assert res.business_status == "failure"


async def test_tier1_single_pass_mounts_supervisor_and_harness():
    """验证 Tier 1 Single-Pass 模式正式打通 ClueSupervisorAgent -> PolicyHarness 主链路"""
    cfg = AgentStrategyConfig(evaluation_mode="single_pass", workflow_id="car_loan")
    engine = AgentDrivenStrategyEngine(config=cfg)

    rec = {
        "answered": 1,
        "outcome": "connected",
        "duration_seconds": 45,
        "create_time": datetime(2026, 11, 19, 13, 55, 0, tzinfo=CST),
        "conversation": [
            {"role": "assistant", "content": "您好，了解分期吗？"},
            {"role": "user", "content": "明天下午三点打给我详细算下月供。"},
        ],
    }
    ctx = ClueContext(
        clue_id="clue_tier1_1",
        clue_intake_time=get_now() - timedelta(hours=1),
        call_records=[rec],
        call_end_time=datetime(2026, 11, 19, 14, 0, 0, tzinfo=CST),
        total_call_count=1,
    )

    fake_decision = MasterSummaryDecision(
        status=StandardBusinessStatus.PARTIAL,
        reason="客户预约明天下三点回拨",
        action=ScheduleActionType.SCHEDULE_CALL,
        next_call_time=datetime(2026, 11, 20, 15, 0, 0, tzinfo=CST),
        voice_agent_directive="携带测算好的36期月供方案回访",
    )

    with patch(
        "app.agents.supervisor.clue_supervisor_agent.run_single_pass_postcall",
        new=AsyncMock(return_value=fake_decision),
    ) as mock_sp:
        res = await engine.calculate(ctx)

    mock_sp.assert_awaited_once()
    assert res.success is True
    assert res.voice_agent_directive == "携带测算好的36期月供方案回访"


async def test_event_coalescing_and_channel_notify_closes_clue():
    """验证 Unified Event Inbox 事件合并 (agent_wakeup + channel_inbox) 与 /clue/channel/notify 闭环"""
    clue = await ClueInfo.create(
        clue_id="clue_coal_1",
        workflow_id="new_car_sales",
        agent_id="default",
        strategy_id="default",
        phone_number="13800138000",
        status=ClueState.PENDING.value,
    )
    wakeup = await ExecutionTask.create(
        task_key="wake_coal_1",
        task_type="agent_wakeup",
        clue_id=clue.clue_id,
        workflow_id="new_car_sales",
        payload={"checkGoal": "检查企微是否通过"},
        status="pending",
        run_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    ch_res = await enqueue_channel_inbox(
        clue.clue_id,
        {"msgType": "notice", "data": {"custId": clue.clue_id, "resultCode": "3"}, "success": True},
    )

    worker = InboxWakeupWorker()
    assert await worker.run_once() is True

    refreshed_clue = await ClueInfo.get(clue_id=clue.clue_id)
    assert refreshed_clue.status == ClueState.ACCEPTED.value
    refreshed_wake = await ExecutionTask.get(id=wakeup.id)
    refreshed_ch = await ExecutionTask.get(id=ch_res["task_id"])
    assert refreshed_wake.status == "succeeded"
    assert refreshed_ch.status == "succeeded"
    assert refreshed_ch.result.get("coalesced_into") == wakeup.id

    # 测试 HTTP /clue/channel/notify?sync=true 端点同步直连
    await ClueInfo.create(
        clue_id="clue_ch_http_1",
        workflow_id="new_car_sales",
        phone_number="13800138001",
        status=ClueState.PENDING.value,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/clue/channel/notify?sync=true",
            json={"msgType": "notice", "data": {"custId": "clue_ch_http_1", "resultCode": "3"}},
        )
        assert resp.status_code == 200
        assert resp.json()["code"] == 0

    c2 = await ClueInfo.get(clue_id="clue_ch_http_1")
    assert c2.status == ClueState.ACCEPTED.value


async def test_deferred_approval_suspend_and_resume_execution():
    """验证 Deferred Tool Calls 挂起 (AWAITING_APPROVAL) 与人工审批后唤醒恢复执行"""
    pack = BusinessDomainPack(
        workflow_id="vip_approval_wf",
        name="VIP审批线",
        allowed_tools=["tool_terminate_clue", "tool_schedule_followup"],
        approval_required_tools=["tool_terminate_clue"],
    )
    domain_pack_registry.register_pack(pack)

    await ClueInfo.create(
        clue_id="clue_appr_1",
        workflow_id="vip_approval_wf",
        phone_number="13900001111",
        status=ClueState.SUMMARY.value,
    )

    # 1. 调用受审批保护的高危工具 -> 应自动挂起
    exec_res = await execute_tool_by_name(
        clue_id="clue_appr_1",
        tool_name="tool_terminate_clue",
        tool_args={"terminal_status": "rejected", "reason": "高客诉风险需人工复核"},
        workflow_id="vip_approval_wf",
    )
    assert exec_res["status"] == "deferred_approval"
    approval_task_id = exec_res["approval_task_id"]

    clue_mid = await ClueInfo.get(clue_id="clue_appr_1")
    assert clue_mid.status == ClueState.AWAITING_APPROVAL.value

    # 2. 通过 HTTP API 查询并审批通过
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        list_resp = await client.get("/api/approvals/pending?clue_id=clue_appr_1")
        assert list_resp.status_code == 200
        assert len(list_resp.json()["data"]) == 1

        res_resp = await client.post(
            f"/api/approvals/{approval_task_id}/resolve",
            json={"approved": True, "reviewer": "qa_lead", "comment": "确认终止"},
        )
        assert res_resp.status_code == 200
        assert res_resp.json()["data"]["approved"] is True

    # 3. InboxWakeupWorker 消费 approval_resumed 唤醒任务，自动执行获批工具收口终态
    worker = InboxWakeupWorker()
    assert await worker.run_once() is True

    clue_done = await ClueInfo.get(clue_id="clue_appr_1")
    assert clue_done.status == ClueState.REJECTED.value
    assert clue_done.close_reason == "高客诉风险需人工复核"
    domain_pack_registry.invalidate("vip_approval_wf")


async def test_atomic_memory_dreamer_no_lost_updates():
    """验证 MemoryDreamer.curate_and_persist 并发写入不会互相覆盖四桶记忆"""
    await ClueInfo.create(
        clue_id="clue_mem_atomic_1",
        workflow_id="new_car_sales",
        phone_number="13912345678",
        status="created",
    )
    await asyncio.gather(
        MemoryDreamer.curate_and_persist("clue_mem_atomic_1", customer_facts={"budget": "25万"}),
        MemoryDreamer.curate_and_persist("clue_mem_atomic_1", customer_facts={"city": "杭州"}),
        MemoryDreamer.curate_and_persist("clue_mem_atomic_1", digest_entry="第1轮通话纪要"),
    )
    clue = await ClueInfo.get(clue_id="clue_mem_atomic_1")
    mem = MemoryDreamer.load_memory(clue.agent_data)
    assert mem.customer_notes.get("budget") == "25万"
    assert mem.customer_notes.get("city") == "杭州"
    assert "第1轮通话纪要" in mem.interaction_digest


async def test_dynamic_working_window_and_memory_injection_in_telephony():
    """验证自定义工时窗口 (非硬编码 9-21) 与四桶记忆动态注入 callParams"""
    agent_cfg = {
        "agent_id": "agent_v2",
        "version": "v2.0",
        "status": "active",
        "call_type": "outbound",
        "open_message": "您好，继续为您跟进。",
        "global_settings": {
            "instruction": "你是新车销售顾问。",
            "tts": {"provider_id": "tts_01"},
        },
    }
    agent_data = MemoryDreamer.apply_to_agent_data(
        {},
        directive="重点介绍周末试驾礼包",
        customer_facts={"model": "SUV-X"},
        digest_entry="客户上周询问过试驾",
        progress="partial",
    )

    from app.utils.decrypt import IT_VERSION_LABEL, crypted_manager
    mock_record = MagicMock()
    mock_record.clue_id = "clue_dyn_1"
    mock_record.call_id = "call_dyn_1"
    mock_record.agent_id = "agent_v2"
    mock_record.config_snapshot = {
        "phoneNumber": crypted_manager.encrypt_field("13800138000", IT_VERSION_LABEL),
        "agentData": agent_data,
        "config": {
            "timezone": "Asia/Shanghai",
            "sip": {"accessorId": "acc_1", "lineId": "line_1"},
            "followup": {"work_start_time": "10:00:00", "work_end_time": "18:00:00"},
        },
    }

    # 19:00 超出自定义的 10:00-18:00 窗口，必须被拦截
    with pytest.raises(ValueError, match="outside call window"):
        build_telephony_params(
            mock_record,
            agent_cfg,
            now=datetime(2026, 11, 19, 19, 0, 0, tzinfo=CST),
            line_type=LineType.LANDLINE,
        )

    # 14:00 在 10:00-18:00 窗口内，正常生成并注入动态记忆与指令
    tele_api = build_telephony_params(
        mock_record,
        agent_cfg,
        now=datetime(2026, 11, 19, 14, 0, 0, tzinfo=CST),
        line_type=LineType.LANDLINE,
    )
    call_params = json.loads(tele_api.phoneList[0].callParams)
    assert "重点介绍周末试驾礼包" in call_params["dynamicInstruction"]
    assert "SUV-X" in call_params["memoryIndex"]


async def test_in_call_realtime_bootstrap_tool_invoke_and_handoff():
    """验证 LiveKit / Pipecat 通话中实时控制面: bootstrap / tool-invoke / mid-call handoff"""
    await ClueInfo.create(
        clue_id="clue_incall_1",
        workflow_id="new_car_sales",
        agent_id="default",
        strategy_id="default",
        phone_number="13899990000",
        status=ClueState.CALLING.value,
        agent_data=MemoryDreamer.apply_to_agent_data(
            {},
            directive="了解试驾偏好",
            customer_facts={"budget": "18万"},
            digest_entry="首次接通",
        ),
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # 1. Bootstrap 拉取实时上下文与 Flow
        boot_resp = await client.get("/clue/in-call/clue_incall_1/bootstrap")
        assert boot_resp.status_code == 200
        boot_data = boot_resp.json()["data"]
        assert boot_data["workflow_id"] == "new_car_sales"
        assert boot_data["in_call_flow"]["initial_node"] == "greeting_triage"
        assert "18万" in boot_data["memory_index"]

        # 2. new_car_sales 越权调用 car_loan 的 tool_calc_loan_plan 应被拦截
        blocked_resp = await client.post(
            "/clue/in-call/clue_incall_1/tool-invoke",
            json={"tool_name": "tool_calc_loan_plan", "tool_args": {"car_price_wan": 18, "down_payment_ratio": 0.3, "months": 36}},
        )
        assert blocked_resp.json()["data"]["status"] == "blocked"

        # 3. 通话中实时切换至 car_loan
        hf_resp = await client.post(
            "/clue/in-call/clue_incall_1/handoff",
            json={"target_workflow_id": "car_loan", "handoff_summary": "客户通话中改问36期低息车贷"},
        )
        assert hf_resp.status_code == 200
        hf_data = hf_resp.json()["data"]
        assert hf_data["success"] is True
        assert hf_data["to_workflow"] == "car_loan"
        assert hf_data["target_pack"]["in_call_flow"]["initial_node"] == "loan_consultation"
        assert any(t["name"] == "tool_calc_loan_plan" for t in hf_data["visible_tools"])

        # 4. 切换后即可在通话中安全调用 car_loan 的 tool_calc_loan_plan (dry_run)
        ok_tool_resp = await client.post(
            "/clue/in-call/clue_incall_1/tool-invoke",
            json={
                "tool_name": "tool_calc_loan_plan",
                "tool_args": {"car_price_wan": 18.0, "down_payment_ratio": 0.3, "months": 36},
                "dry_run": True,
            },
        )
        assert ok_tool_resp.json()["data"]["ok"] is True


async def test_livekit_and_pipecat_gateways_and_transactional_create_clue():
    """验证 LiveKit/Pipecat 网关解析、载荷构建以及 crud.create_clue 事务发件箱"""
    with patch.dict(os.environ, {"TELEPHONY_GATEWAY_MODE": "livekit"}):
        gw_lk = resolve_telephony_gateway()
        assert isinstance(gw_lk, LiveKitTelephonyGateway)

    with patch.dict(os.environ, {"TELEPHONY_GATEWAY_MODE": "pipecat"}):
        gw_pc = resolve_telephony_gateway()
        assert isinstance(gw_pc, PipecatTelephonyGateway)

    mock_rec = MagicMock()
    mock_rec.clue_id = "clue_gw_1"
    mock_rec.call_id = "call_gw_1"
    mock_rec.agent_id = "agent_lk"
    mock_rec.strategy_id = "strat_lk"
    mock_rec.config_snapshot = {
        "phoneNumber": "13800001234",
        "agentConfig": {"instruction": "LiveKit测试"},
        "agentData": {"agent_directive": "询问试驾"},
    }
    lk_payload = LiveKitTelephonyGateway().build_dispatch_payload(mock_rec)
    assert lk_payload["metadata"]["bootstrap_url"].endswith("/clue/in-call/clue_gw_1/bootstrap")
    assert "询问试驾" in lk_payload["metadata"]["dynamicInstruction"]

    # 验证 crud.create_clue 同步写入 new_clue 事务发件箱
    clue_obj = await crud.create_clue(
        NormalizedClue(clueId="clue_tx_outbox_1", phoneNumber="13800138999", workflowId="new_car_sales")
    )
    new_clue_task = await ExecutionTask.filter(clue_id=clue_obj.clue_id, task_type="new_clue").first()
    assert new_clue_task is not None
    assert new_clue_task.status == "pending"


async def test_default_channel_notify_async_coalesces_with_call_end_inbox():
    """验证默认 sync=False 的 /clue/channel/notify 进入 channel_inbox 并与 call_end_inbox 合并去重"""
    from app.agents.inbox_worker import InboxWakeupWorker
    from app.dbmodels.models import CallRecord

    clue = await ClueInfo.create(
        clue_id="clue_coalesce_1",
        workflow_id="new_car_sales",
        agent_id="agent_default",
        strategy_id="strat_default",
        phone_number="13800138222",
        status=ClueState.CALLING.value,
        last_call_id="call_ce_1",
    )
    pending_call = await ExecutionTask.create(
        task_key=f"call:{clue.clue_id}:call_pending_999",
        task_type="call",
        clue_id=clue.clue_id,
        workflow_id="new_car_sales",
        payload={"callId": "call_pending_999"},
        status="pending",
        run_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    await CallRecord.create(
        call_id="call_ce_1",
        clue_id=clue.clue_id,
        agent_id="agent_default",
        strategy_id="strat_default",
        call_task_id=pending_call.id,
        status="submitted",
    )
    pending_wake = await ExecutionTask.create(
        task_key=f"wake:{clue.clue_id}:999",
        task_type="agent_wakeup",
        clue_id=clue.clue_id,
        workflow_id="new_car_sales",
        payload={"checkGoal": "等待企微通过"},
        status="pending",
        run_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # 1. 默认 Fast-ACK (/clue/channel/notify, sync=False) -> 入 channel_inbox
        ch_resp = await client.post(
            "/clue/channel/notify",
            json={"msgType": "notice", "data": {"custId": clue.clue_id, "resultCode": "3"}},
        )
        assert ch_resp.status_code == 200
        assert ch_resp.json()["code"] == 0
        assert "queued" in ch_resp.json()["msg"]

        # 2. 通话结束 Fast-ACK (/clue/call-end/notify) -> 入 call_end_inbox
        ce_resp = await client.post(
            "/clue/call-end/notify",
            json={
                "callId": "call_ce_1",
                "requestId": "req_ce_1",
                "data": {
                    "clueId": clue.clue_id,
                    "currentCall": {
                        "callId": "call_ce_1",
                        "resultCode": "1",
                        "aiCallSeconds": 45,
                        "talkStartTimeDisplay": "2026-11-19 10:00:00",
                    },
                },
            },
        )
        assert ce_resp.status_code == 200
        assert ce_resp.json()["code"] == 0
        assert "accepted" in ce_resp.json()["msg"]

    # 确保 call_end_inbox 与 channel_inbox 均已就绪 (run_at <= now)，且 call_end_inbox 优先被领取
    now_utc = datetime.now(timezone.utc)
    ch_task = await ExecutionTask.filter(clue_id=clue.clue_id, task_type="channel_inbox").first()
    ce_task = await ExecutionTask.filter(clue_id=clue.clue_id, task_type="call_end_inbox").first()
    assert ch_task is not None and ce_task is not None
    ch_task.run_at = now_utc - timedelta(seconds=2)
    await ch_task.save()
    ce_task.run_at = now_utc - timedelta(seconds=10)
    await ce_task.save()

    worker = InboxWakeupWorker()
    assert await worker.run_once() is True

    refreshed_clue = await ClueInfo.get(clue_id=clue.clue_id)
    assert refreshed_clue.status == ClueState.ACCEPTED.value

    refreshed_ce = await ExecutionTask.get(id=ce_task.id)
    refreshed_ch = await ExecutionTask.get(id=ch_task.id)
    refreshed_call = await ExecutionTask.get(id=pending_call.id)
    refreshed_wake = await ExecutionTask.get(id=pending_wake.id)
    assert refreshed_ce.status == "succeeded"
    assert refreshed_ch.status == "succeeded"
    assert refreshed_ch.result.get("coalesced_into") == ce_task.id
    assert refreshed_call.status == "cancelled"
    assert refreshed_wake.status == "cancelled"


async def test_unified_inflow_create_route_uses_db_outbox_not_redis_list():
    """验证 /clue/create 与 /clue/create/batch 收敛至 InflowService 事务发件箱，不走 Redis List，且 main.py 不再启动 clue_consumer_loop"""
    from pathlib import Path
    from app.dbmodels.models import OutboundStrategy, VoiceAgent, WorkflowBinding
    from app.clue_inflow.service import InflowService
    from app.schemas.clue_create import ClueCreateRequest, BatchClueCreateRequest

    main_src = (Path(__file__).resolve().parents[1] / "app" / "main.py").read_text(encoding="utf-8")
    assert "clue_consumer_loop" not in main_src

    await VoiceAgent.create(
        agent_id="agent_uni_1",
        agent_name="统一测试Agent",
        instruction="测试提示词",
        global_settings={},
        stt={},
        tts={},
        turn_taking={},
        llm={},
    )
    await OutboundStrategy.create(
        strategy_id="strat_uni_1",
        name="统一测试策略",
        followup={"maxFollowupHours": 48, "maxCalls": 5, "maxCallPerDay": 2, "maxNoAnswerCalls": 3},
        sip={},
        webhook=[],
    )
    await WorkflowBinding.create(
        workflow_id="wf_uni_1",
        name="统一测试线",
        agent_id="agent_uni_1",
        strategy_id="strat_uni_1",
        is_active=True,
    )

    service = InflowService()
    clue_req = ClueCreateRequest(
        clue_id="clue_unified_1",
        request_id="req_unified_1",
        workflow_id="wf_uni_1",
        phone_number="13800138333",
        customerName="王统一",
    )

    with patch("app.redis_client.redis_manager.redis_manager.rpush_list", new_callable=AsyncMock) as mock_rpush, \
         patch.object(InflowService, "dispatch_followup", new_callable=AsyncMock) as mock_dispatch:
        resp = await service.accept_clue_create(app_id="app_uni", clue_req=clue_req)
        assert resp["code"] == 0
        assert resp["data"]["inflow_type"] == "single"
        mock_rpush.assert_not_called()
        mock_dispatch.assert_awaited_once()

    clue_row = await ClueInfo.get(clue_id="clue_unified_1")
    assert clue_row.strategy_id == "strat_uni_1"
    import_task = await ExecutionTask.filter(request_id="req_unified_1", task_type="import").first()
    new_clue_task = await ExecutionTask.filter(clue_id="clue_unified_1", task_type="new_clue").first()
    assert import_task is not None and import_task.status == "succeeded"
    assert new_clue_task is not None and new_clue_task.status == "pending"

    batch_req = BatchClueCreateRequest(
        request_id="req_unified_batch_1",
        workflow_id="wf_uni_1",
        records=[
            {"clue_id": "clue_unified_b1", "phone_number": "13800138444", "agent_data": {"variables": {"name": "李批量"}}},
            {"clue_id": "clue_unified_b2", "phone_number": "13800138555", "agent_data": {"variables": {"name": "赵批量"}}},
        ],
    )
    with patch("app.redis_client.redis_manager.redis_manager.rpush_list", new_callable=AsyncMock) as mock_rpush, \
         patch.object(InflowService, "dispatch_followup", new_callable=AsyncMock):
        b_resp = await service.accept_batch_create(app_id="app_uni", batch_req=batch_req)
        assert b_resp["code"] == 0
        assert b_resp["data"]["accepted_count"] == 2
        mock_rpush.assert_not_called()

    assert await ExecutionTask.filter(clue_id="clue_unified_b1", task_type="new_clue").count() == 1
    assert await ExecutionTask.filter(clue_id="clue_unified_b2", task_type="new_clue").count() == 1
