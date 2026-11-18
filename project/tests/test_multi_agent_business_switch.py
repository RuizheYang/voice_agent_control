from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import pytest_asyncio
from pydantic import ValidationError
from tortoise import Tortoise

from app.agents.contracts import (
    AgentRuntimeDeps,
    BusinessDomainPack,
    ClueStructuredMemory,
    DeclarativeEvaluatorSpec,
    DeclarativeHttpToolSpec,
    DeclarativeToolParamSpec,
)
from app.agents.domain_registry import DomainPackRegistry
from app.agents.memory_dreamer import MemoryDreamer
from app.agents.supervisor import ClueSupervisorAgent
from app.agents.tool_bus import (
    CredentialProxy,
    DeclarativeHttpToolFactory,
    build_domain_toolset,
    tool_handoff_workflow,
    tool_schedule_followup,
    tool_set_wakeup_timer,
)
from app.clue_handler.clue import ClueHandler
from app.dbmodels import crud
from app.dbmodels.models import CallRecord, ClueInfo, ExecutionTask, WorkflowDomainConfig
from app.schemas.channel import WechatNoticeRequest
from app.strategy.agent_driven.evaluators import EvaluatorRegistry, SubPromptEvaluatorsPipeline
from app.utils.time_utils import get_now

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def setup_in_memory_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["app.dbmodels.models"]},
    )
    await Tortoise.generate_schemas()
    yield
    await Tortoise.close_connections()


async def test_yaml_cartridge_loading_and_db_override():
    """测试 YAML 卡带加载与数据库热覆盖优先级 (DB > YAML)"""
    registry = DomainPackRegistry()
    packs = registry.load_yaml_cartridges(force=True)
    assert "new_car_sales" in packs
    assert "car_loan" in packs
    assert packs["car_loan"].custom_evaluators[0].name == "loan_qualification"

    # 数据库覆盖更新版本与提示词
    override_cfg = packs["car_loan"].model_dump()
    override_cfg["version"] = "v2.0-hotfix"
    override_cfg["domain_master_prompt"] = "DB热更新后的车贷准则"
    await crud.save_workflow_domain_config("car_loan", override_cfg, version="v2.0-hotfix")

    registry.invalidate("car_loan")
    loaded = await registry.get_pack("car_loan")
    assert loaded.version == "v2.0-hotfix"
    assert loaded.domain_master_prompt == "DB热更新后的车贷准则"


async def test_pydantic_ai_toolset_isolation_and_anti_pingpong():
    """测试 PydanticAI FilteredToolset / PreparedToolset 白名单隔离与防乒乓隐身"""
    registry = DomainPackRegistry()
    registry.load_yaml_cartridges(force=True)
    supervisor = ClueSupervisorAgent(registry=registry)

    # 1. new_car_sales 看不见 car_loan 专属工具 tool_calc_loan_plan
    deps_new_car = await supervisor.build_runtime_deps("clue_t1", "new_car_sales", dry_run=True)
    tools_new_car = await supervisor.inspect_visible_tools(deps_new_car)
    names_new_car = {t["name"] for t in tools_new_car}
    assert "tool_handoff_workflow" in names_new_car
    assert "tool_calc_loan_plan" not in names_new_car

    # PreparedToolset 应把下游目标描述注入 tool_handoff_workflow
    handoff_meta = next(t for t in tools_new_car if t["name"] == "tool_handoff_workflow")
    assert "car_loan" in (handoff_meta["description"] or "")

    # 2. car_loan 可以看见 tool_calc_loan_plan
    deps_loan = await supervisor.build_runtime_deps("clue_t2", "car_loan", dry_run=True)
    tools_loan = await supervisor.inspect_visible_tools(deps_loan)
    names_loan = {t["name"] for t in tools_loan}
    assert "tool_calc_loan_plan" in names_loan

    # 3. 达到 max_handoffs=2 后，tool_handoff_workflow 从可见工具集自动消失
    deps_loan.handoff_count = 2
    tools_loan_capped = await supervisor.inspect_visible_tools(deps_loan)
    names_loan_capped = {t["name"] for t in tools_loan_capped}
    assert "tool_handoff_workflow" not in names_loan_capped


async def test_declarative_http_tool_ssrf_and_credential_proxy():
    """测试声明式 HTTP 工具 SSRF 校验与 CredentialProxy 密钥隔离注入"""
    with pytest.raises(ValidationError):
        DeclarativeHttpToolSpec(
            name="tool_bad",
            description="bad url",
            url="http://169.254.169.254/latest/meta-data",
        )

    proxy = CredentialProxy()
    proxy.register_secret("finance_api_token", {"Authorization": "Bearer super_secret_xyz"})
    spec = DeclarativeHttpToolSpec(
        name="tool_calc_loan_plan",
        description="试算月供",
        method="POST",
        url="https://internal-finance.example.com/api/loan/simulate",
        credential_ref="finance_api_token",
        timeout_seconds=2.0,
        parameters={
            "car_price_wan": DeclarativeToolParamSpec(type="float", description="车价", required=True),
        },
    )
    toolset = DeclarativeHttpToolFactory.build_toolset([spec], proxy=proxy)
    assert "tool_calc_loan_plan" in toolset.tools
    # 密钥绝不出现在工具 schema/description 中
    assert "super_secret_xyz" not in str(toolset.tools["tool_calc_loan_plan"].tool_def)

    pack = BusinessDomainPack(workflow_id="car_loan", declarative_http_tools=[spec], allowed_tools=["tool_calc_loan_plan"])
    deps = AgentRuntimeDeps(clue_id="clue_http_1", domain_pack=pack, dry_run=False)
    from pydantic_ai import RunContext
    ctx = RunContext(deps=deps, model=None, usage=None)

    captured_headers = {}

    class _FakeResp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"monthly_payment": 3200}

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass
        async def post(self, url, json=None, headers=None):
            captured_headers.update(headers or {})
            return _FakeResp()

    with patch("app.agents.tool_bus.httpx.AsyncClient", _FakeClient):
        res = await toolset.tools["tool_calc_loan_plan"].function(ctx, car_price_wan=15.0)
        assert res["ok"] is True
        assert res["data"]["monthly_payment"] == 3200
        assert captured_headers.get("Authorization") == "Bearer super_secret_xyz"


async def test_declarative_evaluator_dynamic_model_creation():
    """测试免写 Python 类的声明式业务评估器动态生成与并发执行"""
    registry = EvaluatorRegistry()
    spec = DeclarativeEvaluatorSpec(
        name="loan_qualification",
        prompt="判断客户社保与首付情况",
        output_fields={"has_stable_income": "bool", "expected_down_payment": "str"},
    )
    resolved = registry.resolve_for_domain(
        enabled_types=["intent", "sentiment"],
        custom_evaluators=[spec],
    )
    names = [r.name for r in resolved]
    assert names == ["intent", "sentiment", "loan_qualification"]

    dyn_model = resolved[2].output_type
    instance = dyn_model(has_stable_income=True, expected_down_payment="30%", summary="资质良好")
    assert instance.has_stable_income is True
    assert instance.expected_down_payment == "30%"

    pipeline = SubPromptEvaluatorsPipeline(registry=registry)
    outputs = await pipeline.evaluate_all(
        messages=[{"role": "user", "content": "我有社保，首付三成"}],
        reference_time="2026-11-17 15:00:00",
        enabled_types=["intent"],
        custom_evaluators=[spec],
    )
    assert "intent" in outputs
    assert "loan_qualification" in outputs


async def test_atomic_workflow_handoff_and_max_depth_guardrail():
    """测试跨业务原子热切换、旧任务清理、上下文继承与防乒乓熔断"""
    await ClueInfo.create(
        clue_id="clue_hf_1",
        workflow_id="new_car_sales",
        agent_id="agent_new_car",
        strategy_id="strat_new_car",
        phone_number="13911112222",
        status="scheduled",
    )
    await ExecutionTask.create(
        task_key="call_old_1",
        task_type="call",
        clue_id="clue_hf_1",
        workflow_id="new_car_sales",
        run_at=get_now() + timedelta(hours=2),
        status="pending",
    )

    # 第 1 次切换: new_car_sales -> car_loan
    res1 = await crud.atomic_handoff_clue_workflow(
        clue_id="clue_hf_1",
        target_workflow_id="car_loan",
        target_agent_id="agent_loan",
        target_strategy_id="strat_loan",
        handoff_summary="客户资金偏紧，想了解低息分期方案",
        max_handoffs=2,
    )
    assert res1["success"] is True
    assert res1["handoff_count"] == 1

    clue = await ClueInfo.filter(clue_id="clue_hf_1").first()
    assert clue.workflow_id == "car_loan"
    assert clue.agent_id == "agent_loan"
    assert clue.strategy_id == "strat_loan"
    assert "低息分期方案" in clue.agent_data["agent_directive"]

    old_task = await ExecutionTask.filter(task_key="call_old_1").first()
    assert old_task.status == "cancelled"

    # 第 2 次切换: car_loan -> new_car_sales
    res2 = await crud.atomic_handoff_clue_workflow(
        clue_id="clue_hf_1",
        target_workflow_id="new_car_sales",
        target_agent_id="agent_new_car",
        target_strategy_id="strat_new_car",
        handoff_summary="客户决定全款",
        max_handoffs=2,
    )
    assert res2["success"] is True
    assert res2["handoff_count"] == 2

    # 第 3 次切换: 被 max_handoffs=2 护栏拦截
    res3 = await crud.atomic_handoff_clue_workflow(
        clue_id="clue_hf_1",
        target_workflow_id="car_loan",
        target_agent_id="agent_loan",
        target_strategy_id="strat_loan",
        handoff_summary="再次切换",
        max_handoffs=2,
    )
    assert res3["success"] is False
    assert "max_handoffs_reached" in res3["reason"]


async def test_four_bucket_memory_dreamer_and_global_outbox_pacer():
    """测试 4 桶结构化记忆整理与跨业务跨线索 24h 全局 Outbox 节流器"""
    await ClueInfo.create(
        clue_id="clue_mem_1",
        workflow_id="new_car_sales",
        phone_number="13988889999",
        status="created",
    )
    for i in range(10):
        await MemoryDreamer.curate_and_persist(
            "clue_mem_1",
            directive=f"指引_{i}",
            customer_facts={"budget": "15万", "city": "上海"},
            digest_entry=f"第{i}轮沟通纪要",
            progress=f"round_{i}",
        )

    clue = await ClueInfo.filter(clue_id="clue_mem_1").first()
    mem = MemoryDreamer.load_memory(clue.agent_data)
    assert len(mem.interaction_digest) == MemoryDreamer.MAX_DIGEST_ITEMS
    assert mem.customer_notes["budget"] == "15万"
    assert "15万" in mem.to_compact_index()

    # Global Outbox Pacer: 同手机号跨业务触达计数拦截
    await CallRecord.create(
        call_id="call_pacer_1",
        clue_id="clue_mem_1",
        agent_id="a1",
        strategy_id="s1",
        submitted_at=get_now() - timedelta(hours=1),
    )
    touches = await crud.count_recent_outbound_touches_by_phone(
        "13988889999", get_now() - timedelta(hours=24)
    )
    assert touches == 1

    from pydantic_ai import RunContext
    pack = BusinessDomainPack(workflow_id="car_loan", max_touches_per_24h=1)
    deps = AgentRuntimeDeps(
        clue_id="clue_mem_1",
        domain_pack=pack,
        recent_24h_touches=touches,
        dry_run=True,
    )
    ctx = RunContext(deps=deps, model=None, usage=None)
    sched_res = await tool_schedule_followup(
        ctx, target_time_iso=(get_now() + timedelta(hours=3)).isoformat()
    )
    assert sched_res["scheduled"] is False
    assert "global_outbox_24h_limit_reached" in sched_res["blocked_reason"]


async def test_wakeup_timer_and_channel_cancellation():
    """测试认知自唤醒定时器设定、覆盖及渠道事件提前到达自动作废"""
    await ClueInfo.create(clue_id="clue_wake_1", workflow_id="car_loan", status="pending")
    from pydantic_ai import RunContext

    pack = BusinessDomainPack(workflow_id="car_loan")
    deps = AgentRuntimeDeps(clue_id="clue_wake_1", domain_pack=pack, dry_run=False)
    ctx = RunContext(deps=deps, model=None, usage=None)

    res1 = await tool_set_wakeup_timer(
        ctx,
        wake_at_iso=(get_now() + timedelta(hours=2)).isoformat(),
        check_goal="检查微信是否通过",
    )
    assert res1["timer_set"] is True

    # 设定新 timer 应自动取消旧 timer
    res2 = await tool_set_wakeup_timer(
        ctx,
        wake_at_iso=(get_now() + timedelta(hours=4)).isoformat(),
        check_goal="再次检查微信",
    )
    assert res2["timer_set"] is True
    old_t = await ExecutionTask.get(id=res1["task_id"])
    assert old_t.status == "cancelled"

    # 微信通过通知到达，应取消剩余 timer
    handler = ClueHandler(MagicMock(), MagicMock())
    notice = MagicMock(spec=WechatNoticeRequest)
    notice.clueId = "clue_wake_1"
    await handler.on_channel_notify(notice)

    new_t = await ExecutionTask.get(id=res2["task_id"])
    assert new_t.status == "cancelled"


async def test_inflow_auto_triage_and_dry_run_preview():
    """测试 workflowId='auto' 自动分诊路由与 Dry-Run 预检接口逻辑"""
    registry = DomainPackRegistry()
    registry.load_yaml_cartridges(force=True)
    supervisor = ClueSupervisorAgent(registry=registry)

    routed = await supervisor.triage_inflow_workflow(
        agent_data={"remark": "客户重点询问低息车贷首付和月供分期"},
        fallback_workflow_id="default",
    )
    assert routed == "car_loan"

    preview = await supervisor.dry_run_workflow(
        workflow_id="car_loan",
        conversation=[{"role": "user", "content": "我有稳定社保，想分期36期"}],
        total_call_count=1,
    )
    assert preview["workflow_id"] == "car_loan"
    assert "loan_qualification" in preview["custom_evaluators"]
    assert any(t["name"] == "tool_calc_loan_plan" for t in preview["visible_tools"])
    assert preview["schedule_preview"]["success"] is True


async def test_deferred_approval_and_dry_run_router_endpoint():
    """测试高危工具 Deferred Approval 标记与 /workflow/{id}/dry-run 路由接口"""
    from app.routers.config_admin import dry_run_workflow
    from app.schemas.workflow_config import WorkflowDryRunRequest
    from app.agents.domain_registry import domain_pack_registry

    pack = BusinessDomainPack(
        workflow_id="vip_retention",
        name="VIP挽回线",
        approval_required_tools=["tool_terminate_clue"],
    )
    domain_pack_registry.register_pack(pack)

    resp = await dry_run_workflow(
        workflow_id="vip_retention",
        request=WorkflowDryRunRequest(
            conversation=[{"role": "user", "content": "先别急着挂，我再想想"}],
            total_call_count=1,
        ),
    )
    assert resp.code == 0
    tools_map = {t["name"]: t for t in resp.data["visible_tools"]}
    assert tools_map["tool_terminate_clue"]["requires_approval"] is True
    assert tools_map["tool_schedule_followup"]["requires_approval"] is False
    domain_pack_registry.invalidate("vip_retention")


async def test_inbox_fast_ack_idempotency_and_wakeup_worker():
    """测试 Event Inbox 秒回入箱幂等性、以及 InboxWakeupWorker 消费 call_end_inbox 与 agent_wakeup"""
    from app.agents.inbox_worker import InboxWakeupWorker, enqueue_call_end_inbox
    from app.schemas.call_end import CallEndCurrentCall, CallEndData, CallEndRequest

    req = CallEndRequest.model_construct(
        requestId="req_inbox_001",
        data=CallEndData.model_construct(
            clueId="clue_inbox_1",
            currentCall=CallEndCurrentCall.model_construct(callId="call_inbox_1", batchId="b_1"),
        ),
    )

    first = await enqueue_call_end_inbox(req)
    second = await enqueue_call_end_inbox(req)
    assert first["accepted"] is True and first["duplicate"] is False
    assert second["accepted"] is True and second["duplicate"] is True
    assert first["task_id"] == second["task_id"]

    # 插入一条到期的 agent_wakeup 定时唤醒任务
    wakeup_task = await ExecutionTask.create(
        task_key="agent_wakeup_test_1",
        task_type="agent_wakeup",
        clue_id="clue_inbox_1",
        payload={"checkGoal": "核查用户承诺回款是否到账"},
        status="pending",
        run_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        max_retries=3,
    )

    fake_handler = MagicMock()
    fake_handler.on_call_end = AsyncMock(return_value=True)
    fake_handler.on_wakeup_timer = AsyncMock(return_value=True)

    worker = InboxWakeupWorker()
    with patch(
        "app.clue_handler.factory.ClueHandlerFactory.get_handler_by_clue",
        new=AsyncMock(return_value=fake_handler),
    ):
        assert await worker.run_once() is True
        assert await worker.run_once() is True
        assert await worker.run_once() is False

    fake_handler.on_call_end.assert_awaited_once()
    fake_handler.on_wakeup_timer.assert_awaited_once_with(
        "clue_inbox_1", check_goal="核查用户承诺回款是否到账"
    )
    inbox_db = await ExecutionTask.get(id=first["task_id"])
    wakeup_db = await ExecutionTask.get(id=wakeup_task.id)
    assert inbox_db.status == "succeeded"
    assert wakeup_db.status == "succeeded"


async def test_inbox_worker_lock_timeout_triggers_retry_backoff():
    """测试 InboxWakeupWorker 遇到 LockTimeoutError 时进入退避重试而非直接标失败"""
    from app.agents.inbox_worker import InboxWakeupWorker, enqueue_call_end_inbox
    from app.schemas.call_end import CallEndCurrentCall, CallEndData, CallEndRequest
    from app.utils.exceptions import LockTimeoutError

    req = CallEndRequest.model_construct(
        requestId="req_lock_retry_1",
        data=CallEndData.model_construct(
            clueId="clue_lock_1",
            currentCall=CallEndCurrentCall.model_construct(callId="call_lock_1"),
        ),
    )
    enqueued = await enqueue_call_end_inbox(req)

    fake_handler = MagicMock()
    fake_handler.on_call_end = AsyncMock(side_effect=LockTimeoutError("clue_lock_1 lock busy"))

    worker = InboxWakeupWorker()
    with patch(
        "app.clue_handler.factory.ClueHandlerFactory.get_handler_by_clue",
        new=AsyncMock(return_value=fake_handler),
    ):
        assert await worker.run_once() is True

    task_db = await ExecutionTask.get(id=enqueued["task_id"])
    assert task_db.status == "retry"
    assert task_db.retry_count == 1
    assert "LockTimeoutError" in (task_db.last_error or "")


async def test_postcall_handler_recovers_when_clue_stuck_in_summary():
    """测试 Bug A 修复：线索停留在 summary 但 CallRecord.summary_status 未成功时可恢复继续处理"""
    from app.clue_handler.stages.postcall.default import DefaultPostCallHandler
    from app.schemas.call_end import CallEndCurrentCall, CallEndData, CallEndRequest

    clue = await ClueInfo.create(
        clue_id="clue_recover_summary_1",
        status="summary",
        workflow_id="new_car_sales",
        agent_id="agent_1",
        strategy_id="strat_1",
        last_call_id="call_recover_1",
    )
    call_rec = await CallRecord.create(
        clue_id=clue.clue_id,
        call_id="call_recover_1",
        agent_id="agent_1",
        strategy_id="strat_1",
        call_task_id=101,
        status="ended",
        answered=1,
        summary_status="pending",
    )

    req = CallEndRequest.model_construct(
        requestId="req_recover_1",
        data=CallEndData.model_construct(
            clueId=clue.clue_id,
            currentCall=CallEndCurrentCall.model_construct(callId=call_rec.call_id),
        ),
    )

    fake_schedule_res = MagicMock(
        business_status="success",
        reason="恢复后成功完成评估",
        master_decision=None,
        agent_directive=None,
        should_schedule=False,
    )
    mock_engine = MagicMock()
    mock_engine.calculate = AsyncMock(return_value=fake_schedule_res)
    handler = DefaultPostCallHandler(strategy_engine=mock_engine)

    fake_dq = MagicMock()
    fake_dq.remove_task = AsyncMock(return_value=True)
    fake_lock = MagicMock()
    fake_lock.__aenter__ = AsyncMock(return_value=True)
    fake_lock.__aexit__ = AsyncMock(return_value=False)
    with patch(
        "app.dbmodels.crud.create_call_end_record",
        new=AsyncMock(return_value=call_rec),
    ), patch(
        "app.clue_handler.stages.postcall.default.redis_manager.get_delayed_queue",
        return_value=fake_dq,
    ), patch(
        "app.dbmodels.crud.redis_manager.get_lock",
        return_value=fake_lock,
    ), patch(
        "app.agents.memory_dreamer.MemoryDreamer.schedule_background_dream",
    ), patch(
        "app.observability.events_bus.event_bus.emit",
        new=AsyncMock(),
    ):
        ok = await handler.handle(req)

    assert ok is True
    mock_engine.calculate.assert_awaited_once()
    refreshed = await ClueInfo.get(clue_id=clue.clue_id)
    assert refreshed.status == "accepted"


async def test_atomic_handoff_refreshes_clue_config_from_target_strategy():
    """测试 Bug B2 修复：跨业务线交接时同步刷新 clue.clue_config 为目标 OutboundStrategy 配置"""
    from app.dbmodels.models import OutboundStrategy

    await OutboundStrategy.create(
        strategy_id="strat_car_loan_target",
        name="车贷标准策略",
        schedule_id="seven_day_three_call",
        followup={"max_calls": 5, "min_interval_seconds": 3600},
        sip={"line_id": "sip_loan_01"},
    )
    await ClueInfo.create(
        clue_id="clue_handoff_cfg_1",
        status="summary",
        workflow_id="new_car_sales",
        agent_id="agent_car_1",
        strategy_id="strat_new_car_1",
        clue_config={"followup": {"maxCalls": 2}, "sip": {"lineId": "sip_car_old"}},
    )

    res = await crud.atomic_handoff_clue_workflow(
        clue_id="clue_handoff_cfg_1",
        target_workflow_id="car_loan",
        target_agent_id="agent_loan_1",
        target_strategy_id="strat_car_loan_target",
        handoff_summary="客户需要车贷方案",
    )
    assert res["success"] is True, res

    updated = await ClueInfo.get(clue_id="clue_handoff_cfg_1")
    assert updated.workflow_id == "car_loan"
    assert updated.strategy_id == "strat_car_loan_target"
    assert updated.clue_config["followup"]["maxCalls"] == 5
    assert updated.clue_config["sip"]["lineId"] == "sip_loan_01"

