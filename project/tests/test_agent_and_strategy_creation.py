import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

from tortoise import Tortoise
from pydantic import ValidationError

from app.main import app
from app.dbmodels.models import VoiceAgent, OutboundStrategy, WorkflowBinding, ClueInfo
from app.schemas.voice_agent_config import (
    VoiceAgentCreateRequest,
    VoiceAgentGlobalSettings,
    SttConfig,
    TtsConfig,
    TurnTakingConfig,
    LlmConfig,
)
from app.schemas.strategy_config import (
    StrategyCreateRequest,
    FollowupConfig,
    SipConfig,
    WebhookSubscription,
    SUBSCRIBABLE_EVENTS,
    EVENT_ALIAS_MAP,
)
from fastapi import HTTPException
from app.schemas.call_end import CallEndRequest, CallEndData, CallEndOutboundReport
from app.schemas.workflow_config import (
    WorkflowCreateRequest,
    WorkflowBundleCreateRequest,
)
from app.dbmodels.crud import provision_workflow_bundle
from app.routers.config_admin import (
    create_voice_agent,
    get_voice_agent,
    create_strategy,
    get_strategy,
    create_workflow_bundle,
    create_workflow,
    get_workflow,
)
from app.routers.clue_webhook import call_end, cancel_clue
from app.apis.webhook_dispatcher import (
    dispatch_webhook_event,
    SUPPORTED_EVENTS,
    CANONICAL_EVENT_MAP,
)


class TestAgentAndStrategyCreation(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        await Tortoise.init(
            db_url="sqlite://:memory:",
            modules={"models": ["app.dbmodels.models"]}
        )
        await Tortoise.generate_schemas()

    async def asyncTearDown(self):
        await Tortoise.close_connections()

    # =========================================================================
    # 1. Voice Agent 创建接口测试 (对齐文档 2.1)
    # =========================================================================

    async def test_voice_agent_create_with_full_doc_spec(self):
        """测试按照文档 2.1 完整规范创建 Voice Agent"""
        req = VoiceAgentCreateRequest(
            agent_id="agent-123456",
            agent_name="AgentA",
            version="v1.0",
            description="This agent is used to demonstrate the handoff functionality.",
            global_settings=VoiceAgentGlobalSettings(
                stt=SttConfig(
                    provider_id="tencent",
                    hotwords=["word1", "word2", "word3"],
                    language="zh-CN",
                    vad_silence=240
                ),
                tts=TtsConfig(
                    provider_id="yx",
                    voice="female",
                    language="zh-CN",
                    speed=1.0
                ),
                turn_taking=TurnTakingConfig(provider_id="yx-turn"),
                knowledge_base={"kb_id": "kb-123456"},
                tools=["check_wechat_account", "add_wechat"],
                llm=LlmConfig(model_id="gemma4-31B"),
                instruction="You are a helpful assistant. Answer user questions. {{username}}"
            ),
            agent_type="single_prompt",
            open_message="Hello, is that {{user_name}}",
            call_type="outbound"
        )

        resp = await create_voice_agent(req)
        self.assertEqual(resp.code, 0)
        self.assertEqual(resp.msg, "success")

        agent_data = resp.data
        self.assertEqual(agent_data["agent_id"], "agent-123456")
        self.assertEqual(agent_data["agent_name"], "AgentA")
        self.assertEqual(agent_data["call_type"], "outbound")
        self.assertEqual(agent_data["global_settings"]["stt"]["provider_id"], "tencent")
        self.assertEqual(agent_data["global_settings"]["tts"]["voice"], "female")
        self.assertIn("add_wechat", agent_data["global_settings"]["tools"])
        self.assertEqual(
            agent_data["global_settings"]["instruction"],
            "You are a helpful assistant. Answer user questions. {{username}}"
        )

        # 校验数据库落库
        record = await VoiceAgent.filter(agent_id="agent-123456").first()
        self.assertIsNotNone(record)
        self.assertEqual(record.agent_name, "AgentA")
        self.assertEqual(record.instruction, "You are a helpful assistant. Answer user questions. {{username}}")

        # 测试回查接口
        query_resp = await get_voice_agent(agent_identifier="agent-123456")
        self.assertEqual(query_resp.code, 0)
        query_data = query_resp.data
        self.assertEqual(query_data["agent_id"], "agent-123456")
        self.assertEqual(query_data["global_settings"]["llm"]["model_id"], "gemma4-31B")

    async def test_voice_agent_create_auto_generates_id(self):
        """测试不传 agent_id 时服务端自动生成唯一 ID"""
        req = VoiceAgentCreateRequest(
            agent_name="AutoIdAgent",
            instruction="你好，我是自动生成的Agent"
        )
        resp = await create_voice_agent(req)
        self.assertEqual(resp.code, 0)
        self.assertTrue(resp.data["agent_id"].startswith("agent_"))

        # 数据库中可查
        record = await VoiceAgent.filter(agent_id=resp.data["agent_id"]).first()
        self.assertIsNotNone(record)
        self.assertEqual(record.agent_name, "AutoIdAgent")

    async def test_voice_agent_update_existing_id(self):
        """测试传入已存在的 agent_id 时执行覆盖更新"""
        await VoiceAgent.create(
            agent_id="agent-existing-01",
            agent_name="OriginalName",
            instruction="Original instruction",
            version="v1.0"
        )

        req = VoiceAgentCreateRequest(
            agent_id="agent-existing-01",
            agent_name="UpdatedName",
            instruction="Updated instruction content"
        )
        resp = await create_voice_agent(req)
        self.assertEqual(resp.code, 0)

        record = await VoiceAgent.filter(agent_id="agent-existing-01").first()
        self.assertEqual(record.agent_name, "UpdatedName")
        self.assertEqual(record.instruction, "Updated instruction content")

    def test_voice_agent_rejects_empty_instruction(self):
        """测试参数合法性校验：instruction 不能为空"""
        with self.assertRaises(ValidationError):
            VoiceAgentCreateRequest(
                agent_name="NoInstructionAgent",
                instruction="   "
            )

    def test_voice_agent_rejects_invalid_call_type(self):
        """测试呼叫方向必须是 inbound 或 outbound"""
        with self.assertRaises(ValidationError):
            VoiceAgentCreateRequest(
                agent_name="InvalidCallTypeAgent",
                instruction="合法指令",
                call_type="unknown_type"
            )

    # =========================================================================
    # 2. 外呼策略创建接口测试 (对齐文档 2.2 及 7 大订阅事件)
    # =========================================================================

    async def test_strategy_create_with_full_doc_spec(self):
        """测试按照文档 2.2 完整规范创建外呼跟进策略"""
        req = StrategyCreateRequest(
            strategy_id="strategy-1234567",
            schedule_id="agent_driven",
            name="三天通用追呼策略",
            followup=FollowupConfig(
                summary_type="single_call", summary_prompt="recommend-common-interest",
                max_followup_hours=72,
                max_calls=10,
                max_call_per_day=3,
                max_no_answer_calls=3
            ),
            sip=SipConfig(
                accessor_id="accessor-001",
                line_id="line-001",
                mobile_line_id="mobile-line-001"
            ),
            webhook=WebhookSubscription(
                url="https://api.example.com/webhooks/clue-events",
                events=["call_start", "call_end", "call_schedule", "action_wechat", "action_sms", "action_email", "clue_end"]
            )
        )

        resp = await create_strategy(req)
        self.assertEqual(resp.code, 0)
        self.assertEqual(resp.msg, "success")

        strat_data = resp.data
        self.assertEqual(strat_data["strategy_id"], "strategy-1234567")
        self.assertEqual(strat_data["schedule_id"], "agent_driven")
        self.assertEqual(strat_data["followup"]["summary_type"], "single_call")
        self.assertEqual(strat_data["followup"]["summary_prompt"], "recommend-common-interest")
        self.assertEqual(strat_data["followup"]["max_followup_hours"], 72)
        self.assertEqual(strat_data["sip"]["mobile_line_id"], "mobile-line-001")
        self.assertIn("call_start", strat_data["webhook"]["events"])
        self.assertIn("action_email", strat_data["webhook"]["events"])

        # 数据库落库校验
        record = await OutboundStrategy.filter(strategy_id="strategy-1234567").first()
        self.assertIsNotNone(record)
        self.assertEqual(record.followup["max_calls"], 10)

        # 测试回查接口
        query_resp = await get_strategy(strategy_id="strategy-1234567")
        self.assertEqual(query_resp.code, 0)
        query_data = query_resp.data
        self.assertEqual(query_data["strategy_id"], "strategy-1234567")
        self.assertEqual(query_data["sip"]["line_id"], "line-001")

    async def test_strategy_create_auto_generates_id(self):
        """测试不传 strategy_id 时服务端自动生成唯一 ID"""
        req = StrategyCreateRequest(
            schedule_id="agent_driven",
            followup=FollowupConfig(
                summary_type="single_call", summary_prompt="default-summary",
                max_followup_hours=48,
                max_calls=5,
                max_call_per_day=2,
                max_no_answer_calls=2
            )
        )
        resp = await create_strategy(req)
        self.assertEqual(resp.code, 0)
        self.assertTrue(resp.data["strategy_id"].startswith("strategy_"))

        record = await OutboundStrategy.filter(strategy_id=resp.data["strategy_id"]).first()
        self.assertIsNotNone(record)

    async def test_strategy_update_existing_id(self):
        """测试传入已存在的 strategy_id 时执行覆盖更新"""
        await OutboundStrategy.create(
            strategy_id="strat-update-test",
            name="OldName",
            schedule_id="agent_driven",
            followup={"summary_type": "single_call", "summary_prompt": "old_sum", "max_calls": 3}
        )

        req = StrategyCreateRequest(
            strategy_id="strat-update-test",
            name="NewName",
            followup=FollowupConfig(
                summary_type="single_call", summary_prompt="new_sum",
                max_followup_hours=24,
                max_calls=8,
                max_call_per_day=4,
                max_no_answer_calls=2
            )
        )
        resp = await create_strategy(req)
        self.assertEqual(resp.code, 0)

        record = await OutboundStrategy.filter(strategy_id="strat-update-test").first()
        self.assertEqual(record.name, "NewName")
        self.assertEqual(record.followup["max_calls"], 8)

    def test_strategy_rejects_invalid_followup_limits(self):
        """测试参数合法性校验：上限必须大于 0"""
        with self.assertRaises(ValidationError):
            FollowupConfig(
                summary_type="single_call", summary_prompt="test",
                max_calls=0  # 非法：0
            )

    def test_strategy_rejects_invalid_webhook_url(self):
        """测试参数合法性校验：Webhook URL 格式必须以 http 开头"""
        with self.assertRaises(ValidationError):
            WebhookSubscription(
                url="ftp://invalid-url.com",
                events=["call_start"]
            )

    # =========================================================================
    # 3. 用户指定的 7 大可订阅事件规范验证
    # =========================================================================

    def test_subscribable_events_match_exact_user_spec(self):
        """
        验证可订阅事件完全对齐用户要求：
        call_schedule 外呼排期
        call_start 通话开始
        call_end 通话结束
        action_wechat 微信动作
        action_sms 短信动作
        action_email 邮件动作
        clue_end 线索结束
        """
        expected_events = {
            "call_schedule",
            "call_start",
            "call_end",
            "action_wechat",
            "action_sms",
            "action_email",
            "clue_end",
        }
        self.assertEqual(set(SUBSCRIBABLE_EVENTS), expected_events)
        self.assertEqual(SUPPORTED_EVENTS, expected_events)

    def test_event_alias_normalization(self):
        """验证各类历史与别名事件格式自动标准化映射至 7 大标准事件"""
        sub = WebhookSubscription(
            url="https://api.example.com/webhook",
            events=[
                "call-schedule", "call_scheduled",
                "call-start", "call_started",
                "call-end", "call_ended",
                "wechat", "wechat_requested", "action-wechat",
                "sms", "sms_sent", "action-sms",
                "email", "action-email",
                "clue-end", "clue_ended"
            ]
        )
        expected_unique = {
            "call_schedule",
            "call_start",
            "call_end",
            "action_wechat",
            "action_sms",
            "action_email",
            "clue_end",
        }
        self.assertEqual(set(sub.events), expected_unique)

    async def test_webhook_dispatch_filters_unsubscribed_events(self):
        """测试分发系统能够严格按照用户策略订阅的 7 大事件进行白名单过滤"""
        clue_id = "clue_event_filter_01"
        await ClueInfo.create(
            clue_id=clue_id,
            strategy_id="strat_filter_test",
            workflow_id="wf_filter",
            phone_number="13800000000"
        )
        # 仅订阅 call_start 和 action_email
        await OutboundStrategy.create(
            strategy_id="strat_filter_test",
            name="FilterStrategy",
            followup={"summary_type": "single_call", "summary_prompt": "sum1"},
            webhook={
                "url": "https://callback.example.com/events",
                "events": ["call_start", "action_email"]
            }
        )

        with patch("app.apis.webhook_dispatcher._send_http_with_retry", new_callable=AsyncMock) as mock_send, \
             patch("app.observability.events_bus.event_bus.emit"):

            mock_send.return_value = True

            # 1. 触发已订阅的 call_start -> 应当推送
            dispatched = await dispatch_webhook_event(
                event="call_start",
                clue_id=clue_id,
                data={"timestamp": 123456},
                blocking=True
            )
            self.assertTrue(dispatched)
            self.assertEqual(mock_send.call_count, 1)
            self.assertEqual(mock_send.call_args[1]["event"], "call_start")

            mock_send.reset_mock()

            # 2. 触发未订阅的 action_sms -> 应当拦截跳过
            dispatched_sms = await dispatch_webhook_event(
                event="action_sms",
                clue_id=clue_id,
                data={"sms_content": "hello"},
                blocking=True
            )
            self.assertFalse(dispatched_sms)
            mock_send.assert_not_called()

            # 3. 触发已订阅的 action_email -> 应当推送
            dispatched_email = await dispatch_webhook_event(
                event="action_email",
                clue_id=clue_id,
                data={"subject": "test"},
                blocking=True
            )
            self.assertTrue(dispatched_email)
            self.assertEqual(mock_send.call_count, 1)
            self.assertEqual(mock_send.call_args[1]["event"], "action_email")

    # =========================================================================
    # 4. clue_webhook.py 旧逻辑下线与核心挂机回调保留验证
    # =========================================================================

    def test_legacy_inflow_routes_removed_and_schedule_compatibility_retained(self):
        """旧流入入口下线，三个新入口、渠道回调及 In-Call 实时路由已挂载。"""
        all_routes = app.openapi()["paths"]
        self.assertNotIn("/clue/new", all_routes)
        self.assertNotIn("/clue/new/queue", all_routes)
        self.assertNotIn("/clue/schedule/notify", all_routes)

        # 核心通话挂机回调、渠道回调与 In-Call 控制面均正常在注册路由中
        self.assertIn("/clue/create", all_routes)
        self.assertIn("/clue/create/batch", all_routes)
        self.assertIn("/clue/create/file", all_routes)
        self.assertIn("/clue/call-end/notify", all_routes)
        self.assertIn("/clue/channel/notify", all_routes)
        self.assertIn("/clue/in-call/{clue_id}/bootstrap", all_routes)
        self.assertIn("/clue/in-call/{clue_id}/tool-invoke", all_routes)
        self.assertIn("/clue/in-call/{clue_id}/handoff", all_routes)
        self.assertIn("/api/approvals/pending", all_routes)
        self.assertIn("/voice-agent/create", all_routes)
        self.assertIn("/strategy/create", all_routes)

    async def test_telephony_call_end_callback_is_retained(self):
        """验证底层外呼网关通话结束挂机通知接口默认走 Inbox 异步秒回，且保留 sync=True 同步模式"""
        req = CallEndRequest.model_construct(
            data=CallEndData.model_construct(clueId="clue_callback_test"),
            requestId="req_cb_001"
        )
        # 1. 默认异步秒回模式 (sync=False): 入箱即刻返回
        with patch("app.agents.inbox_worker.enqueue_call_end_inbox", new_callable=AsyncMock) as mock_enqueue:
            mock_enqueue.return_value = {"accepted": True, "duplicate": False, "task_id": 101}
            resp = await call_end(req)
            self.assertEqual(resp.code, 0)
            mock_enqueue.assert_called_once_with(req)

        # 2. 兼容同步阻塞模式 (sync=True): 直接调度 on_call_end
        with patch("app.routers.clue_webhook.ClueHandlerFactory.get_handler_by_clue", new_callable=AsyncMock) as mock_get_handler:
            mock_handler = MagicMock()
            mock_handler.on_call_end = AsyncMock()
            mock_get_handler.return_value = mock_handler

            resp_sync = await call_end(req, sync=True)
            self.assertEqual(resp_sync.code, 0)
            self.assertTrue(mock_handler.on_call_end.called)

    # =========================================================================
    # 5. 一站式业务线聚合创建测试 (POST /workflow/bundle/create & POST /workflow/create)
    # =========================================================================

    async def test_workflow_bundle_create_with_full_doc_spec(self):
        """测试一站式聚合创建业务线配置包（同时创建 Agent、策略、建立业务线绑定）"""
        req = WorkflowBundleCreateRequest(
            workflow_id="car_loan_bundle_001",
            name="汽车金融首呼与追呼业务线",
            version="v1.0",
            description="面向车贷线索的自动化AI首呼及3天追呼闭环",
            is_active=True,
            voice_agent=VoiceAgentCreateRequest(
                agent_id="agent-carloan-001",
                agent_name="CarLoanVoiceAgent",
                version="v1.0",
                description="车贷专属客服助手",
                global_settings=VoiceAgentGlobalSettings(
                    stt=SttConfig(
                        provider_id="tencent",
                        hotwords=["车贷", "绿本"],
                        language="zh-CN",
                        vad_silence=240,
                    ),
                    tts=TtsConfig(
                        provider_id="yx",
                        voice="female",
                        language="zh-CN",
                        speed=1.0,
                    ),
                    turn_taking=TurnTakingConfig(provider_id="yx-turn"),
                    knowledge_base={"kb_id": "kb-carloan-qa"},
                    tools=["check_wechat_account", "add_wechat"],
                    llm=LlmConfig(model_id="gemma4-31B"),
                    instruction="你是汽车金融客服，客户姓名是 {{user_name}}，核实用户车贷需求与意向。",
                ),
                agent_type="single_prompt",
                open_message="您好，请问是 {{user_name}} 先生/女士吗？",
                call_type="outbound",
            ),
            strategy=StrategyCreateRequest(
                strategy_id="strategy-carloan-3d",
                schedule_id="agent_driven",
                name="车贷3天排期跟进策略",
                followup=FollowupConfig(
                    summary_type="single_call", summary_prompt="recommend-common-interest",
                    max_followup_hours=72,
                    max_calls=10,
                    max_call_per_day=3,
                    max_no_answer_calls=3,
                ),
                sip=SipConfig(
                    accessor_id="accessor-001",
                    line_id="line-001",
                    mobile_line_id="mobile-line-001",
                ),
                webhook=WebhookSubscription(
                    url="https://api.business.com/webhooks/voice-events",
                    events=[
                        "call_schedule",
                        "call_start",
                        "call_end",
                        "action_wechat",
                        "action_sms",
                        "action_email",
                        "clue_end",
                    ],
                ),
            ),
        )

        resp = await create_workflow_bundle(req)
        self.assertEqual(resp.code, 0)
        self.assertEqual(resp.msg, "success")
        data = resp.data
        self.assertEqual(data["workflow_id"], "car_loan_bundle_001")
        self.assertEqual(data["agent_id"], "agent-carloan-001")
        self.assertEqual(data["strategy_id"], "strategy-carloan-3d")
        self.assertEqual(data["name"], "汽车金融首呼与追呼业务线")
        self.assertEqual(data["is_active"], True)

        # 嵌套 Agent 和 Strategy 详情校验
        self.assertIsNotNone(data["voice_agent"])
        self.assertEqual(data["voice_agent"]["agent_name"], "CarLoanVoiceAgent")
        self.assertIsNotNone(data["strategy"])
        self.assertEqual(data["strategy"]["schedule_id"], "agent_driven")

        # 数据库三张表落库校验
        wf_record = await WorkflowBinding.filter(workflow_id="car_loan_bundle_001").first()
        self.assertIsNotNone(wf_record)
        self.assertEqual(wf_record.agent_id, "agent-carloan-001")
        self.assertEqual(wf_record.strategy_id, "strategy-carloan-3d")

        agent_record = await VoiceAgent.filter(agent_id="agent-carloan-001").first()
        self.assertIsNotNone(agent_record)
        self.assertEqual(agent_record.agent_name, "CarLoanVoiceAgent")

        strat_record = await OutboundStrategy.filter(strategy_id="strategy-carloan-3d").first()
        self.assertIsNotNone(strat_record)
        self.assertEqual(strat_record.name, "车贷3天排期跟进策略")

        # 回查接口验证
        query_resp = await get_workflow(workflow_id="car_loan_bundle_001")
        self.assertEqual(query_resp.code, 0)
        self.assertEqual(query_resp.data["workflow_id"], "car_loan_bundle_001")
        self.assertEqual(query_resp.data["voice_agent"]["agent_name"], "CarLoanVoiceAgent")
        self.assertEqual(query_resp.data["strategy"]["schedule_id"], "agent_driven")

    async def test_workflow_bundle_create_auto_generates_sub_ids(self):
        """测试不传 agent_id 和 strategy_id 时，聚合创建自动生成唯一 ID 并正确绑定"""
        req = WorkflowBundleCreateRequest(
            workflow_id="auto_ids_bundle_01",
            voice_agent=VoiceAgentCreateRequest(
                agent_name="AutoSubAgent",
                instruction="自动生成ID的提示词",
            ),
            strategy=StrategyCreateRequest(
                schedule_id="agent_driven",
                followup=FollowupConfig(
                    summary_type="single_call", summary_prompt="auto-summary",
                    max_followup_hours=24,
                    max_calls=3,
                    max_call_per_day=2,
                    max_no_answer_calls=2,
                ),
            ),
        )

        resp = await create_workflow_bundle(req)
        self.assertEqual(resp.code, 0)
        data = resp.data
        self.assertTrue(data["agent_id"].startswith("agent_"))
        self.assertTrue(data["strategy_id"].startswith("strategy_"))

        wf_record = await WorkflowBinding.filter(workflow_id="auto_ids_bundle_01").first()
        self.assertIsNotNone(wf_record)
        self.assertEqual(wf_record.agent_id, data["agent_id"])
        self.assertEqual(wf_record.strategy_id, data["strategy_id"])

    async def test_workflow_bundle_create_reusing_existing_agent_and_strategy(self):
        """测试外部仅传入已存在的 agent_id 和 strategy_id（复用既有能力）进行绑定"""
        await VoiceAgent.create(
            agent_id="agent-reuse-001",
            agent_name="ReusedAgent",
            instruction="已有客服",
        )
        await OutboundStrategy.create(
            strategy_id="strategy-reuse-001",
            name="ReusedStrategy",
            schedule_id="agent_driven",
            followup={"summary_type": "single_call", "summary_prompt": "sum1"},
        )

        req = WorkflowBundleCreateRequest(
            workflow_id="bundle_reused_wf",
            agent_id="agent-reuse-001",
            strategy_id="strategy-reuse-001",
            name="复用业务线",
        )

        resp = await create_workflow_bundle(req)
        self.assertEqual(resp.code, 0)
        self.assertEqual(resp.data["agent_id"], "agent-reuse-001")
        self.assertEqual(resp.data["strategy_id"], "strategy-reuse-001")
        self.assertEqual(resp.data["voice_agent"]["agent_name"], "ReusedAgent")
        self.assertEqual(resp.data["strategy"]["name"], "ReusedStrategy")

    async def test_workflow_bundle_create_hybrid_existing_agent_and_new_strategy(self):
        """测试混合模式：复用已有的 agent_id，同时现场新建 strategy 策略"""
        await VoiceAgent.create(
            agent_id="agent-hybrid-01",
            agent_name="ExistingAgent",
            instruction="已有话术指令",
        )

        req = WorkflowBundleCreateRequest(
            workflow_id="bundle_hybrid_wf",
            agent_id="agent-hybrid-01",
            strategy=StrategyCreateRequest(
                strategy_id="strat-hybrid-new",
                schedule_id="agent_driven",
                followup=FollowupConfig(
                    summary_type="single_call", summary_prompt="hybrid-summary",
                    max_followup_hours=48,
                    max_calls=5,
                    max_call_per_day=2,
                    max_no_answer_calls=2,
                ),
            ),
        )

        resp = await create_workflow_bundle(req)
        self.assertEqual(resp.code, 0)
        self.assertEqual(resp.data["agent_id"], "agent-hybrid-01")
        self.assertEqual(resp.data["strategy_id"], "strat-hybrid-new")
        self.assertEqual(resp.data["voice_agent"]["agent_name"], "ExistingAgent")

        strat_db = await OutboundStrategy.filter(strategy_id="strat-hybrid-new").first()
        self.assertIsNotNone(strat_db)

    async def test_workflow_bundle_transaction_rollback_on_failure(self):
        """测试事务原子性保障：如果策略或绑定环节发生异常，前面已处理的 VoiceAgent 自动回滚"""
        payload = {
            "workflow_id": "wf_rollback_test",
            "voice_agent": {
                "agent_id": "agent-rollback-target",
                "agent_name": "RollbackAgent",
                "instruction": "事务回滚测试指令",
            },
            # 指定一个不存在的 strategy_id，模拟事务中间抛错
            "strategy_id": "non_existent_strategy_id",
        }

        with self.assertRaises(ValueError) as ctx:
            await provision_workflow_bundle(payload)
        self.assertIn("此workflow_id业务线没有对应的agent可以配套", str(ctx.exception))

        # 验证事务已回滚：前面写入的 Agent 不应残留在数据库中
        agent_in_db = await VoiceAgent.filter(agent_id="agent-rollback-target").first()
        self.assertIsNone(agent_in_db)

        # WorkflowBinding 也不应写入
        wf_in_db = await WorkflowBinding.filter(workflow_id="wf_rollback_test").first()
        self.assertIsNone(wf_in_db)

    async def test_workflow_create_endpoint_unpaired_agent_returns_code_3(self):
        """测试创建 Workflow 绑定时，若未提供配置且指定的 agent_id / strategy_id 不存在，返回 code: 3"""
        req = WorkflowCreateRequest(
            workflow_id="wf_unpaired_test",
            name="未配套测试",
            agent_id="non_existent_agent_999",
            strategy_id="non_existent_strat_999",
        )
        resp = await create_workflow(req)
        self.assertEqual(resp.code, 3)
        self.assertEqual(resp.msg, "此workflow_id业务线没有对应的agent可以配套")
        self.assertIsNone(resp.data)

    async def test_workflow_bundle_create_unpaired_returns_code_3(self):
        """测试一站式创建 WorkflowBundle 时，若策略未配套返回 code: 3"""
        req = WorkflowBundleCreateRequest(
            workflow_id="wf_bundle_unpaired",
            voice_agent=VoiceAgentCreateRequest(
                agent_id="agent-bundle-ok",
                agent_name="BundleAgent",
                instruction="有效指令",
            ),
            strategy_id="non_existent_strat_999",
        )
        resp = await create_workflow_bundle(req)
        self.assertEqual(resp.code, 3)
        self.assertEqual(resp.msg, "此workflow_id业务线没有对应的agent可以配套")
        self.assertIsNone(resp.data)

    async def test_workflow_create_endpoint_polymorphism(self):
        """测试主干接口 POST /workflow/create 多态支持一站式聚合请求"""
        req = WorkflowCreateRequest(
            workflow_id="wf_polymorphic_test",
            name="多态接口测试",
            voice_agent=VoiceAgentCreateRequest(
                agent_id="agent-poly-01",
                agent_name="PolyAgent",
                instruction="多态指令",
            ),
            strategy=StrategyCreateRequest(
                strategy_id="strat-poly-01",
                schedule_id="agent_driven",
                followup=FollowupConfig(
                    summary_type="single_call", summary_prompt="poly-sum",
                    max_followup_hours=24,
                    max_calls=4,
                    max_call_per_day=2,
                    max_no_answer_calls=2,
                ),
            ),
        )

        resp = await create_workflow(req)
        self.assertEqual(resp.code, 0)
        self.assertEqual(resp.data["workflow_id"], "wf_polymorphic_test")
        self.assertEqual(resp.data["agent_id"], "agent-poly-01")
        self.assertEqual(resp.data["strategy_id"], "strat-poly-01")

        # 数据库确认
        wf = await WorkflowBinding.filter(workflow_id="wf_polymorphic_test").first()
        self.assertIsNotNone(wf)

    def test_workflow_bundle_validation_error_missing_both(self):
        """测试请求体校验：缺少 agent 来源或 strategy 来源时直接抛出 ValidationError"""
        with self.assertRaises(ValidationError):
            WorkflowBundleCreateRequest(
                workflow_id="invalid_wf",
                # 未提供 voice_agent 且未提供 agent_id
                strategy_id="some_strat_id",
            )

        with self.assertRaises(ValidationError):
            WorkflowBundleCreateRequest(
                workflow_id="invalid_wf",
                agent_id="some_agent_id",
                # 未提供 strategy 且未提供 strategy_id
            )

    def test_workflow_bundle_endpoints_registered_in_app(self):
        """验证聚合创建接口已成功挂载至全局 FastAPI 路由表"""
        all_routes = app.openapi()["paths"]
        self.assertIn("/workflow/bundle/create", all_routes)
        self.assertIn("/workflow/create", all_routes)
        self.assertIn("/workflow/{workflow_id}", all_routes)


if __name__ == "__main__":
    unittest.main()
