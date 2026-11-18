import asyncio
import json
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

from tortoise import Tortoise

from app.clue_handler.factory import ClueHandlerFactory
from app.clue_handler.stages.postcall import DefaultPostCallHandler as PostCallHandler, extract_conversation_messages
from app.clue_handler.state import ClueState
from app.dbmodels.models import CallRecord, ClueInfo, ExecutionTask, OutboundStrategy, WorkflowBinding
from app.observability.events_bus import ClueEndEvent
from app.strategy.single_call.config import SingleCallStrategyConfig
from app.strategy.single_call.engine import SingleCallStrategyEngine
from app.summary.contracts import ConversationMessage, SummaryResult
from app.summary.factory import SummaryEngine, get_summary_engine

CST = ZoneInfo("Asia/Shanghai")


class DummyLock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class TestSummaryAndSingleCallIntegration(unittest.IsolatedAsyncioTestCase):
    """
    业务线 Workflow、Summary 总结模块与 SingleCall 单通外呼策略集成测试
    """

    async def asyncSetUp(self):
        # 初始化 Tortoise 内存数据库
        await Tortoise.init(
            db_url="sqlite://:memory:",
            modules={"models": ["app.dbmodels.models"]},
        )
        await Tortoise.generate_schemas()
        ClueHandlerFactory.invalidate_all()

        # Mock Redis 分布式锁，避免测试依赖真实 Redis 集群
        self.lock_patcher = patch(
            "app.redis_client.redis_manager.redis_manager.get_lock",
            return_value=DummyLock(),
        )
        self.lock_patcher.start()

        class DummyCallQueue:
            async def publish(self, *args, **kwargs):
                return True

        self.queue_patcher = patch(
            "app.call_schedule.service.get_call_queue",
            return_value=DummyCallQueue(),
        )
        self.queue_patcher.start()

    async def asyncTearDown(self):
        self.queue_patcher.stop()
        self.lock_patcher.stop()
        await Tortoise.close_connections()
        ClueHandlerFactory.invalidate_all()

    def test_extract_conversation_messages(self):
        """测试对话列表清洗：正确解析 robot/assistant 与 user 角色，过滤空消息和内部指令"""
        raw_context = [
            {"sender": "robot", "text": "您好，请问是张三先生吗？"},
            {"sender": "user", "text": "是的，您哪位？"},
            {"sender": "robot", "text": "Call the wecomServer_addContact tool..."},
            {"sender": "robot", "text": "我是淘车顾问小李。"},
            {"sender": "robot", "text": "   "},
            {"role": "user", "content": "好的，请讲。"},
        ]
        messages = extract_conversation_messages(raw_context)
        self.assertEqual(len(messages), 4)
        self.assertEqual(messages[0], {"role": "assistant", "content": "您好，请问是张三先生吗？"})
        self.assertEqual(messages[1], {"role": "user", "content": "是的，您哪位？"})
        self.assertEqual(messages[2], {"role": "assistant", "content": "我是淘车顾问小李。"})
        self.assertEqual(messages[3], {"role": "user", "content": "好的，请讲。"})

    async def test_clue_handler_factory_injects_summary_and_strategy(self):
        """测试通过 workflow_id 装配时，工厂自动识别 single_call 与 followup.summary_type 并注入 PostCallHandler"""
        wf_id = "wf_single_summary_test"
        strat_id = "strat_single_001"

        # 写入 OutboundStrategy 与 WorkflowBinding
        await OutboundStrategy.create(
            strategy_id=strat_id,
            schedule_id="single_call",
            followup={
                "summary_type": "car_loan_single_summary",
                "summary_prompt": "请提取贷款意向车型及金额",
                "max_calls": 2,
            },
        )
        await WorkflowBinding.create(
            workflow_id=wf_id,
            name="单通外呼与总结测试流",
            agent_id="agent_voice_001",
            strategy_id=strat_id,
            is_active=True,
        )

        handler = await ClueHandlerFactory.get_handler_by_workflow(wf_id)
        self.assertIsNotNone(handler)
        self.assertIsNotNone(handler.postcall_handler)

        postcall = handler.postcall_handler
        self.assertIsInstance(postcall.strategy_engine, SingleCallStrategyEngine)
        self.assertEqual(postcall.summary_type, "car_loan_single_summary")
        self.assertEqual(postcall.summary_prompt, "请提取贷款意向车型及金额")
        self.assertIsNotNone(postcall.summary_engine)

    @patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue")
    @patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock)
    @patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock)
    async def test_single_call_connected_real_human_ends_clue_immediately(
        self,
        mock_dispatch_webhook,
        mock_event_emit,
        mock_delayed_queue,
    ):
        """测试单通策略真人接通场景：由 Summary 告知未转信箱且打通，达成目标即刻结案 (200)，不再排期"""
        mock_dq = MagicMock()
        mock_dq.remove_task = AsyncMock()
        mock_dq.add_task = AsyncMock()
        mock_delayed_queue.return_value = mock_dq

        clue_id = "clue_real_human_01"
        wf_id = "wf_single_human"
        strat_id = "strat_single_human"

        await OutboundStrategy.create(
            strategy_id=strat_id,
            schedule_id="single_call",
            followup={"summary_type": "standard_summary"},
        )
        await WorkflowBinding.create(
            workflow_id=wf_id,
            name="真人接通流",
            agent_id="agent_01",
            strategy_id=strat_id,
            is_active=True,
        )
        await ClueInfo.create(
            clue_id=clue_id,
            workflow_id=wf_id,
            strategy_id=strat_id,
            status=ClueState.SUMMARY.value,
            total_call_count=1,
        )

        # 构造真人对话上下文
        dialog = [
            {"sender": "robot", "text": "您好，请问最近有资金周转需求吗？"},
            {"sender": "user", "text": "有的，你们利息多少？"},
            {"sender": "robot", "text": "年化4%起，今天方便为您测算额度吗？"},
            {"sender": "user", "text": "可以的，加我微信吧。"},
        ]
        await CallRecord.create(
            call_id="call_human_01",
            clue_id=clue_id,
            agent_id="agent_01",
            strategy_id=strat_id,
            attempt_no=1,
            status="ended",
            answered=1,
            config_snapshot={"aiCallContextList": dialog},
        )

        # Mock SummaryEngine 返回真人分析结果
        mock_summary_engine = MagicMock(spec=SummaryEngine)
        mock_summary_engine.analyze_call = AsyncMock(return_value=SummaryResult(
            summary_type="standard_summary",
            summary_version="1",
            common_call_events={
                "is_voicemail": "no",
                "customer_hostility_opt_out": "no",
                "require_human_follow_up": "no",
                "has_next_call_appointment": "no",
                "call_summary": "客户有周转需求同意测算",
            },
            business_result={},
            prompt_result=None,
            decision="success",
            reason="真人已接通，无敏感情绪，结束线索",
        ))

        strategy_engine = SingleCallStrategyEngine(SingleCallStrategyConfig(max_call_num=3, noanswer_num=3))
        handler = PostCallHandler(
            strategy_engine=strategy_engine,
            summary_engine=mock_summary_engine,
            summary_type="standard_summary",
        )

        result = await handler.handle(clue_id, "req_sum_001")
        self.assertTrue(result)

        # 1. 验证总结引擎被调用且传入 summary_type，且未配置 summary_prompt 时不传 summary_prompt
        mock_summary_engine.analyze_call.assert_called_once()
        call_kwargs = mock_summary_engine.analyze_call.call_args[1]
        self.assertEqual(call_kwargs.get("summary_type"), "standard_summary")
        self.assertIsNone(call_kwargs.get("summary_prompt"))

        # 2. 验证 CallRecord 更新
        updated_call = await CallRecord.filter(call_id="call_human_01").first()
        self.assertEqual(updated_call.summary_status, "succeeded")
        self.assertEqual(updated_call.common_call_events["is_voicemail"], "no")

        # 3. 验证线索状态直接流转至 ACCEPTED 终态，并记录结案原因
        updated_clue = await ClueInfo.filter(clue_id=clue_id).first()
        self.assertEqual(updated_clue.status, ClueState.ACCEPTED.value)
        self.assertEqual(updated_clue.close_reason, "真人已接通，无敏感情绪，结束线索")

        # 4. 验证发射 ClueEndEvent (code=200)
        clue_end_events = [c[0][0] for c in mock_event_emit.call_args_list if isinstance(c[0][0], ClueEndEvent)]
        self.assertEqual(len(clue_end_events), 1)
        event: ClueEndEvent = clue_end_events[0]
        self.assertEqual(event.clue_id, clue_id)
        self.assertEqual(event.workflow_id, wf_id)
        self.assertEqual(event.event_data["code"], 200)

        # 5. 验证绝对未安排下一轮外呼 (单通已达成目标)
        added_schedule_tasks = [
            call for call in mock_dq.add_task.call_args_list if "schedule:" in str(call)
        ]
        self.assertEqual(len(added_schedule_tasks), 0)

    @patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue")
    @patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock)
    @patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock)
    async def test_single_call_voicemail_schedules_retry(
        self,
        mock_dispatch_webhook,
        mock_event_emit,
        mock_delayed_queue,
    ):
        """测试单通策略遇到语音信箱：Summary 告知未真正打通，交策略引擎计算并安排追呼重试"""
        mock_dq = MagicMock()
        mock_dq.remove_task = AsyncMock()
        mock_dq.add_task = AsyncMock()
        mock_delayed_queue.return_value = mock_dq

        clue_id = "clue_voicemail_01"
        wf_id = "wf_single_vm"
        strat_id = "strat_single_vm"

        await OutboundStrategy.create(
            strategy_id=strat_id,
            schedule_id="single_call",
            followup={"summary_type": "standard_summary"},
        )
        await WorkflowBinding.create(
            workflow_id=wf_id,
            name="语音信箱重试流",
            agent_id="agent_01",
            strategy_id=strat_id,
            is_active=True,
        )
        await ClueInfo.create(
            clue_id=clue_id,
            workflow_id=wf_id,
            strategy_id=strat_id,
            status=ClueState.SUMMARY.value,
            total_call_count=1,
            no_answer_count=1,
        )

        # 构造语音信箱对话
        dialog = [
            {"sender": "robot", "text": "您好，请问是李四先生吗？"},
            {"sender": "user", "text": "您好，机主现在无法接听电话，请在嘟声后留言..."},
        ]
        await CallRecord.create(
            call_id="call_vm_01",
            clue_id=clue_id,
            agent_id="agent_01",
            strategy_id=strat_id,
            attempt_no=1,
            status="ended",
            answered=1,  # 话务平台显示接通，但实际上是语音信箱
            config_snapshot={"aiCallContextList": dialog},
        )

        mock_summary_engine = MagicMock(spec=SummaryEngine)
        mock_summary_engine.analyze_call = AsyncMock(return_value=SummaryResult(
            summary_type="standard_summary",
            summary_version="1",
            common_call_events={
                "is_voicemail": "yes",  # Summary 明确识别为语音信箱
                "customer_hostility_opt_out": "no",
                "require_human_follow_up": "no",
                "has_next_call_appointment": "no",
                "call_summary": "语音信箱代接",
            },
            business_result={},
            prompt_result=None,
            decision="schedule",
            reason="非真人有效接听",
        ))

        # 策略允许最多重试 3 次，当前仅第 1 次未真正打通
        strategy_engine = SingleCallStrategyEngine(
            SingleCallStrategyConfig(max_call_num=3, noanswer_num=3, retry_interval_minutes=30)
        )
        handler = PostCallHandler(
            strategy_engine=strategy_engine,
            summary_engine=mock_summary_engine,
            summary_type="standard_summary",
        )

        result = await handler.handle(clue_id, "req_sum_002")
        self.assertTrue(result)

        # 1. 验证线索状态切入 SCHEDULED，未结案
        updated_clue = await ClueInfo.filter(clue_id=clue_id).first()
        self.assertEqual(updated_clue.status, ClueState.SCHEDULED.value)

        # 2. 验证排期任务已持久化生成 ExecutionTask
        task = await ExecutionTask.filter(task_type="call", clue_id=clue_id).first()
        self.assertIsNotNone(task)
        self.assertEqual(task.status, "pending")

        # 3. 验证未触发 ClueEndEvent 结案事件
        clue_end_events = [c[0][0] for c in mock_event_emit.call_args_list if isinstance(c[0][0], ClueEndEvent)]
        self.assertEqual(len(clue_end_events), 0)

        # 4. 验证 call_scheduled Webhook 触发
        scheduled_dispatches = [
            call for call in mock_dispatch_webhook.call_args_list if call[1].get("event") == "call_scheduled"
        ]
        self.assertEqual(len(scheduled_dispatches), 1)

    @patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue")
    @patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock)
    @patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock)
    async def test_single_call_unanswered_reaches_retry_limit_terminates_clue(
        self,
        mock_dispatch_webhook,
        mock_event_emit,
        mock_delayed_queue,
    ):
        """测试单通策略未接通达到上限：熔断结案 (300)"""
        mock_dq = MagicMock()
        mock_dq.remove_task = AsyncMock()
        mock_delayed_queue.return_value = mock_dq

        clue_id = "clue_limit_01"
        wf_id = "wf_single_limit"
        strat_id = "strat_single_limit"

        await OutboundStrategy.create(
            strategy_id=strat_id,
            schedule_id="single_call",
            followup={"summary_type": "standard_summary"},
        )
        await WorkflowBinding.create(
            workflow_id=wf_id,
            name="单通上限结案流",
            agent_id="agent_01",
            strategy_id=strat_id,
            is_active=True,
        )
        # 已呼叫 1 次且配置最多呼 1 次
        await ClueInfo.create(
            clue_id=clue_id,
            workflow_id=wf_id,
            strategy_id=strat_id,
            status=ClueState.SUMMARY.value,
            total_call_count=1,
            no_answer_count=1,
        )

        await CallRecord.create(
            call_id="call_limit_01",
            clue_id=clue_id,
            agent_id="agent_01",
            strategy_id=strat_id,
            attempt_no=1,
            status="ended",
            answered=0,  # 未接通
            config_snapshot={"aiCallContextList": []},
        )

        strategy_engine = SingleCallStrategyEngine(
            SingleCallStrategyConfig(max_call_num=1, noanswer_num=1)
        )
        handler = PostCallHandler(
            strategy_engine=strategy_engine,
            summary_engine=None,
            summary_type="standard_summary",
        )

        result = await handler.handle(clue_id, "req_sum_003")
        self.assertTrue(result)

        # 验证结案为 REJECTED (300)
        updated_clue = await ClueInfo.filter(clue_id=clue_id).first()
        self.assertEqual(updated_clue.status, ClueState.REJECTED.value)

        clue_end_events = [c[0][0] for c in mock_event_emit.call_args_list if isinstance(c[0][0], ClueEndEvent)]
        self.assertEqual(len(clue_end_events), 1)
        event: ClueEndEvent = clue_end_events[0]
        self.assertEqual(event.event_data["code"], 300)

    @patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue")
    @patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock)
    @patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock)
    async def test_customer_appointment_in_summary_respected(
        self,
        mock_dispatch_webhook,
        mock_event_emit,
        mock_delayed_queue,
    ):
        """测试 Summary 抽取到客户预约时间时，正确透传给策略引擎按客户预约排期"""
        mock_dq = MagicMock()
        mock_dq.remove_task = AsyncMock()
        mock_dq.add_task = AsyncMock()
        mock_delayed_queue.return_value = mock_dq

        clue_id = "clue_appt_01"
        wf_id = "wf_appt"
        strat_id = "strat_appt"

        await OutboundStrategy.create(
            strategy_id=strat_id,
            schedule_id="seven_day_three_call",
            followup={"summary_type": "standard_summary"},
        )
        await WorkflowBinding.create(
            workflow_id=wf_id,
            name="客户预约流",
            agent_id="agent_01",
            strategy_id=strat_id,
            is_active=True,
        )
        await ClueInfo.create(
            clue_id=clue_id,
            workflow_id=wf_id,
            strategy_id=strat_id,
            status=ClueState.SUMMARY.value,
            total_call_count=1,
        )

        dialog = [
            {"sender": "robot", "text": "您好，请问现在方便沟通吗？"},
            {"sender": "user", "text": "现在开会，明天下午四点再打给我。"},
        ]
        await CallRecord.create(
            call_id="call_appt_01",
            clue_id=clue_id,
            agent_id="agent_01",
            strategy_id=strat_id,
            attempt_no=1,
            status="ended",
            answered=1,
            config_snapshot={"aiCallContextList": dialog},
        )

        # 预约明天工作时间 16:00
        mock_summary_engine = MagicMock(spec=SummaryEngine)
        mock_summary_engine.analyze_call = AsyncMock(return_value=SummaryResult(
            summary_type="standard_summary",
            summary_version="1",
            common_call_events={
                "is_voicemail": "no",
                "customer_hostility_opt_out": "no",
                "require_human_follow_up": "no",
                "has_next_call_appointment": "yes",
                "next_call_time": "2026-10-21 16:00",
                "call_summary": "客户预约明天下午四点沟通",
            },
            business_result={},
            prompt_result=None,
            decision="schedule",
            reason="客户预约明天下午四点沟通",
        ))

        handler = PostCallHandler(
            summary_engine=mock_summary_engine,
            summary_type="standard_summary",
        )

        result = await handler.handle(clue_id, "req_sum_004")
        self.assertTrue(result)

        updated_clue = await ClueInfo.filter(clue_id=clue_id).first()
        self.assertEqual(updated_clue.status, ClueState.SCHEDULED.value)

        # 检查排期任务已持久化生成 ExecutionTask，且按预约时间排期
        task = await ExecutionTask.filter(task_type="call", clue_id=clue_id).first()
        self.assertIsNotNone(task)
        self.assertEqual(task.status, "pending")
        self.assertEqual(task.run_at.astimezone(CST).strftime("%Y-%m-%d %H:%M"), "2026-10-21 16:00")

    @patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue")
    @patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock)
    @patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock)
    async def test_summary_type_and_summary_prompt_passed_independently(
        self,
        mock_dispatch_webhook,
        mock_event_emit,
        mock_delayed_queue,
    ):
        """测试 summary_type (总结套件版本) 与 summary_prompt (场景定制业务提示词) 独立传递，互不混淆"""
        mock_delayed_queue.return_value.remove_task = AsyncMock()
        clue_id = "clue_dual_param_01"
        wf_id = "wf_dual_param"
        strat_id = "strat_dual_param"

        await OutboundStrategy.create(
            strategy_id=strat_id,
            schedule_id="single_call",
            followup={
                "summary_type": "poly-sum-v2",
                "summary_prompt": "请提炼客户意向车型与首付预算",
            },
        )
        await WorkflowBinding.create(
            workflow_id=wf_id,
            name="双参数测试流",
            agent_id="agent_01",
            strategy_id=strat_id,
            is_active=True,
        )
        await ClueInfo.create(
            clue_id=clue_id,
            workflow_id=wf_id,
            strategy_id=strat_id,
            status=ClueState.SUMMARY.value,
            total_call_count=1,
        )

        dialog = [
            {"sender": "robot", "text": "您好，想买什么车呢？"},
            {"sender": "user", "text": "看中了比亚迪汉，准备首付五成。"},
        ]
        await CallRecord.create(
            call_id="call_dual_01",
            clue_id=clue_id,
            agent_id="agent_01",
            strategy_id=strat_id,
            attempt_no=1,
            status="ended",
            answered=1,
            config_snapshot={"aiCallContextList": dialog},
        )

        mock_summary_engine = MagicMock(spec=SummaryEngine)
        mock_summary_engine.analyze_call = AsyncMock(return_value=SummaryResult(
            summary_type="poly-sum-v2",
            summary_version="1",
            common_call_events={
                "is_voicemail": "no",
                "call_summary": "客户想买比亚迪汉首付五成",
            },
            business_result={},
            prompt_result={
                "summary": "意向车型比亚迪汉，首付50%",
            },
            decision="success",
            reason="真人已接通，无敏感情绪，结束线索",
        ))

        handler = await ClueHandlerFactory.get_handler_by_workflow(wf_id)
        # 替换为 mock summary engine
        handler.postcall_handler.summary_engine = mock_summary_engine

        result = await handler.postcall_handler.handle(clue_id, "req_dual_001")
        self.assertTrue(result)

        mock_summary_engine.analyze_call.assert_called_once()
        call_kwargs = mock_summary_engine.analyze_call.call_args[1]

        # 严格验证两个参数一一对应，绝不混淆
        self.assertEqual(call_kwargs.get("summary_type"), "poly-sum-v2")
        self.assertEqual(call_kwargs.get("summary_prompt"), "请提炼客户意向车型与首付预算")
        self.assertNotEqual(call_kwargs.get("summary_type"), call_kwargs.get("summary_prompt"))

    @patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue")
    @patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock)
    @patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock)
    async def test_clue_snapshot_summary_prompt_override_takes_precedence(
        self,
        mock_dispatch_webhook,
        mock_event_emit,
        mock_delayed_queue,
    ):
        """测试通话快照中带有的定制业务 summary_prompt 能够动态生效，且保持 summary_type 不变"""
        mock_delayed_queue.return_value.remove_task = AsyncMock()
        clue_id = "clue_override_01"
        wf_id = "wf_override"
        strat_id = "strat_override"

        await OutboundStrategy.create(
            strategy_id=strat_id,
            schedule_id="single_call",
            followup={
                "summary_type": "standard_single",
                "summary_prompt": "默认策略提示词",
            },
        )
        await WorkflowBinding.create(
            workflow_id=wf_id,
            name="覆盖提示词测试流",
            agent_id="agent_01",
            strategy_id=strat_id,
            is_active=True,
        )
        await ClueInfo.create(
            clue_id=clue_id,
            workflow_id=wf_id,
            strategy_id=strat_id,
            status=ClueState.SUMMARY.value,
            total_call_count=1,
        )

        dialog = [
            {"sender": "robot", "text": "您好！"},
            {"sender": "user", "text": "你好！"},
        ]
        # 快照中带上了单条线索覆盖的业务提示词
        await CallRecord.create(
            call_id="call_override_01",
            clue_id=clue_id,
            agent_id="agent_01",
            strategy_id=strat_id,
            attempt_no=1,
            status="ended",
            answered=1,
            config_snapshot={
                "aiCallContextList": dialog,
                "summary_prompt": "线索专属的高优先级业务提取提示词",
            },
        )

        mock_summary_engine = MagicMock(spec=SummaryEngine)
        mock_summary_engine.analyze_call = AsyncMock(return_value=SummaryResult(
            summary_type="standard_single",
            summary_version="1",
            common_call_events={
                "is_voicemail": "no",
                "call_summary": "客户确认打招呼",
            },
            business_result={},
            prompt_result={"summary": "线索专属的高优先级业务提取提示词"},
            decision="success",
            reason="真人已接通，无敏感情绪，结束线索",
        ))

        handler = await ClueHandlerFactory.get_handler_by_workflow(wf_id)
        handler.postcall_handler.summary_engine = mock_summary_engine

        result = await handler.postcall_handler.handle(clue_id, "req_override_001")
        self.assertTrue(result)

        mock_summary_engine.analyze_call.assert_called_once()
        call_kwargs = mock_summary_engine.analyze_call.call_args[1]

        # 验证 summary_type 保持为策略绑定的版本，summary_prompt 正确取线索快照的业务 prompt
        self.assertEqual(call_kwargs.get("summary_type"), "standard_single")
        self.assertEqual(call_kwargs.get("summary_prompt"), "线索专属的高优先级业务提取提示词")

    @patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue")
    @patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock)
    @patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock)
    async def test_single_call_hostility_reject_ends_clue_with_code_300(
        self,
        mock_dispatch_webhook,
        mock_event_emit,
        mock_delayed_queue,
    ):
        """测试单通外呼用户明确拒绝/敏感情绪：Summary 判定 decision='reject'，线索立即结案终止 (300)"""
        mock_delayed_queue.return_value.remove_task = AsyncMock()
        clue_id = "clue_reject_01"
        wf_id = "wf_reject"
        strat_id = "strat_reject"

        await OutboundStrategy.create(
            strategy_id=strat_id,
            schedule_id="single_call",
            followup={"summary_type": "single_call"},
        )
        await WorkflowBinding.create(
            workflow_id=wf_id,
            name="拒绝结案测试流",
            agent_id="agent_01",
            strategy_id=strat_id,
            is_active=True,
        )
        await ClueInfo.create(
            clue_id=clue_id,
            workflow_id=wf_id,
            strategy_id=strat_id,
            status=ClueState.SUMMARY.value,
            total_call_count=1,
        )
        await CallRecord.create(
            call_id="call_reject_01",
            clue_id=clue_id,
            agent_id="agent_01",
            strategy_id=strat_id,
            attempt_no=1,
            status="ended",
            answered=1,
            config_snapshot={"aiCallContextList": [{"sender": "user", "text": "别再打了，烦死了！"}]},
        )

        mock_summary_engine = MagicMock(spec=SummaryEngine)
        mock_summary_engine.analyze_call = AsyncMock(return_value=SummaryResult(
            summary_type="single_call",
            summary_version="1",
            common_call_events={"is_voicemail": "no", "customer_hostility_opt_out": "yes", "call_summary": "用户明确拒绝"},
            business_result={},
            prompt_result=None,
            decision="reject",
            reason="用户敏感情绪或明确拒绝再联系",
        ))

        handler = await ClueHandlerFactory.get_handler_by_workflow(wf_id)
        handler.postcall_handler.summary_engine = mock_summary_engine

        result = await handler.postcall_handler.handle(clue_id, "req_reject_001")
        self.assertTrue(result)

        updated_clue = await ClueInfo.filter(clue_id=clue_id).first()
        self.assertEqual(updated_clue.status, ClueState.REJECTED.value)
        self.assertEqual(updated_clue.close_reason, "用户敏感情绪或明确拒绝再联系")

        clue_end_events = [c[0][0] for c in mock_event_emit.call_args_list if isinstance(c[0][0], ClueEndEvent)]
        self.assertEqual(len(clue_end_events), 1)
        self.assertEqual(clue_end_events[0].event_data["code"], 300)

    @patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue")
    @patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock)
    @patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock)
    async def test_legacy_analyze_conversation_fallback_when_analyze_call_unavailable(
        self,
        mock_dispatch_webhook,
        mock_event_emit,
        mock_delayed_queue,
    ):
        """测试老版本总结引擎无 analyze_call 时，优雅平滑降级调用 analyze_conversation"""
        mock_delayed_queue.return_value.remove_task = AsyncMock()
        clue_id = "clue_legacy_01"
        wf_id = "wf_legacy"
        strat_id = "strat_legacy"

        await OutboundStrategy.create(
            strategy_id=strat_id,
            schedule_id="single_call",
            followup={"summary_type": "legacy_summary"},
        )
        await WorkflowBinding.create(
            workflow_id=wf_id,
            name="老版本降级测试流",
            agent_id="agent_01",
            strategy_id=strat_id,
            is_active=True,
        )
        await ClueInfo.create(
            clue_id=clue_id,
            workflow_id=wf_id,
            strategy_id=strat_id,
            status=ClueState.SUMMARY.value,
            total_call_count=1,
        )
        await CallRecord.create(
            call_id="call_legacy_01",
            clue_id=clue_id,
            agent_id="agent_01",
            strategy_id=strat_id,
            attempt_no=1,
            status="ended",
            answered=1,
            config_snapshot={"aiCallContextList": [{"sender": "robot", "text": "你好"}, {"sender": "user", "text": "你好"}]},
        )

        # 仅有 analyze_conversation 的老引擎
        legacy_engine = MagicMock()
        del legacy_engine.analyze_call
        legacy_engine.analyze_conversation = AsyncMock(return_value={
            "common_call_events": {
                "is_voicemail": "no",
                "call_summary": "降级成功",
            }
        })

        handler = await ClueHandlerFactory.get_handler_by_workflow(wf_id)
        handler.postcall_handler.summary_engine = legacy_engine

        result = await handler.postcall_handler.handle(clue_id, "req_legacy_001")
        self.assertTrue(result)
        legacy_engine.analyze_conversation.assert_called_once()


if __name__ == "__main__":
    unittest.main()
