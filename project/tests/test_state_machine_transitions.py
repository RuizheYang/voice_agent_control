import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from tortoise import Tortoise

from app.clue_handler.state import ClueState, VALID_TRANSITIONS
from app.clue_handler.stages import (
    DefaultInflowHandler as NewClueHandler,
    DefaultScheduleHandler as ScheduleHandler,
    DefaultPostCallHandler as PostCallHandler,
    DefaultCancelHandler as CancelCallHandler,
)
from app.clue_handler.clue import ClueHandler
from app.dbmodels import crud
from app.dbmodels.models import ClueInfo, CallRecord, OutboundStrategy
from app.schemas.clue_create import NormalizedClue
from app.call_schedule.contracts import ScheduledCallRequest
from app.schemas.call_end import CallEndRequest, CallEndData, CallEndCurrentCall, CallEndOutboundReport
from app.strategy.contracts import ScheduleResult
from app.strategy.single_call.engine import SingleCallStrategyEngine
from app.utils.exceptions import StateTransitionError


class DummyLock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class DummyDelayedQueue:
    async def add_task(self, *args, **kwargs):
        pass

    async def remove_task(self, *args, **kwargs):
        pass


class TestStateMachineTransitions(unittest.IsolatedAsyncioTestCase):
    """全面验证状态机闭环流转契约与 4 大子 Handler 行为"""

    async def asyncSetUp(self):
        await Tortoise.init(
            db_url="sqlite://:memory:",
            modules={"models": ["app.dbmodels.models"]},
        )
        await Tortoise.generate_schemas()

        # Mock Redis 分布式锁与队列
        self.lock_patcher = patch(
            "app.redis_client.redis_manager.redis_manager.get_lock",
            return_value=DummyLock(),
        )
        self.lock_patcher.start()

        self.dq_patcher = patch(
            "app.redis_client.redis_manager.redis_manager.get_delayed_queue",
            return_value=DummyDelayedQueue(),
        )
        self.dq_patcher.start()

        self.rpush_patcher = patch(
            "app.redis_client.redis_manager.redis_manager.rpush_list",
            new_callable=AsyncMock,
        )
        self.rpush_patcher.start()

        self.webhook_patcher = patch(
            "app.apis.webhook_dispatcher.dispatch_webhook_event",
            new_callable=AsyncMock,
        )
        self.webhook_patcher.start()

    async def asyncTearDown(self):
        self.webhook_patcher.stop()
        self.rpush_patcher.stop()
        self.dq_patcher.stop()
        self.lock_patcher.stop()
        await Tortoise.close_connections()

    def test_valid_transitions_definition(self):
        """验证 state.py 中 VALID_TRANSITIONS 契约完备性"""
        # SCHEDULED 必须允许流转到 CALLING
        self.assertIn(ClueState.CALLING, VALID_TRANSITIONS[ClueState.SCHEDULED])
        self.assertIn(ClueState.REJECTED, VALID_TRANSITIONS[ClueState.SCHEDULED])
        self.assertIn(ClueState.ACCEPTED, VALID_TRANSITIONS[ClueState.SCHEDULED])

        # CREATED 必须允许流转到 SCHEDULED 和 CALLING，但不能跨级到 ACCEPTED
        self.assertIn(ClueState.SCHEDULED, VALID_TRANSITIONS[ClueState.CREATED])
        self.assertIn(ClueState.CALLING, VALID_TRANSITIONS[ClueState.CREATED])
        self.assertNotIn(ClueState.ACCEPTED, VALID_TRANSITIONS[ClueState.CREATED])

        # SUMMARY 必须包含 SCHEDULED, PENDING, ACCEPTED, REJECTED
        self.assertIn(ClueState.SCHEDULED, VALID_TRANSITIONS[ClueState.SUMMARY])
        self.assertIn(ClueState.PENDING, VALID_TRANSITIONS[ClueState.SUMMARY])
        self.assertIn(ClueState.ACCEPTED, VALID_TRANSITIONS[ClueState.SUMMARY])
        self.assertIn(ClueState.REJECTED, VALID_TRANSITIONS[ClueState.SUMMARY])

        # 终态出度严格为空
        self.assertEqual(VALID_TRANSITIONS[ClueState.ACCEPTED], [])
        self.assertEqual(VALID_TRANSITIONS[ClueState.REJECTED], [])

    async def test_terminal_states_prevent_transition(self):
        """验证终态保护：ACCEPTED 与 REJECTED 严禁任何反向流转"""
        clue = await ClueInfo.create(
            clue_id="clue_term_001",
            status=ClueState.ACCEPTED.value,
            phone="13800000001",
        )
        with self.assertRaises(StateTransitionError):
            await crud.update_clue_state_or_raise("clue_term_001", ClueState.CALLING.value)

        await ClueInfo.filter(clue_id="clue_term_001").update(status=ClueState.REJECTED.value)
        with self.assertRaises(StateTransitionError):
            await crud.update_clue_state_or_raise("clue_term_001", ClueState.SCHEDULED.value)

    async def test_new_clue_handler_transitions_to_scheduled(self):
        """测试线索流入：工作时间首呼将状态从 CREATED 流转为 SCHEDULED"""
        mock_strategy = MagicMock()
        handler = NewClueHandler(strategy_engine=mock_strategy)

        clue = NormalizedClue(
            clueId="clue_new_sm_001",
            accessReceiveTime="2026-10-23 10:00:00",
            phoneNumber="13800000001",
            customerName="张三",
            workflowId="wf_001",
        )
        await crud.create_clue(clue)

        with patch("app.clue_handler.stages.inflow.default.schedule_call", new_callable=AsyncMock) as mock_sch:
            success = await handler.handle(clue)
            self.assertTrue(success)

        current_state = await crud.get_clue_state("clue_new_sm_001")
        self.assertEqual(current_state, ClueState.SCHEDULED.value)

    async def test_new_clue_handler_non_work_time_transitions_to_scheduled(self):
        """测试线索流入：非工作时间次呼排期将状态流转为 SCHEDULED"""
        mock_strategy = AsyncMock()
        mock_strategy.calculate.return_value = ScheduleResult(
            success=True,
            next_call_time=datetime(2026, 10, 24, 9, 30, tzinfo=timezone.utc),
            priority=5,
            use_work_phone=False,
        )
        handler = NewClueHandler(strategy_engine=mock_strategy)

        clue = NormalizedClue(
            clueId="clue_new_sm_002",
            accessReceiveTime="2026-10-23 22:30:00",
            phoneNumber="13800000002",
            customerName="李四",
            workflowId="wf_001",
        )
        await crud.create_clue(clue)

        with patch("app.clue_handler.stages.inflow.default.schedule_call", new_callable=AsyncMock) as mock_sch:
            success = await handler.handle(clue)
            self.assertTrue(success)

        current_state = await crud.get_clue_state("clue_new_sm_002")
        self.assertEqual(current_state, ClueState.SCHEDULED.value)

    async def test_schedule_handler_transitions_to_wait_for_calling(self):
        """测试排期到期送号：准备送号前将状态从 SCHEDULED 流转为 WAIT_FOR_CALLING"""
        clue_id = "clue_sch_sm_003"
        await ClueInfo.create(
            clue_id=clue_id,
            status=ClueState.SCHEDULED.value,
            phone="13800000003",
        )

        mock_send_call = AsyncMock()
        handler = ScheduleHandler(send_call_fn=mock_send_call)

        request = ScheduledCallRequest(
            clueId=clue_id,
            taskId=301,
            executionToken="tok_301",
        )

        success = await handler.handle(request)
        self.assertTrue(success)
        mock_send_call.assert_called_once()

        current_state = await crud.get_clue_state(clue_id)
        self.assertEqual(current_state, ClueState.WAIT_FOR_CALLING.value)

    async def test_full_lifecycle_wait_for_calling_to_calling_on_call_start(self):
        """测试完整闭环：SCHEDULED -> WAIT_FOR_CALLING(送号) -> CALLING(网关振铃通知)"""
        clue_id = "clue_full_cycle_001"
        await ClueInfo.create(
            clue_id=clue_id,
            status=ClueState.SCHEDULED.value,
            phone="13800000009",
            agent_id="default",
            strategy_id="default",
        )

        mock_send_call = AsyncMock(return_value=MagicMock())
        schedule_handler = ScheduleHandler(send_call_fn=mock_send_call)

        # 1. 到期送号 -> 转为 WAIT_FOR_CALLING
        req = ScheduledCallRequest(clueId=clue_id, taskId=501, executionToken="tok_501")
        await schedule_handler.handle(req)
        state_after_schedule = await crud.get_clue_state(clue_id)
        self.assertEqual(state_after_schedule, ClueState.WAIT_FOR_CALLING.value)

        # 2. 网关真正拨通振铃 -> 触发 on_call_start -> 转为 CALLING
        clue_hub = ClueHandler(
            new_clue_handler=MagicMock(),
            postcall_handler=MagicMock(),
            schedule_handler=schedule_handler,
            cancel_call_handler=MagicMock(),
        )
        await clue_hub.on_call_start(clue_id, call_id="call_fc_001")
        state_after_start = await crud.get_clue_state(clue_id)
        self.assertEqual(state_after_start, ClueState.CALLING.value)

    async def test_schedule_handler_terminal_skips_call(self):
        """测试排期到期送号：终态线索自动跳过"""
        clue_id = "clue_sch_term_004"
        await ClueInfo.create(
            clue_id=clue_id,
            status=ClueState.ACCEPTED.value,
            phone="13800000004",
        )

        mock_send_call = AsyncMock()
        handler = ScheduleHandler(send_call_fn=mock_send_call)

        request = ScheduledCallRequest(
            clueId=clue_id,
            taskId=302,
            executionToken="tok_302",
        )

        success = await handler.handle(request)
        self.assertTrue(success)
        mock_send_call.assert_not_called()

    async def test_postcall_handler_transitions_to_pending_on_wechat_action(self):
        """测试挂机处理：Summary分析命中加微动作，状态流转为 PENDING"""
        clue_id = "clue_pc_sm_005"
        await ClueInfo.create(
            clue_id=clue_id,
            status=ClueState.CALLING.value,
            phone="13800000005",
            workflow_id="wf_001",
            strategy_id="strat_001",
        )
        await OutboundStrategy.create(strategy_id="strat_001", schedule_id="single_call")

        mock_summary_engine = MagicMock()
        mock_summary_engine.analyze_call = AsyncMock(return_value={
            "decision": "success",
            "reason": "真人接通并同意加微",
            "common_call_events": {
                "wechat_requested": "yes",
                "is_voicemail": "no",
                "call_summary": "客户同意添加微信",
            },
            "business_result": {"action": "wechat"},
        })

        mock_strategy = MagicMock(spec=SingleCallStrategyEngine)
        handler = PostCallHandler(
            summary_engine=mock_summary_engine,
            strategy_engine=mock_strategy,
        )

        request = CallEndRequest(
            requestId="req_sm_005",
            data=CallEndData(
                clueId=clue_id,
                currentCall=CallEndCurrentCall(callId="call_005"),
                outboundReport=CallEndOutboundReport(
                    wechatRequested=True,
                    userQualified=True,
                ),
            ),
        )

        success = await handler.handle(request)
        self.assertTrue(success)

        current_state = await crud.get_clue_state(clue_id)
        self.assertEqual(current_state, ClueState.PENDING.value)

    async def test_postcall_handler_transitions_to_accepted_on_success(self):
        """测试挂机处理：单通打通达标结案，状态流转为 ACCEPTED"""
        clue_id = "clue_pc_sm_006"
        await ClueInfo.create(
            clue_id=clue_id,
            status=ClueState.CALLING.value,
            phone="13800000006",
            workflow_id="wf_001",
            strategy_id="strat_001",
        )
        await OutboundStrategy.create(strategy_id="strat_001", schedule_id="single_call")

        mock_summary_engine = MagicMock()
        mock_summary_engine.analyze_call = AsyncMock(return_value={
            "decision": "success",
            "reason": "已有效连通",
            "common_call_events": {
                "wechat_requested": "no",
                "is_voicemail": "no",
                "call_summary": "沟通良好",
            },
            "business_result": {},
        })

        mock_strategy = MagicMock(spec=SingleCallStrategyEngine)
        handler = PostCallHandler(
            summary_engine=mock_summary_engine,
            strategy_engine=mock_strategy,
        )

        request = CallEndRequest(
            requestId="req_sm_006",
            data=CallEndData(
                clueId=clue_id,
                currentCall=CallEndCurrentCall(callId="call_006"),
                outboundReport=CallEndOutboundReport(
                    wechatRequested=False,
                    userQualified=True,
                ),
            ),
        )

        success = await handler.handle(request)
        self.assertTrue(success)

        current_state = await crud.get_clue_state(clue_id)
        self.assertEqual(current_state, ClueState.ACCEPTED.value)

    async def test_postcall_handler_transitions_to_scheduled_on_retry(self):
        """测试挂机处理：未接通需要追呼排期，状态流转为 SCHEDULED"""
        clue_id = "clue_pc_sm_007"
        await ClueInfo.create(
            clue_id=clue_id,
            status=ClueState.CALLING.value,
            phone="13800000007",
            workflow_id="wf_001",
            strategy_id="strat_002",
        )
        await OutboundStrategy.create(strategy_id="strat_002", schedule_id="seven_day_three_call")

        mock_summary_engine = MagicMock()
        mock_summary_engine.analyze_call = AsyncMock(return_value={
            "decision": "schedule",
            "reason": "用户未接通",
            "common_call_events": {
                "wechat_requested": "no",
                "is_voicemail": "no",
            },
            "business_result": {},
        })

        mock_strategy = AsyncMock()
        mock_strategy.calculate.return_value = ScheduleResult(
            success=True,
            next_call_time=datetime(2026, 10, 23, 14, 0, tzinfo=timezone.utc),
            priority=5,
            use_work_phone=False,
        )

        handler = PostCallHandler(
            summary_engine=mock_summary_engine,
            strategy_engine=mock_strategy,
        )

        request = CallEndRequest(
            requestId="req_sm_007",
            data=CallEndData(
                clueId=clue_id,
                currentCall=CallEndCurrentCall(callId="call_007"),
            ),
        )

        with patch("app.clue_handler.stages.postcall.default.schedule_call", new_callable=AsyncMock) as mock_sch:
            mock_sch.return_value = MagicMock(id=701, run_at=datetime.now())
            success = await handler.handle(request)
            self.assertTrue(success)

        current_state = await crud.get_clue_state(clue_id)
        self.assertEqual(current_state, ClueState.SCHEDULED.value)

    async def test_cancel_call_handler_transitions_to_rejected(self):
        """测试取消/兜底处理器：将线索置为 REJECTED 并发送结案 Webhook"""
        clue_id = "clue_cancel_008"
        await ClueInfo.create(
            clue_id=clue_id,
            status=ClueState.CALLING.value,
            phone="13800000008",
            workflow_id="wf_001",
        )

        handler = CancelCallHandler()
        result = await handler.handle(clue_id, closeReason="人工主动取消")
        self.assertTrue(result.success)

        current_state = await crud.get_clue_state(clue_id)
        self.assertEqual(current_state, ClueState.REJECTED.value)
        self.assertEqual(result.reason, "已成功结束线索并同步事件")

        clue_record = await ClueInfo.filter(clue_id=clue_id).first()
        self.assertEqual(clue_record.close_reason, "人工主动取消")
