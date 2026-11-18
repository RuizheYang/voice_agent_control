import asyncio
from datetime import datetime, time, timedelta
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from pydantic import ValidationError
from zoneinfo import ZoneInfo

from app.strategy.single_call import SingleCallStrategyConfig, SingleCallStrategyEngine
from app.strategy.sevenday import SevenDayStrategyConfig, SevenDayStrategyEngine
from app.strategy.factory import StrategyEngineFactory, SCHEDULE_REGISTRY, SUPPORTED_SCHEDULE_IDS
from app.schemas.strategy_config import StrategyCreateRequest, FollowupConfig
from app.clue_handler.state import ClueState
from app.clue_handler.stages.postcall import DefaultPostCallHandler as PostCallHandler
from app.utils.time_utils import ensure_tz_aware

CST = ZoneInfo("Asia/Shanghai")


class TestSingleCallStrategyEngine(unittest.IsolatedAsyncioTestCase):
    """测试 SingleCallStrategyEngine (单通即结案策略) 的排期与拒绝推演"""

    def setUp(self):
        self.config = SingleCallStrategyConfig(
            work_start_time="09:00:00",
            work_end_time="21:00:00",
            call_max_time=24,
            max_call_num=1,
            noanswer_num=1,
        )
        self.engine = SingleCallStrategyEngine(self.config)
        self.time_patcher = patch(
            "app.strategy.single_call.engine.get_now",
            return_value=datetime(2026, 10, 16, 14, 30, 0, tzinfo=CST),
        )
        self.mock_now = self.time_patcher.start()

    def tearDown(self):
        self.time_patcher.stop()

    async def test_first_call_during_working_hours_schedules_immediate(self):
        """测试工作时间内首次进件：安排即刻外呼"""
        intake_time = datetime(2026, 10, 16, 14, 30, 0, tzinfo=CST)
        mock_ctx = MagicMock()
        mock_ctx.clue_id = "clue_single_01"
        mock_ctx.total_call_count = 0
        mock_ctx.call_records = []
        mock_ctx.clue_intake_time = intake_time
        mock_ctx.customer_date = None

        res = await self.engine.calculate(mock_ctx)
        self.assertTrue(res.success)
        self.assertEqual(res.next_call_time, intake_time)
        self.assertFalse(res.use_work_phone)
        self.assertIn("工作时间内首呼即时外呼", res.reason)

    async def test_first_call_during_night_hours_schedules_next_morning(self):
        """测试夜间非工作时间进件 (如 22:30)：顺延至次日 09:00 开工外呼"""
        intake_time = datetime(2026, 10, 16, 22, 30, 0, tzinfo=CST)
        mock_ctx = MagicMock()
        mock_ctx.clue_id = "clue_single_02"
        mock_ctx.total_call_count = 0
        mock_ctx.call_records = []
        mock_ctx.clue_intake_time = intake_time
        mock_ctx.customer_date = None

        res = await self.engine.calculate(mock_ctx)
        self.assertTrue(res.success)
        expected_time = datetime(2026, 10, 17, 9, 0, 0, tzinfo=CST)
        self.assertEqual(res.next_call_time, expected_time)
        self.assertIn("非工作时间顺延至次日开工外呼", res.reason)

    async def test_first_call_with_valid_customer_appointment(self):
        """测试客户指定工作时间内约定外呼：预约时间优先生效"""
        intake_time = datetime(2026, 10, 16, 10, 0, 0, tzinfo=CST)
        appt_time = datetime(2026, 10, 16, 16, 0, 0, tzinfo=CST)
        mock_ctx = MagicMock()
        mock_ctx.clue_id = "clue_single_03"
        mock_ctx.total_call_count = 0
        mock_ctx.call_records = []
        mock_ctx.clue_intake_time = intake_time
        mock_ctx.customer_date = appt_time

        res = await self.engine.calculate(mock_ctx)
        self.assertTrue(res.success)
        self.assertEqual(res.next_call_time, appt_time)
        self.assertIn("客户约定时间优先生效", res.reason)

    async def test_rejects_subsequent_call_when_already_called_once(self):
        """测试打过一通电话后，无论结果如何均坚决拒绝二次排期并结案"""
        mock_ctx = MagicMock()
        mock_ctx.clue_id = "clue_single_04"
        mock_ctx.total_call_count = 1
        mock_ctx.call_records = [MagicMock()]
        mock_ctx.clue_intake_time = datetime(2026, 10, 16, 10, 0, 0, tzinfo=CST)
        mock_ctx.customer_date = None

        res = await self.engine.calculate(mock_ctx)
        self.assertFalse(res.success)
        self.assertIn("单通策略完成：线索已执行过 1 次外呼", res.reason)

    async def test_rejects_when_lifecycle_exceeded(self):
        """测试线索已超过最长跟进生命周期限制时拒绝排期"""
        intake_time = datetime(2026, 10, 14, 8, 0, 0, tzinfo=CST)
        mock_ctx = MagicMock()
        mock_ctx.clue_id = "clue_single_05"
        mock_ctx.total_call_count = 0
        mock_ctx.call_records = []
        mock_ctx.clue_intake_time = intake_time
        mock_ctx.customer_date = None

        res = await self.engine.calculate(mock_ctx)
        self.assertFalse(res.success)
        self.assertIn("超出单通最大生命周期", res.reason)


class TestStrategyEngineFactoryAndRegistry(unittest.TestCase):
    """测试 StrategyEngineFactory 工厂与 1:1 严格注册表映射机制"""

    def test_factory_returns_single_call_engine(self):
        """测试 schedule_id='single_call' 准确返回 SingleCallStrategyEngine 实例"""
        engine = StrategyEngineFactory.get_engine("single_call")
        self.assertIsInstance(engine, SingleCallStrategyEngine)

    def test_factory_returns_seven_day_three_call_engine(self):
        """测试 schedule_id='seven_day_three_call' 及缺省 None 返回 SevenDayStrategyEngine 实例"""
        engine1 = StrategyEngineFactory.get_engine("seven_day_three_call")
        self.assertIsInstance(engine1, SevenDayStrategyEngine)
        engine_default = StrategyEngineFactory.get_engine(None)
        self.assertIsInstance(engine_default, SevenDayStrategyEngine)

    def test_factory_rejects_unknown_or_legacy_aliases(self):
        """测试非标准名称或历史遗留别名被严格拒绝，直接抛出 ValueError"""
        invalid_or_legacy_ids = [
            "single-call-1",
            "threeday-common-1",
            "noworkphone",
            "workphone",
            "sevenday",
            "seven_day",
            "unknown_schedule_xyz",
        ]
        for bad_id in invalid_or_legacy_ids:
            self.assertFalse(StrategyEngineFactory.is_valid_schedule_id(bad_id))
            with self.assertRaises(ValueError) as ctx:
                StrategyEngineFactory.get_engine(bad_id)
            self.assertIn("未知或不支持的排期策略", str(ctx.exception))

    def test_factory_caches_engine_singletons(self):
        """测试引擎实例在工厂内部缓存复用，避免每次实例化开销"""
        engine1 = StrategyEngineFactory.get_engine("single_call")
        engine2 = StrategyEngineFactory.get_engine("single_call")
        self.assertIs(engine1, engine2)

    def test_is_valid_schedule_id(self):
        """测试合法性严格校验方法"""
        self.assertTrue(StrategyEngineFactory.is_valid_schedule_id("single_call"))
        self.assertTrue(StrategyEngineFactory.is_valid_schedule_id("seven_day_three_call"))
        self.assertFalse(StrategyEngineFactory.is_valid_schedule_id("threeday-common-1"))
        self.assertFalse(StrategyEngineFactory.is_valid_schedule_id("workphone"))
        self.assertFalse(StrategyEngineFactory.is_valid_schedule_id("noworkphone"))
        self.assertFalse(StrategyEngineFactory.is_valid_schedule_id("single-call-1"))
        self.assertFalse(StrategyEngineFactory.is_valid_schedule_id("invalid_fake_id"))
        self.assertFalse(StrategyEngineFactory.is_valid_schedule_id(""))
        self.assertFalse(StrategyEngineFactory.is_valid_schedule_id(None))


class TestStrategySchemaValidation(unittest.TestCase):
    """测试外呼策略 Schema 在创建时对 schedule_id 的白名单校验"""

    def test_valid_schedule_id_passes(self):
        for sid in ["single_call", "seven_day_three_call"]:
            req = StrategyCreateRequest(
                strategy_id=f"strat_test_{sid}",
                schedule_id=sid,
                followup=FollowupConfig(summary_type="single_call", summary_prompt="common_summary"),
            )
            self.assertEqual(req.schedule_id, sid)

    def test_invalid_schedule_id_raises_validation_error(self):
        with self.assertRaises(ValidationError) as ctx:
            StrategyCreateRequest(
                strategy_id="strat_test_02",
                schedule_id="invalid_xyz_template",
                followup=FollowupConfig(summary_type="single_call", summary_prompt="common_summary"),
            )
        self.assertIn("未知的排期策略 schedule_id", str(ctx.exception))


class TestPostCallHandlerLifecycle(unittest.IsolatedAsyncioTestCase):
    """测试 PostCallHandler 结合 single_call 策略的终态自动流转与 Webhook 闭环"""

    @patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue")
    @patch("app.dbmodels.crud.get_clue_state", return_value=ClueState.SUMMARY.value)
    @patch("app.dbmodels.crud.update_clue_state_or_raise", new_callable=AsyncMock)
    @patch("app.dbmodels.crud.save_close_reason", new_callable=AsyncMock)
    @patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock)
    @patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock)
    @patch("app.dbmodels.models.CallRecord.filter")
    @patch("app.dbmodels.models.ClueInfo.filter")
    @patch("app.dbmodels.models.OutboundStrategy.filter")
    @patch("app.strategy.contracts.context.ClueContext.from_db")
    async def test_postcall_single_call_finishes_and_emits_clue_ended(
        self,
        mock_ctx_from_db,
        mock_strat_filter,
        mock_clue_filter,
        mock_call_filter,
        mock_webhook_dispatch,
        mock_event_bus_emit,
        mock_save_reason,
        mock_update_state,
        mock_get_state,
        mock_delayed_queue,
    ):
        mock_delayed_queue.return_value.remove_task = AsyncMock()
        clue_id = "clue_postcall_single_01"

        # Mock ClueInfo
        mock_clue = MagicMock()
        mock_clue.strategy_id = "strat_single_01"
        mock_clue.workflow_id = "wf_car_loan_01"
        mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)

        # Mock OutboundStrategy (使用 single_call)
        mock_strategy = MagicMock()
        mock_strategy.schedule_id = "single_call"
        mock_strat_filter.return_value.first = AsyncMock(return_value=mock_strategy)

        # Mock ClueContext (已有 1 次通话)
        mock_call_record = MagicMock()
        mock_call_record.decision_result = {"userQualified": True}
        mock_context = MagicMock()
        mock_context.clue_id = clue_id
        mock_context.total_call_count = 1
        mock_context.call_records = [mock_call_record]
        mock_ctx_from_db.return_value = mock_context

        # Mock CallRecord.filter
        mock_call_filter.return_value.order_by.return_value.first = AsyncMock(return_value=None)

        handler = PostCallHandler()
        res = await handler.handle(clue_id, "req_001")

        self.assertTrue(res)
        # 验证状态流转为 ACCEPTED (因为 userQualified 为 True)
        mock_update_state.assert_called_with(clue_id, ClueState.ACCEPTED.value)
        # 验证结案原因保存
        mock_save_reason.assert_called_once()
        # 验证 ClueEndEvent 发射
        mock_event_bus_emit.assert_called_once()
        emitted_event = mock_event_bus_emit.call_args[0][0]
        self.assertEqual(emitted_event.clue_id, clue_id)
        self.assertEqual(emitted_event.workflow_id, "wf_car_loan_01")
        self.assertEqual(emitted_event.event_type, "clue_end")
        self.assertEqual(emitted_event.event_data["code"], 200)


class TestStrategyDependencyInjection(unittest.IsolatedAsyncioTestCase):
    """测试外呼策略引擎在 clue.py 与各子 Handler 中的依赖注入机制 (方案A)"""

    @patch("app.dbmodels.crud.get_workflow_config", return_value=None)
    @patch("app.dbmodels.models.OutboundStrategy.filter")
    async def test_clue_handler_factory_wires_default_engine(self, mock_strat_filter, mock_wf):
        """测试 ClueHandlerFactory 组装默认 workflow 时，正确将具体策略引擎注入到子处理器中"""
        from app.clue_handler.factory import ClueHandlerFactory
        from app.strategy.sevenday import SevenDayStrategyEngine

        mock_strat_filter.return_value.first = AsyncMock(return_value=None)
        clue_handler = await ClueHandlerFactory.get_handler_by_workflow("default")
        self.assertIsNotNone(clue_handler.new_clue_handler)
        self.assertIsNotNone(clue_handler.postcall_handler)

        # 验证 new_clue_handler 与 postcall_handler 均直接注入了具体策略引擎 (SevenDayStrategyEngine)
        self.assertIsInstance(clue_handler.new_clue_handler.strategy_engine, SevenDayStrategyEngine)
        self.assertIsInstance(clue_handler.postcall_handler.strategy_engine, SevenDayStrategyEngine)

    @patch("app.dbmodels.models.OutboundStrategy.filter")
    async def test_new_clue_handler_uses_injected_engine_resolver(self, mock_strat_filter):
        """测试 NewClueHandler 在解析线索策略时，优先调用注入的 engine_resolver"""
        from app.clue_handler.stages.inflow import DefaultInflowHandler as NewClueHandler
        from app.schemas.clue_create import NormalizedClue

        mock_strategy = MagicMock()
        mock_strategy.schedule_id = "custom_injected_schedule"
        mock_strat_filter.return_value.first = AsyncMock(return_value=mock_strategy)

        # 准备 Mock 解析器
        mock_custom_engine = MagicMock(spec=SingleCallStrategyEngine)
        mock_resolver = MagicMock(return_value=mock_custom_engine)

        mock_call_fn = AsyncMock()
        handler = NewClueHandler(
            send_call_fn=mock_call_fn,
            engine_resolver=mock_resolver,  # 依赖注入自定义解析器
        )

        clue = NormalizedClue(
            clueId="clue_di_test_01",
            strategyId="strat_custom_01",
            workflowId="wf_di_test",
            customerName="张三",
            phoneNumber="13800000000",
        )

        resolved_engine = await handler.get_engine_for_clue(clue)
        self.assertEqual(resolved_engine, mock_custom_engine)
        mock_resolver.assert_called_once_with("custom_injected_schedule")

    @patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue")
    @patch("app.dbmodels.crud.get_clue_state", return_value=ClueState.SUMMARY.value)
    @patch("app.dbmodels.crud.update_clue_state_or_raise", new_callable=AsyncMock)
    @patch("app.dbmodels.crud.save_close_reason", new_callable=AsyncMock)
    @patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock)
    @patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock)
    @patch("app.dbmodels.models.ClueInfo.filter")
    @patch("app.dbmodels.models.OutboundStrategy.filter")
    @patch("app.strategy.contracts.context.ClueContext.from_db")
    async def test_postcall_handler_uses_injected_engine_resolver(
        self,
        mock_ctx_from_db,
        mock_strat_filter,
        mock_clue_filter,
        mock_webhook_dispatch,
        mock_event_bus_emit,
        mock_save_reason,
        mock_update_state,
        mock_get_state,
        mock_delayed_queue,
    ):
        """测试 PostCallHandler 在下一轮追呼推演时，使用注入的 engine_resolver"""
        mock_delayed_queue.return_value.remove_task = AsyncMock()
        clue_id = "clue_di_postcall_01"

        mock_clue = MagicMock()
        mock_clue.strategy_id = "strat_di_01"
        mock_clue.workflow_id = "wf_di_postcall"
        mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)

        mock_strategy = MagicMock()
        mock_strategy.schedule_id = "single_call"
        mock_strat_filter.return_value.first = AsyncMock(return_value=mock_strategy)

        mock_context = MagicMock()
        mock_context.clue_id = clue_id
        mock_context.total_call_count = 1
        mock_context.call_records = []
        mock_ctx_from_db.return_value = mock_context

        # 准备注入的 Mock 解析器
        mock_engine = MagicMock()
        mock_engine.calculate = AsyncMock(return_value=MagicMock(success=False, reason="单通测试结束"))
        mock_resolver = MagicMock(return_value=mock_engine)

        handler = PostCallHandler(engine_resolver=mock_resolver)
        res = await handler.handle(clue_id, "req_di_01")

        self.assertTrue(res)
        mock_resolver.assert_called_once_with("single_call")
        mock_engine.calculate.assert_called_once_with(mock_context)


class TestClueHandlerFactoryWorkflowScoped(unittest.IsolatedAsyncioTestCase):
    """测试 ClueHandlerFactory 基于 workflow_id 作用域的专属装配、直接注入与缓存机制"""

    def setUp(self):
        from app.clue_handler.factory import ClueHandlerFactory
        ClueHandlerFactory.invalidate_all()

    @patch("app.dbmodels.crud.get_workflow_config")
    @patch("app.dbmodels.models.OutboundStrategy.filter")
    async def test_factory_get_handler_by_workflow_injects_concrete_engines(
        self,
        mock_strat_filter,
        mock_get_wf,
    ):
        """测试根据 workflow_id 装配时，直接将 SingleCallStrategyEngine 注入子 Handler"""
        from app.clue_handler.factory import ClueHandlerFactory
        from app.strategy.single_call import SingleCallStrategyEngine

        # Mock Workflow 配置：wf_car_loan 绑定 strat_single_01
        mock_wf = MagicMock()
        mock_wf.workflow_id = "wf_car_loan"
        mock_wf.strategy_id = "strat_single_01"
        mock_get_wf.return_value = mock_wf

        # Mock OutboundStrategy：strat_single_01 的 schedule_id 为 single_call
        mock_strat = MagicMock()
        mock_strat.strategy_id = "strat_single_01"
        mock_strat.schedule_id = "single_call"
        mock_strat.followup = {"summary_type": "single_call", "summary_prompt": "fast_decision"}
        mock_strat_filter.return_value.first = AsyncMock(return_value=mock_strat)

        handler = await ClueHandlerFactory.get_handler_by_workflow("wf_car_loan")

        self.assertIsNotNone(handler)
        self.assertEqual(handler.workflow_id, "wf_car_loan")

        # 核心断言：子 Handler 已经直接被注入了 SingleCallStrategyEngine 具体实例！
        self.assertIsInstance(handler.new_clue_handler.strategy_engine, SingleCallStrategyEngine)
        self.assertIsInstance(handler.postcall_handler.strategy_engine, SingleCallStrategyEngine)

    @patch("app.dbmodels.crud.get_workflow_config")
    @patch("app.dbmodels.models.OutboundStrategy.filter")
    async def test_factory_caching_and_invalidation(
        self,
        mock_strat_filter,
        mock_get_wf,
    ):
        """测试 ClueHandlerFactory 的缓存机制与失效机制"""
        from app.clue_handler.factory import ClueHandlerFactory

        mock_wf = MagicMock(strategy_id="strat_01")
        mock_get_wf.return_value = mock_wf

        mock_strat = MagicMock(schedule_id="seven_day_three_call", followup={})
        mock_strat_filter.return_value.first = AsyncMock(return_value=mock_strat)

        # 首次获取：查库装配并缓存
        handler1 = await ClueHandlerFactory.get_handler_by_workflow("wf_test_cache")
        # 二次获取：直接命中缓存，对象指针完全一致
        handler2 = await ClueHandlerFactory.get_handler_by_workflow("wf_test_cache")
        self.assertIs(handler1, handler2)

        # 单独失效该 workflow 的缓存
        ClueHandlerFactory.invalidate_workflow("wf_test_cache")
        handler3 = await ClueHandlerFactory.get_handler_by_workflow("wf_test_cache")
        self.assertIsNot(handler1, handler3)

    @patch("app.dbmodels.models.ClueInfo.filter")
    @patch("app.dbmodels.crud.get_workflow_config")
    @patch("app.dbmodels.models.OutboundStrategy.filter")
    async def test_factory_get_handler_by_clue(
        self,
        mock_strat_filter,
        mock_get_wf,
        mock_clue_filter,
    ):
        """测试通过 clue_id 查找其所属 workflow_id 并获取一致的 ClueHandler"""
        from app.clue_handler.factory import ClueHandlerFactory

        mock_clue = MagicMock(clue_id="clue_lookup_001", workflow_id="wf_customer_service")
        mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)

        mock_wf = MagicMock(strategy_id="strat_cs_01")
        mock_get_wf.return_value = mock_wf

        mock_strat = MagicMock(schedule_id="seven_day_three_call", followup={})
        mock_strat_filter.return_value.first = AsyncMock(return_value=mock_strat)

        handler = await ClueHandlerFactory.get_handler_by_clue("clue_lookup_001")
        self.assertEqual(handler.workflow_id, "wf_customer_service")
