import unittest
from unittest.mock import MagicMock, AsyncMock

from app.clue_handler.contracts import (
    BaseInflowHandler,
    BasePostCallHandler,
    BaseScheduleHandler,
    BaseCancelHandler,
    BaseCancelCallHandler,
    CancelClueResult,
)
from app.clue_handler.stages import (
    DefaultInflowHandler,
    INFLOW_REGISTRY,
    get_inflow_handler_cls,
    DefaultPostCallHandler,
    PostCallHandler,
    POSTCALL_REGISTRY,
    get_postcall_handler_cls,
    DefaultScheduleHandler,
    ScheduleHandler,
    SCHEDULE_REGISTRY,
    get_schedule_handler_cls,
    DefaultCancelHandler,
    CancelCallHandler,
    CANCEL_REGISTRY,
    get_cancel_handler_cls,
)
from app.clue_handler import ClueHandler, ClueHandlerFactory


class TestClueHandlerStageArchitecture(unittest.TestCase):
    """测试 ClueHandler 生命周期分阶段目录架构、契约继承与注册表机制"""

    def test_contracts_hierarchy_and_aliases(self):
        """测试抽象契约继承关系与阶段处理器映射"""
        self.assertIs(BaseCancelCallHandler, BaseCancelHandler)
        self.assertIs(PostCallHandler, DefaultPostCallHandler)
        self.assertIs(ScheduleHandler, DefaultScheduleHandler)
        self.assertIs(CancelCallHandler, DefaultCancelHandler)

    def test_default_handlers_implement_contracts(self):
        """测试默认阶段处理器均正确继承对应抽象契约"""
        self.assertTrue(issubclass(DefaultInflowHandler, BaseInflowHandler))
        self.assertTrue(issubclass(DefaultPostCallHandler, BasePostCallHandler))
        self.assertTrue(issubclass(DefaultScheduleHandler, BaseScheduleHandler))
        self.assertTrue(issubclass(DefaultCancelHandler, BaseCancelHandler))

    def test_inflow_stage_registry(self):
        """测试 Inflow 阶段注册表与查询"""
        self.assertIn("default", INFLOW_REGISTRY)
        self.assertIs(get_inflow_handler_cls("default"), DefaultInflowHandler)
        self.assertIs(get_inflow_handler_cls("  DEFAULT  "), DefaultInflowHandler)
        with self.assertRaises(ValueError) as ctx:
            get_inflow_handler_cls("unknown_inflow_type")
        self.assertIn("未知的 InflowHandler 标识", str(ctx.exception))

    def test_postcall_stage_registry(self):
        """测试 PostCall 阶段注册表与查询"""
        self.assertIn("default", POSTCALL_REGISTRY)
        self.assertIs(get_postcall_handler_cls("default"), DefaultPostCallHandler)
        with self.assertRaises(ValueError) as ctx:
            get_postcall_handler_cls("unknown_postcall_type")
        self.assertIn("未知的 PostCallHandler 标识", str(ctx.exception))

    def test_schedule_stage_registry(self):
        """测试 Schedule 阶段注册表与查询"""
        self.assertIn("default", SCHEDULE_REGISTRY)
        self.assertIs(get_schedule_handler_cls("default"), DefaultScheduleHandler)
        with self.assertRaises(ValueError) as ctx:
            get_schedule_handler_cls("unknown_schedule_type")
        self.assertIn("未知的 ScheduleHandler 标识", str(ctx.exception))

    def test_cancel_stage_registry(self):
        """测试 Cancel 阶段注册表与查询"""
        self.assertIn("default", CANCEL_REGISTRY)
        self.assertIs(get_cancel_handler_cls("default"), DefaultCancelHandler)
        with self.assertRaises(ValueError) as ctx:
            get_cancel_handler_cls("unknown_cancel_type")
        self.assertIn("未知的 CancelHandler 标识", str(ctx.exception))

    def test_custom_stage_handler_extensibility(self):
        """验证开发者扩展自定义业务子 Handler 的极简三步走机制"""
        # 1. 继承契约实现自定义阶段 Handler
        class FastTrackInflowHandler(BaseInflowHandler):
            async def handle(self, clue):
                return True

        # 2. 登记入阶段注册表
        INFLOW_REGISTRY["fast_track"] = FastTrackInflowHandler

        try:
            # 3. 验证注册获取
            resolved = get_inflow_handler_cls("fast_track")
            self.assertIs(resolved, FastTrackInflowHandler)
            self.assertTrue(issubclass(resolved, BaseInflowHandler))
        finally:
            INFLOW_REGISTRY.pop("fast_track", None)
