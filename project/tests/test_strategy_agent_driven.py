import asyncio
from datetime import datetime, timedelta, time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

from app.strategy.agent_driven.models import (
    StandardBusinessStatus,
    ScheduleActionType,
    AppointmentEvidence,
    IntentAnalysisOutput,
    ObjectionAnalysisOutput,
    AppointmentAnalysisOutput,
    SentimentAnalysisOutput,
    MasterSummaryDecision,
    AgentStrategyConfig,
)
from app.strategy.agent_driven.tools import (
    is_within_working_window,
    snap_to_working_window,
    calculate_relative_time,
    format_conversation_for_prompt,
)
from app.strategy.agent_driven.harness import PolicyHarness, CorridorEnvelope
from app.strategy.agent_driven.fallback import HeuristicSafetyFallbackEngine
from app.strategy.agent_driven.engine import AgentDrivenStrategyEngine
from app.strategy.factory import StrategyEngineFactory, SUPPORTED_SCHEDULE_IDS, SCHEDULE_REGISTRY
from app.strategy.contracts.context import ClueContext
from app.strategy.contracts.base import ScheduleResult
from app.utils.time_utils import ensure_tz_aware

CST = ZoneInfo("Asia/Shanghai")


class TestAgentDrivenStrategyFramework(unittest.IsolatedAsyncioTestCase):
    """测试完全由 Agent 驱动的外呼策略排期与强护栏 Harness 架构"""

    def setUp(self):
        self.config = AgentStrategyConfig(
            work_start_time="09:00:00",
            work_end_time="21:00:00",
            min_cooldown_minutes=30,
            max_call_num=10,
            noanswer_num=4,
            call_max_time=72,
            timeout_seconds=5.0,
        )
        self.harness = PolicyHarness(self.config)
        self.fallback = HeuristicSafetyFallbackEngine(self.config)

    # ==============================================================================
    # 1. 契约与标准 4 态模型测试
    # ==============================================================================

    def test_standard_four_states_and_pydantic_validation(self):
        """测试标准 4 态枚举完备性与 MasterSummaryDecision 校验"""
        self.assertEqual(StandardBusinessStatus.SUCCESS.value, "success")
        self.assertEqual(StandardBusinessStatus.FAILURE.value, "failure")
        self.assertEqual(StandardBusinessStatus.REJECTION.value, "rejection")
        self.assertEqual(StandardBusinessStatus.PARTIAL.value, "partial")

        # 终态结案状态下，自动清空排期时间
        decision_success = MasterSummaryDecision(
            status=StandardBusinessStatus.SUCCESS,
            reason="客户同意添加微信并预约试驾",
            action=ScheduleActionType.TERMINATE_SUCCESS,
            next_call_time=datetime(2026, 10, 29, 14, 0, 0, tzinfo=CST),
            voice_agent_directive="已达成目标，结束跟进",
        )
        self.assertIsNone(decision_success.next_call_time)

        # 存续跟进状态下，保留排期时间
        target_dt = datetime(2026, 10, 29, 15, 30, 0, tzinfo=CST)
        decision_partial = MasterSummaryDecision(
            status=StandardBusinessStatus.PARTIAL,
            reason="客户开会中，预约下午沟通",
            action=ScheduleActionType.SCHEDULE_CALL,
            next_call_time=target_dt,
            appointment=AppointmentEvidence(
                has_appointment=True,
                verbatim_quote="下午三点半打给我",
                parsed_time=target_dt,
            ),
            voice_agent_directive="开门见山询问会议是否结束",
        )
        self.assertEqual(decision_partial.next_call_time, target_dt)

    # ==============================================================================
    # 2. 强护栏 Harness 工具函数与安全吸附测试
    # ==============================================================================

    def test_working_window_and_safety_snapping(self):
        """测试法定工时检测与几何安全吸附 (09:00 - 21:00)"""
        # 1. 白天合法时间保持不变
        day_time = datetime(2026, 10, 29, 14, 0, 0, tzinfo=CST)
        self.assertTrue(is_within_working_window(day_time))
        self.assertEqual(snap_to_working_window(day_time), day_time)

        # 2. 清晨 07:15 (早于 09:00) 自动吸附至当日 09:30
        early_time = datetime(2026, 10, 29, 7, 15, 0, tzinfo=CST)
        self.assertFalse(is_within_working_window(early_time))
        snapped_early = snap_to_working_window(early_time)
        self.assertEqual(snapped_early, datetime(2026, 10, 29, 9, 30, 0, tzinfo=CST))

        # 3. 夜间 22:45 (晚于 21:00) 自动吸附至次日 09:30
        night_time = datetime(2026, 10, 29, 22, 45, 0, tzinfo=CST)
        self.assertFalse(is_within_working_window(night_time))
        snapped_night = snap_to_working_window(night_time)
        self.assertEqual(snapped_night, datetime(2026, 10, 30, 9, 30, 0, tzinfo=CST))

    # ==============================================================================
    # 3. 强护栏原话证据链核验测试 (Evidence-based Verbatim Grounding)
    # ==============================================================================

    def test_verbatim_evidence_verification(self):
        """测试原话证据链核验：真实原话放行，凭空臆测拦截"""
        turns = [
            {"role": "assistant", "content": "您好，请问您在看 Model Y 吗？"},
            {"role": "user", "content": "我现在正在接送孩子，明天上午十点后再联系我吧。"},
        ]

        # 证据确凿的决策 (原话在对话中真实存在)
        verified_decision = MasterSummaryDecision(
            status=StandardBusinessStatus.PARTIAL,
            reason="客户预约明天上午",
            action=ScheduleActionType.SCHEDULE_CALL,
            appointment=AppointmentEvidence(
                has_appointment=True,
                verbatim_quote="明天上午十点后再联系我吧",
                parsed_time=datetime(2026, 10, 30, 10, 15, 0, tzinfo=CST),
            ),
        )
        self.assertTrue(self.harness.verify_verbatim_evidence(verified_decision, turns))

        # 凭空臆测无原话支撑的决策 (原话不存在于对话中)
        hallucinated_decision = MasterSummaryDecision(
            status=StandardBusinessStatus.PARTIAL,
            reason="大模型凭空推断",
            action=ScheduleActionType.SCHEDULE_CALL,
            appointment=AppointmentEvidence(
                has_appointment=True,
                verbatim_quote="下周五晚上八点打给我",
                parsed_time=datetime(2026, 11, 6, 20, 0, 0, tzinfo=CST),
            ),
        )
        self.assertFalse(self.harness.verify_verbatim_evidence(hallucinated_decision, turns))

    # ==============================================================================
    # 4. Harness 4 态评估与调度收敛测试
    # ==============================================================================

    def test_harness_evaluate_success_and_rejection(self):
        """测试 SUCCESS 与 REJECTION 状态直接触发终态收敛"""
        mock_ctx = MagicMock(spec=ClueContext)
        mock_ctx.total_call_count = 1
        mock_ctx.continuous_no_answer_count = 0

        # SUCCESS 测试
        dec_success = MasterSummaryDecision(
            status=StandardBusinessStatus.SUCCESS,
            reason="客户已同意添加企业微信",
            action=ScheduleActionType.TERMINATE_SUCCESS,
            voice_agent_directive="达成目标结案",
        )
        res_succ = self.harness.evaluate_and_snap(dec_success, mock_ctx, [])
        self.assertFalse(res_succ.success)  # 结案不产生新排期
        self.assertEqual(res_succ.business_status, "success")
        self.assertIn("外呼目标达成结案", res_succ.reason)

        # REJECTION 测试
        dec_reject = MasterSummaryDecision(
            status=StandardBusinessStatus.REJECTION,
            reason="客户明确表示已买竞品，要求不要再打电话",
            action=ScheduleActionType.TERMINATE_REJECT,
        )
        res_rej = self.harness.evaluate_and_snap(dec_reject, mock_ctx, [])
        self.assertFalse(res_rej.success)
        self.assertEqual(res_rej.business_status, "rejection")
        self.assertIn("客户明确拒绝", res_rej.reason)

    def test_harness_evaluate_partial_with_corridor_snapping(self):
        """测试 PARTIAL 状态无证据偏航时，强制钳位至走廊区间"""
        ref_time = datetime(2026, 10, 29, 14, 0, 0, tzinfo=CST)
        mock_ctx = MagicMock(spec=ClueContext)
        mock_ctx.clue_intake_time = ref_time
        mock_ctx.total_call_count = 1
        mock_ctx.continuous_no_answer_count = 0

        # 模拟模型输出极度走偏的时间 (如 10 天后的深夜 23:00)
        wild_time = ref_time + timedelta(days=10, hours=9)
        dec_wild = MasterSummaryDecision(
            status=StandardBusinessStatus.PARTIAL,
            reason="客户在考虑",
            action=ScheduleActionType.SCHEDULE_CALL,
            next_call_time=wild_time,
            voice_agent_directive="回访意向",
        )

        res = self.harness.evaluate_and_snap(dec_wild, mock_ctx, [], base_time=ref_time)
        self.assertTrue(res.success)
        self.assertIsNotNone(res.next_call_time)
        # 验证被强制拉回到了 48 小时内的合法工时窗口
        self.assertLessEqual(res.next_call_time, ref_time + timedelta(days=3))
        self.assertTrue(is_within_working_window(res.next_call_time))
        self.assertEqual(res.business_status, "partial")
        self.assertEqual(res.voice_agent_directive, "回访意向")

    # ==============================================================================
    # 5. 方案 A 启发式安全兜底引擎测试 (Heuristic Fallback)
    # ==============================================================================

    def test_fallback_engine_connected_and_unanswered(self):
        """测试大模型超时或报错时，启发式安全兜底引擎的表现"""
        mock_ctx = MagicMock(spec=ClueContext)
        mock_ctx.clue_id = "clue_fallback_01"
        mock_ctx.total_call_count = 1
        mock_ctx.continuous_no_answer_count = 1
        mock_ctx.customer_date = None

        # 未接通降级：推延 3 小时
        res_unans = self.fallback.generate_fallback_decision(mock_ctx, is_connected=False)
        self.assertTrue(res_unans.success)
        self.assertIn("未接通追呼排期", res_unans.reason)
        self.assertEqual(res_unans.business_status, "failure")

        # 接通降级：推延 4 小时或次日开工
        res_conn = self.fallback.generate_fallback_decision(mock_ctx, is_connected=True)
        self.assertTrue(res_conn.success)
        self.assertIn("通话后常规跟进排期", res_conn.reason)
        self.assertEqual(res_conn.business_status, "partial")

        # 熔断降级：连续未接达上限 4 次
        mock_ctx.continuous_no_answer_count = 4
        res_fused = self.fallback.generate_fallback_decision(mock_ctx, is_connected=False)
        self.assertFalse(res_fused.success)
        self.assertIn("连续未接通达到上限", res_fused.reason)

    # ==============================================================================
    # 6. AgentDrivenStrategyEngine 端到端集成与工厂测试
    # ==============================================================================

    async def test_engine_factory_registration(self):
        """测试 agent_driven 策略在工厂中注册为一等公民"""
        self.assertIn("agent_driven", SUPPORTED_SCHEDULE_IDS)
        self.assertIn("agent_driven", SCHEDULE_REGISTRY)

        engine = StrategyEngineFactory.get_engine("agent_driven")
        self.assertIsInstance(engine, AgentDrivenStrategyEngine)
        self.assertTrue(StrategyEngineFactory.is_valid_schedule_id("agent_driven"))

    async def test_engine_calculate_inflow_first_call(self):
        """测试新线索流入首呼推演"""
        engine = AgentDrivenStrategyEngine(self.config)
        mock_ctx = MagicMock(spec=ClueContext)
        mock_ctx.clue_id = "clue_new_01"
        mock_ctx.total_call_count = 0
        mock_ctx.call_records = []
        mock_ctx.customer_date = None
        mock_ctx.call_end_time = datetime(2026, 10, 29, 14, 30, 0, tzinfo=CST)

        res = await engine.calculate(mock_ctx)
        self.assertTrue(res.success)
        self.assertIn("工作时间内新线索即时外呼", res.reason)

    async def test_engine_calculate_post_call_with_fallback(self):
        """测试无模型或异常时，Engine 自动触发方案 A 启发式兜底并产出有效排期"""
        engine = AgentDrivenStrategyEngine(self.config)
        mock_ctx = MagicMock(spec=ClueContext)
        mock_ctx.clue_id = "clue_post_01"
        mock_ctx.total_call_count = 1
        mock_ctx.continuous_no_answer_count = 1
        mock_ctx.customer_date = None
        mock_ctx.call_end_time = datetime(2026, 10, 29, 15, 0, 0, tzinfo=CST)

        # 模拟通话记录
        record = MagicMock()
        record.answered = 0
        record.outcome = "no_answer"
        record.duration = 0
        record.config_snapshot = {}
        mock_ctx.call_records = [record]

        res = await engine.calculate(mock_ctx)
        self.assertTrue(res.success)
        self.assertIsNotNone(res.next_call_time)
        self.assertIn("降级", res.reason)
        self.assertTrue(is_within_working_window(res.next_call_time))


if __name__ == "__main__":
    unittest.main()
