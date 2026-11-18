import pytest
from datetime import datetime, date, timedelta, time
from unittest.mock import MagicMock, patch, AsyncMock

from app.strategy.sevenday import (
    SevenDayStrategyConfig,
    SevenDayStrategyEngine,
    SEVENDAY_MATRIX,
)
from app.strategy.factory import (
    StrategyEngineFactory,
    SUPPORTED_SCHEDULE_IDS,
    SCHEDULE_REGISTRY,
)
from app.strategy.contracts import ClueContext
from app.utils.time_utils import ensure_tz_aware


def build_sevenday_ctx(
    clue_id: str = "TEST_SEVENDAY_001",
    intake_time: datetime = None,
    call_end_time: datetime = None,
    total_call_count: int = 0,
    continuous_no_answer_count: int = 0,
    customer_date: datetime = None,
    last_one_code: int = 500,
    last_two_code: int = 508,
) -> ClueContext:
    ctx = MagicMock(spec=ClueContext)
    ctx.clue_id = clue_id
    ctx.clue_intake_time = intake_time
    ctx.call_end_time = call_end_time
    ctx.total_call_count = total_call_count
    ctx.continuous_no_answer_count = continuous_no_answer_count
    ctx.customer_date = customer_date
    if last_one_code and last_two_code:
        record = MagicMock()
        record.oneLevelCode = str(last_one_code)
        record.twoLevelCode = str(last_two_code)
        ctx.call_records = [record]
    else:
        ctx.call_records = []
    return ctx


class TestSevenDayStrategyEngine:
    @pytest.fixture
    def engine(self):
        return SevenDayStrategyEngine()

    def test_factory_standard_naming_and_rejection_of_legacy_aliases(self):
        """验证策略工厂的标准命名 (snake_case) 及严格拒绝历史兼容别名"""
        # 1. 验证 SUPPORTED_SCHEDULE_IDS 包含标准名称 (含 agent_driven)
        expected_standards = ["single_call", "seven_day_three_call", "agent_driven"]
        assert SUPPORTED_SCHEDULE_IDS == expected_standards
        assert set(SCHEDULE_REGISTRY.keys()) == set(expected_standards)

        # 2. 验证标准名称获取引擎
        engine_std = StrategyEngineFactory.get_engine("seven_day_three_call")
        assert isinstance(engine_std, SevenDayStrategyEngine)
        assert StrategyEngineFactory.is_valid_schedule_id("seven_day_three_call") is True

        # 3. 验证历史别名或未注册名称被严格拒绝，抛出 ValueError
        invalid_aliases = [
            "sevenday",
            "seven_day",
            "sevenday-common-1",
            "threeday-common-1",
            "threeday-workphone-1",
            "noworkphone",
            "workphone",
        ]
        for bad_id in invalid_aliases:
            assert StrategyEngineFactory.is_valid_schedule_id(bad_id) is False
            with pytest.raises(ValueError) as excinfo:
                StrategyEngineFactory.get_engine(bad_id)
            assert "未知或不支持的排期策略" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_round1_working_hours(self, engine):
        """工作时段 (09:00-21:00) 首呼：立即呼叫 (10s 延迟)，优先级 6"""
        base_time = ensure_tz_aware(datetime(2026, 10, 26, 10, 0, 0))  # Monday 10:00
        ctx = build_sevenday_ctx(intake_time=base_time, call_end_time=base_time, total_call_count=0)
        res = await engine.calculate(ctx)
        assert res.success is True
        assert res.priority == 6
        assert res.next_call_time == base_time + timedelta(seconds=10)

    @pytest.mark.asyncio
    async def test_round1_evening_non_working_hours(self, engine):
        """晚间非工作时段 (21:00-23:59) 首呼：次日 09:30，优先级 5"""
        base_time = ensure_tz_aware(datetime(2026, 10, 26, 21, 30, 0))
        ctx = build_sevenday_ctx(intake_time=base_time, call_end_time=base_time, total_call_count=0)
        res = await engine.calculate(ctx)
        assert res.success is True
        assert res.priority == 5
        expected = ensure_tz_aware(datetime(2026, 10, 27, 9, 30, 0))
        assert res.next_call_time == expected

    @pytest.mark.asyncio
    async def test_round1_midnight_hours(self, engine):
        """凌晨时段 (00:00-08:59) 首呼：当日早晨 09:10，优先级 5"""
        base_time = ensure_tz_aware(datetime(2026, 10, 26, 2, 0, 0))
        ctx = build_sevenday_ctx(intake_time=base_time, call_end_time=base_time, total_call_count=0)
        res = await engine.calculate(ctx)
        assert res.success is True
        assert res.priority == 5
        expected = ensure_tz_aware(datetime(2026, 10, 26, 9, 10, 0))
        assert res.next_call_time == expected

    @pytest.mark.asyncio
    async def test_day0_rounds_2_and_3_interval(self, engine):
        """Day 0 第 2 呼与第 3 呼：每次间隔 3 小时"""
        intake_time = ensure_tz_aware(datetime(2026, 10, 26, 10, 0, 0))

        # 1. 第 2 呼推演 (上一通 10:05 结束)
        call1_end = ensure_tz_aware(datetime(2026, 10, 26, 10, 5, 0))
        ctx_r2 = build_sevenday_ctx(intake_time=intake_time, call_end_time=call1_end, total_call_count=1)
        res_r2 = await engine.calculate(ctx_r2)
        assert res_r2.success is True
        assert res_r2.next_call_time == call1_end + timedelta(hours=3)
        assert res_r2.next_call_time == ensure_tz_aware(datetime(2026, 10, 26, 13, 5, 0))

        # 2. 第 3 呼推演 (第 2 呼 13:10 结束)
        call2_end = ensure_tz_aware(datetime(2026, 10, 26, 13, 10, 0))
        ctx_r3 = build_sevenday_ctx(intake_time=intake_time, call_end_time=call2_end, total_call_count=2)
        res_r3 = await engine.calculate(ctx_r3)
        assert res_r3.success is True
        assert res_r3.next_call_time == call2_end + timedelta(hours=3)
        assert res_r3.next_call_time == ensure_tz_aware(datetime(2026, 10, 26, 16, 10, 0))

    @pytest.mark.asyncio
    async def test_day0_evening_skip_to_day1_morning(self, engine):
        """Day 0 傍晚流入导致次呼超出 21:00 时：自动作废并跳步至 Day 1 09:30"""
        intake_time = ensure_tz_aware(datetime(2026, 10, 26, 19, 0, 0))
        call1_end = ensure_tz_aware(datetime(2026, 10, 26, 19, 5, 0))

        # 第 2 呼推算为 19:05 + 3h = 22:05 (超出 21:00) -> 作废跳步
        # 第 3 呼推算为 22:05 + 3h = 01:05 (超出 21:00) -> 作废跳步
        # 推进至第 4 呼 (Day 1 09:30)
        ctx = build_sevenday_ctx(intake_time=intake_time, call_end_time=call1_end, total_call_count=1)
        with patch("app.dbmodels.crud.increment_total_call_count", new_callable=AsyncMock):
            res = await engine.calculate(ctx)

        assert res.success is True
        expected_day1_start = ensure_tz_aware(datetime(2026, 10, 27, 9, 30, 0))
        assert res.next_call_time == expected_day1_start

    @pytest.mark.asyncio
    async def test_day1_day2_day7_rhythm(self, engine):
        """验证跨日节拍：Day 1、Day 2、Day 7 每日 3 呼且间隔 3 小时"""
        intake_time = ensure_tz_aware(datetime(2026, 10, 26, 10, 0, 0))

        # Day 1 首呼 (Round 4) -> 10/27 09:30
        call3_end = ensure_tz_aware(datetime(2026, 10, 26, 16, 10, 0))
        ctx_r4 = build_sevenday_ctx(intake_time=intake_time, call_end_time=call3_end, total_call_count=3)
        res_r4 = await engine.calculate(ctx_r4)
        assert res_r4.next_call_time == ensure_tz_aware(datetime(2026, 10, 27, 9, 30, 0))

        # Day 2 首呼 (Round 7) -> 10/28 09:30
        call6_end = ensure_tz_aware(datetime(2026, 10, 27, 16, 0, 0))
        ctx_r7 = build_sevenday_ctx(intake_time=intake_time, call_end_time=call6_end, total_call_count=6)
        res_r7 = await engine.calculate(ctx_r7)
        assert res_r7.next_call_time == ensure_tz_aware(datetime(2026, 10, 28, 9, 30, 0))

        # Day 7 首呼 (Round 10) -> 11/02 09:30 (中间第 3~6 日静默)
        call9_end = ensure_tz_aware(datetime(2026, 10, 28, 16, 0, 0))
        ctx_r10 = build_sevenday_ctx(intake_time=intake_time, call_end_time=call9_end, total_call_count=9)
        res_r10 = await engine.calculate(ctx_r10)
        assert res_r10.next_call_time == ensure_tz_aware(datetime(2026, 11, 2, 9, 30, 0))

        # Day 7 第 11 呼 -> 11/02 12:30
        call10_end = ensure_tz_aware(datetime(2026, 11, 2, 9, 35, 0))
        ctx_r11 = build_sevenday_ctx(intake_time=intake_time, call_end_time=call10_end, total_call_count=10)
        res_r11 = await engine.calculate(ctx_r11)
        assert res_r11.next_call_time == ensure_tz_aware(datetime(2026, 11, 2, 12, 35, 0))

        # Day 7 第 12 呼 -> 11/02 15:40
        call11_end = ensure_tz_aware(datetime(2026, 11, 2, 12, 40, 0))
        ctx_r12 = build_sevenday_ctx(intake_time=intake_time, call_end_time=call11_end, total_call_count=11)
        res_r12 = await engine.calculate(ctx_r12)
        assert res_r12.next_call_time == ensure_tz_aware(datetime(2026, 11, 2, 15, 40, 0))

    @pytest.mark.asyncio
    async def test_consecutive_no_answer_circuit_break(self, engine):
        """验证连续未接通熔断：连续 5 次未接通终止排期"""
        base_time = ensure_tz_aware(datetime(2026, 10, 26, 10, 0, 0))

        # 4 次未接通 -> 允许排期
        ctx_ok = build_sevenday_ctx(
            intake_time=base_time,
            call_end_time=base_time,
            total_call_count=4,
            continuous_no_answer_count=4,
        )
        res_ok = await engine.calculate(ctx_ok)
        assert res_ok.success is True

        # 5 次连续未接通 -> 触发熔断
        ctx_reject = build_sevenday_ctx(
            intake_time=base_time,
            call_end_time=base_time,
            total_call_count=5,
            continuous_no_answer_count=5,
        )
        res_reject = await engine.calculate(ctx_reject)
        assert res_reject.success is False
        assert "连续未接通次数(5)超限" in res_reject.reason

    @pytest.mark.asyncio
    async def test_max_call_num_limit(self, engine):
        """验证最大外呼次数限制：累计达 12 次终止排期"""
        base_time = ensure_tz_aware(datetime(2026, 10, 26, 10, 0, 0))

        ctx = build_sevenday_ctx(
            intake_time=base_time,
            call_end_time=base_time,
            total_call_count=12,
            continuous_no_answer_count=0,
        )
        res = await engine.calculate(ctx)
        assert res.success is False
        assert "达到最大外呼次数限制: 12次" in res.reason

    @pytest.mark.asyncio
    async def test_day7_lifecycle_cutoff(self, engine):
        """验证生命周期防线：超过第 7 日晚间工时窗 (21:00) 拒绝排期"""
        intake_time = ensure_tz_aware(datetime(2026, 10, 26, 10, 0, 0))
        # 模拟上一通电话在 Day 8 发生
        call_late = ensure_tz_aware(datetime(2026, 11, 3, 10, 0, 0))
        ctx = build_sevenday_ctx(
            intake_time=intake_time,
            call_end_time=call_late,
            total_call_count=11,
            continuous_no_answer_count=0,
        )
        res = await engine.calculate(ctx)
        assert res.success is False
        assert "超出第 7 日生命周期限制" in res.reason

    @pytest.mark.asyncio
    async def test_customer_appointment_priority(self, engine):
        """验证客户预约在工时窗内优先生效"""
        intake_time = ensure_tz_aware(datetime(2026, 10, 26, 10, 0, 0))
        appt_time = ensure_tz_aware(datetime(2026, 10, 29, 14, 30, 0))  # Day 3 (处于静默期，但预约优先生效)

        ctx = build_sevenday_ctx(
            intake_time=intake_time,
            call_end_time=intake_time,
            total_call_count=6,
            customer_date=appt_time,
        )
        res = await engine.calculate(ctx)
        assert res.success is True
        assert res.next_call_time == appt_time
        assert "合法客户预约优先生效" in res.reason
