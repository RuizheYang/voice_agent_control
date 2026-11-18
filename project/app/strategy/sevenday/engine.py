"""
策略引擎模块 - 七日矩阵 (seven_day_three_call)
实现【外呼排期策略 - 第 0、1、2、7 日每日三呼】规则：
- 第 0、1、2、7 日外呼，外呼日每日 3 呼，每次间隔 3 小时；
- 连续 5 次未接通熔断，最大呼叫 12 次；
- 单通最大跟进周期 7 天 (168h)；
- 严格限制在工作时段 (09:00 - 21:00) 拨打。
"""
from __future__ import annotations

import logging
from datetime import datetime, time, timedelta
from typing import Optional, TYPE_CHECKING

from app.strategy.contracts.base import BaseStrategyEngine, ScheduleResult
from app.strategy.sevenday.config import (
    SevenDayStrategyConfig,
    SEVENDAY_MATRIX,
    SEVENDAY_WORKING_PRIORITY,
    SEVENDAY_NON_WORKING_PRIORITY,
)
from app.strategy.common.models import CallOutcome, CallOutcomeClassifier, InflowSlot
from app.utils.time_utils import ensure_tz_aware, get_now

if TYPE_CHECKING:
    from app.strategy.contracts.context import ClueContext

logger = logging.getLogger(__name__)


class SevenDayStrategyEngine(BaseStrategyEngine):
    """七日 (第 0、1、2、7 日每日三呼) 外呼排期核心引擎"""

    def __init__(
        self,
        config: Optional[SevenDayStrategyConfig] = None,
        matrix: Optional[dict] = None,
    ):
        self._config = config or SevenDayStrategyConfig()
        self._matrix = matrix if matrix is not None else SEVENDAY_MATRIX

    @property
    def strategy_config(self) -> SevenDayStrategyConfig:
        return self._config

    @property
    def strategy_matrix(self) -> dict:
        return self._matrix

    def is_within_window(self, target_time: datetime) -> bool:
        """判断给定时间是否在允许的工作时间窗内 (09:00 - 21:00)"""
        target_t = target_time.time()
        start_t = ensure_tz_aware(datetime.strptime(self.strategy_config.work_start_time, "%H:%M:%S")).time()
        end_t = ensure_tz_aware(datetime.strptime(self.strategy_config.work_end_time, "%H:%M:%S")).time()
        return start_t <= target_t <= end_t

    def get_inflow_slot(self, intake_time: time) -> InflowSlot:
        """
        依据线索流入时刻划分时段：
        - 工作时段 (09:00–21:00)
        - 晚间非工作时段 (21:00–23:59)
        - 凌晨时段 (00:00–08:59)
        """
        if time(9, 0) <= intake_time < time(21, 0):
            return InflowSlot.WORKING
        elif time(21, 0) <= intake_time <= time(23, 59, 59):
            return InflowSlot.EVENING
        else:
            return InflowSlot.MIDNIGHT

    def _get_priority(self, in_time: time, round_num: int) -> int:
        """根据进件时段与呼叫轮次统一计算外呼优先级"""
        is_working_slot = (self.get_inflow_slot(in_time) == InflowSlot.WORKING)
        p_dict = (
            getattr(self.strategy_config, "priority_dict", SEVENDAY_WORKING_PRIORITY)
            if is_working_slot
            else getattr(self.strategy_config, "non_working_priority_dict", SEVENDAY_NON_WORKING_PRIORITY)
        )
        return p_dict.get(round_num, 1)

    def _check_global_red_lines(self, context: ClueContext) -> Optional[ScheduleResult]:
        """检查全局熔断红线 (连续未接通 5 次熔断，最大 12 次外呼)"""
        if context.total_call_count >= self.strategy_config.max_call_num:
            logger.info(
                f"[SevenDayStrategy] [{context.clue_id}] 达到最大外呼次数限制: {self.strategy_config.max_call_num}次"
            )
            return ScheduleResult.reject(f"达到最大外呼次数限制: {self.strategy_config.max_call_num}次")

        if context.continuous_no_answer_count >= self.strategy_config.noanswer_num:
            logger.info(
                f"[SevenDayStrategy] [{context.clue_id}] 连续未接通次数({context.continuous_no_answer_count})超限(上限{self.strategy_config.noanswer_num}次)"
            )
            return ScheduleResult.reject(
                f"连续未接通次数({context.continuous_no_answer_count})超限(上限{self.strategy_config.noanswer_num}次)"
            )

        return None

    def _check_customer_appointment(self, context: ClueContext) -> Optional[ScheduleResult]:
        """处理客户主动预约排期逻辑"""
        if not context.customer_date:
            return None

        safe_intake = ensure_tz_aware(context.clue_intake_time)
        safe_appt = ensure_tz_aware(context.customer_date)

        # 客户预约时间超出 7 天生命周期红线
        if safe_intake and (safe_appt - safe_intake) > timedelta(hours=self.strategy_config.call_max_time + 12):
            logger.info(
                f"[SevenDayStrategy] [{context.clue_id}] 客户预约时间({context.customer_date})超出生命周期红线"
            )
            return ScheduleResult.reject(f"客户预约时间({context.customer_date})超出生命周期红线")

        if self.is_within_window(context.customer_date):
            clue_intake_wall_clock_time = (
                context.clue_intake_time.time()
                if context.clue_intake_time
                else context.call_end_time.time()
            )
            priority = self._get_priority(clue_intake_wall_clock_time, context.total_call_count + 1)
            logger.info(f"[SevenDayStrategy] [{context.clue_id}] 客户预约时间({context.customer_date}) 合法客户预约优先生效")
            return ScheduleResult.schedule(context.customer_date, priority, reason="合法客户预约优先生效")

        logger.info(f"[SevenDayStrategy] [{context.clue_id}] 预约时间({context.customer_date})处于非工作时段，降级走大盘策略")
        return None

    async def calculate(self, context: ClueContext) -> ScheduleResult:
        """主控排期推演流程 (0-1-2-7 矩阵推演)"""
        from app.dbmodels import crud

        ref_time = ensure_tz_aware(context.call_end_time or get_now())
        intake_dt = ensure_tz_aware(context.clue_intake_time or ref_time)
        intake_date = intake_dt.date()
        inflow_slot = self.get_inflow_slot(intake_dt.time())

        logger.info(
            f"[SevenDayStrategy] 开始排期计算, clue_id={context.clue_id}, "
            f"已呼次数={context.total_call_count}, 进件时段={inflow_slot.value}, 参考时间={ref_time}"
        )

        # 1. 全局熔断检查
        red_line_result = self._check_global_red_lines(context)
        if red_line_result:
            return red_line_result

        # 2. 客户主动约定时间优先生效
        appointment_result = self._check_customer_appointment(context)
        if appointment_result:
            return appointment_result

        # 3. 提炼推演上下文
        last_record = context.call_records[0] if context.call_records else None
        if last_record:
            last_outcome = CallOutcomeClassifier.classify(last_record)
        elif context.total_call_count > 0:
            last_outcome = CallOutcome.UNANSWERED
        else:
            last_outcome = CallOutcome.INITIAL

        elapsed_days = (ref_time.date() - intake_date).days
        current_round_number = context.total_call_count + 1
        current_reference_time = ref_time

        # 4. 轮次推演循环 (最高推演至 12 轮)
        while current_round_number <= self.strategy_config.max_call_num:
            if current_round_number not in self.strategy_matrix:
                logger.info(f"[SevenDayStrategy] [{context.clue_id}] 请求的第 {current_round_number} 轮排期超出矩阵配置")
                return ScheduleResult.reject(f"请求的第 {current_round_number} 轮排期超出矩阵配置")

            current_round_rule = self.strategy_matrix[current_round_number]

            calc_context = {
                "reference_time": current_reference_time,
                "anchor_date": intake_date,
                "inflow_slot": inflow_slot,
                "last_call_outcome": last_outcome,
                "elapsed_days": elapsed_days,
            }

            next_scheduled_time = current_round_rule.calculate(calc_context)
            priority = self._get_priority(intake_dt.time(), current_round_number)

            if next_scheduled_time is None:
                logger.info(f"[SevenDayStrategy] [{context.clue_id}] 命中第 {current_round_number} 轮的阻断(Reject)规则")
                return ScheduleResult.reject(f"命中第 {current_round_number} 轮的阻断(Reject)规则")

            # 矩阵规则跳步判定 1：排期早于当前时间，跳步并累加轮次
            if next_scheduled_time <= ref_time - timedelta(minutes=1):
                logger.info(
                    f"[SevenDayStrategy] [{context.clue_id}] 第 {current_round_number} 轮排期 ({next_scheduled_time}) "
                    f"早于当前时间，跳过"
                )
                current_round_number += 1
                context.total_call_count += 1
                try:
                    await crud.increment_total_call_count(context.clue_id)
                except Exception as e:
                    logger.debug(f"increment_total_call_count ignored: {e}")
                continue

            # 矩阵规则跳步判定 2：落在营业窗外作废跳步 (如晚上 19:00 + 3h = 22:00 超出 21:00)
            if not self.is_within_window(next_scheduled_time):
                logger.info(
                    f"[SevenDayStrategy] [{context.clue_id}] 第 {current_round_number} 轮排期 ({next_scheduled_time}) "
                    f"落在工作时段之外 (09:00–21:00)，该次外呼作废，跳过"
                )
                current_round_number += 1
                context.total_call_count += 1
                current_reference_time = next_scheduled_time
                try:
                    await crud.increment_total_call_count(context.clue_id)
                except Exception as e:
                    logger.debug(f"increment_total_call_count ignored: {e}")
                continue

            # 大盘推演生命周期防线：不得超出第 7 日工作窗口截止时间 (anchor_date + 7天 21:00)
            safe_next = ensure_tz_aware(next_scheduled_time)
            day7_end = ensure_tz_aware(datetime.combine(intake_date + timedelta(days=7), time(21, 0, 0)))
            if safe_next > day7_end:
                logger.info(
                    f"[SevenDayStrategy] [{context.clue_id}] 推演至第 {current_round_number} 轮时超出第 7 日生命周期限制 ({day7_end})"
                )
                return ScheduleResult.reject(f"推演至第 {current_round_number} 轮时超出第 7 日生命周期限制")

            reason_desc = (
                f"命中七日(0-1-2-7)第 {current_round_number} 轮排期策略 "
                f"(进件={inflow_slot.value}, 间隔3h, 轮次={current_round_number}/12)"
            )
            return ScheduleResult.schedule(
                next_scheduled_time,
                priority,
                reason=reason_desc,
            )

        return ScheduleResult.reject("策略矩阵轮次全部耗尽，自然终止")


# 别名对齐
SevenDayThreeCallStrategyEngine = SevenDayStrategyEngine
