from __future__ import annotations

import logging
from datetime import datetime, time, timedelta
from typing import Optional, TYPE_CHECKING

from app.strategy.contracts.base import (
    BaseStrategyConfig,
    BaseStrategyEngine,
    ScheduleResult,
)
from app.strategy.single_call.config import SingleCallStrategyConfig
from app.utils.time_utils import ensure_tz_aware, get_now

if TYPE_CHECKING:
    from app.strategy.contracts.context import ClueContext

logger = logging.getLogger(__name__)


class SingleCallStrategyEngine(BaseStrategyEngine):
    """
    单通外呼结束策略引擎：
    1. 首呼逻辑：
       - 客户预约时间有效且在工作时间内时：预约优先；
       - 线索在工作时间内 (09:00 - 21:00) 流入：安排即刻外呼；
       - 线索在非工作时间 (夜间/清晨) 流入：排期顺延至次日开工时刻 (09:00:00)；
    2. 呼毕即结：
       - 只要 total_call_count >= 1 或已有通话记录，无论是否接通、无论意向好坏，一律拒绝排期并结案。
    """

    def __init__(self, config: Optional[SingleCallStrategyConfig] = None):
        self._config = config or SingleCallStrategyConfig()

    @property
    def strategy_config(self) -> SingleCallStrategyConfig:
        return self._config

    def _parse_time(self, time_str: str) -> time:
        parts = [int(p) for p in time_str.split(":")]
        if len(parts) == 2:
            return time(parts[0], parts[1], 0)
        return time(parts[0], parts[1], parts[2])

    def is_within_window(self, dt: datetime) -> bool:
        """判断给定时间点是否处于合法外呼工时窗口内"""
        start = self._parse_time(self._config.work_start_time)
        end = self._parse_time(self._config.work_end_time)
        return start <= dt.time() < end

    def get_next_work_start_time(self, dt: datetime) -> datetime:
        """获取下一个工作窗口的起始时间点"""
        start = self._parse_time(self._config.work_start_time)
        if dt.time() < start:
            # 当日尚未开工，顺延至当日开工时间
            return dt.replace(hour=start.hour, minute=start.minute, second=start.second, microsecond=0)
        else:
            # 当日已过开工时间或夜间，顺延至次日开工时间
            next_day = dt + timedelta(days=1)
            return next_day.replace(hour=start.hour, minute=start.minute, second=start.second, microsecond=0)

    @staticmethod
    def _is_record_connected(record: Any) -> bool:
        if getattr(record, "answered", None) == 1:
            events = getattr(record, "common_call_events", None) or {}
            if isinstance(events, dict) and events.get("is_voicemail") == "yes":
                return False
            return True
        return False

    async def calculate(self, context: ClueContext) -> ScheduleResult:
        """
        执行单通策略推演计算：
        1. 检查历史通话记录，若已真正接通真人，则达成目标拒绝二次排期并结案；
        2. 熔断检查：达到最大外呼次数限制 (max_call_num) 或连续未接通上限 (noanswer_num) 结案；
        3. 生命周期熔断：超出生命周期限制结案；
        4. 客户约定时间优先；
        5. 首呼与未接通重试排期。
        """
        clue_id = context.clue_id
        records = context.call_records if isinstance(getattr(context, "call_records", None), list) else []
        total_calls = (
            context.total_call_count
            if isinstance(getattr(context, "total_call_count", None), int)
            else len(records)
        )

        # 1. 核心判定：若已有通话真正打通，直接达成目标结案
        if any(self._is_record_connected(r) for r in records):
            reason = "单通策略完成：线索已成功打通，目标达成结案"
            logger.info(f"[SingleCallStrategyEngine] clue_id={clue_id}: {reason}")
            return ScheduleResult.reject(reason)

        # 2. 最大外呼次数限制
        if total_calls >= self._config.max_call_num:
            reason = f"单通策略完成：线索已执行过 {total_calls} 次外呼，无论是否接通均结案"
            logger.info(f"[SingleCallStrategyEngine] clue_id={clue_id}: {reason}")
            return ScheduleResult.reject(reason)

        # 3. 连续未接通上限熔断
        raw_continuous = getattr(context, "continuous_no_answer_count", None)
        continuous_no_answer = raw_continuous if isinstance(raw_continuous, int) else total_calls
        if continuous_no_answer >= self._config.noanswer_num and total_calls > 0:
            reason = f"单通策略连续未接通达到上限 ({self._config.noanswer_num} 次)，停止跟进并结案"
            logger.info(f"[SingleCallStrategyEngine] clue_id={clue_id}: {reason}")
            return ScheduleResult.reject(reason)

        # 4. 生命周期熔断判定
        raw_call_end = getattr(context, "call_end_time", None)
        call_end_time = raw_call_end if isinstance(raw_call_end, datetime) else None
        now = ensure_tz_aware(call_end_time or get_now())

        raw_intake = getattr(context, "clue_intake_time", None)
        intake_time = ensure_tz_aware(raw_intake) if isinstance(raw_intake, datetime) else now
        if (now - intake_time) > timedelta(hours=self._config.call_max_time):
            reason = f"超出单通最大生命周期限制 ({self._config.call_max_time} 小时)，停止跟进并结案"
            logger.info(f"[SingleCallStrategyEngine] clue_id={clue_id}: {reason}")
            return ScheduleResult.reject(reason)

        # 5. 客户主动约定时间优先
        raw_customer_date = getattr(context, "customer_date", None)
        customer_date = raw_customer_date if isinstance(raw_customer_date, datetime) else None
        if customer_date:
            appt_time = ensure_tz_aware(customer_date)
            if self.is_within_window(appt_time):
                reason = f"客户约定时间优先生效: {appt_time}"
                logger.info(f"[SingleCallStrategyEngine] clue_id={clue_id}: {reason}")
                return ScheduleResult.schedule(
                    call_time=appt_time,
                    priority=6,
                    reason=reason,
                    use_work_phone=False,
                )
            else:
                next_start = self.get_next_work_start_time(appt_time)
                reason = f"客户约定时间 {appt_time} 在非工作时间，顺延至下一个工时起始 {next_start}"
                logger.info(f"[SingleCallStrategyEngine] clue_id={clue_id}: {reason}")
                return ScheduleResult.schedule(
                    call_time=next_start,
                    priority=5,
                    reason=reason,
                    use_work_phone=False,
                )

        # 6. 首呼排期判定
        if total_calls == 0:
            if self.is_within_window(intake_time):
                reason = "工作时间内首呼即时外呼"
                logger.info(f"[SingleCallStrategyEngine] clue_id={clue_id}: {reason}, call_time={intake_time}")
                return ScheduleResult.schedule(
                    call_time=intake_time,
                    priority=6,
                    reason=reason,
                    use_work_phone=False,
                )
            else:
                next_start = self.get_next_work_start_time(intake_time)
                reason = "非工作时间顺延至次日开工外呼"
                logger.info(f"[SingleCallStrategyEngine] clue_id={clue_id}: {reason}, call_time={next_start}")
                return ScheduleResult.schedule(
                    call_time=next_start,
                    priority=5,
                    reason=reason,
                    use_work_phone=False,
                )

        # 7. 未接通重试排期 (total_calls > 0)
        ref_time = ensure_tz_aware(call_end_time or get_now())
        interval = getattr(self._config, "retry_interval_minutes", 60)
        retry_time = ref_time + timedelta(minutes=interval)
        if self.is_within_window(retry_time):
            reason = f"单通未接通重试排期: {retry_time}"
            logger.info(f"[SingleCallStrategyEngine] clue_id={clue_id}: {reason}")
            return ScheduleResult.schedule(
                call_time=retry_time,
                priority=5,
                reason=reason,
                use_work_phone=False,
            )
        else:
            next_start = self.get_next_work_start_time(retry_time)
            reason = f"单通未接通重试处于非工作时间，顺延至下一个工时起始: {next_start}"
            logger.info(f"[SingleCallStrategyEngine] clue_id={clue_id}: {reason}")
            return ScheduleResult.schedule(
                call_time=next_start,
                priority=5,
                reason=reason,
                use_work_phone=False,
            )
