from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, time
from typing import Optional, Any

from app.strategy.contracts.base import BaseStrategyConfig
from app.strategy.common.models import InflowSlot
from app.strategy.common.rules import (
    TimeRule, Immediate, Relative, Absolute, InflowSlotBranch, Reject
)


# ── 七日策略 (0-1-2-7 每日三呼) 外呼优先级矩阵 ────────────────────────
SEVENDAY_WORKING_PRIORITY: dict[int, int] = {
    1: 6,
    2: 5,
    3: 4,
    4: 3,
    5: 2,
    6: 1,
    7: 1,
    8: 1,
    9: 1,
    10: 1,
    11: 1,
    12: 1,
}

SEVENDAY_NON_WORKING_PRIORITY: dict[int, int] = {
    1: 5,
    2: 4,
    3: 3,
    4: 3,
    5: 2,
    6: 1,
    7: 1,
    8: 1,
    9: 1,
    10: 1,
    11: 1,
    12: 1,
}


@dataclass
class SevenDayStrategyConfig(BaseStrategyConfig):
    """七日 (第 0、1、2、7 日每日三呼) 外呼工时与熔断配置"""
    work_start_time: str = "09:00:00"  # 外呼工作时段开始 (09:00)
    work_end_time: str = "21:00:00"    # 外呼工作时段结束 (21:00)
    max_call_num: int = 12             # 最大外呼总次数为 12 次 (4天 × 3呼)
    noanswer_num: int = 5              # 连续未接通熔断 5 次
    call_max_time: int = 168           # 单通最大跟进周期 7 天 (168小时)
    priority_dict: dict = field(
        default_factory=lambda: dict(SEVENDAY_WORKING_PRIORITY)
    )
    non_working_priority_dict: dict = field(
        default_factory=lambda: dict(SEVENDAY_NON_WORKING_PRIORITY)
    )


SEVENDAY_MATRIX: dict[int, TimeRule] = {
    # ── Day 0（流入日，每日三呼） ────────────────────────────────
    # 工作时段 (09:00–21:00): 立即呼叫 (保障10秒内入池)
    # 晚间非工作时段 (21:00–23:59): 次日 09:30
    # 凌晨时段 (00:00–08:59): 当日早晨 09:10
    1: InflowSlotBranch(branches={
        InflowSlot.WORKING: Immediate(delay_seconds=10),
        InflowSlot.EVENING: Absolute(days_offset=1, hour=9, minute=30),
        InflowSlot.MIDNIGHT: Absolute(days_offset=0, hour=9, minute=10),
    }),
    2: Relative(hours=3),
    3: Relative(hours=3),

    # ── Day 1（次日，每日三呼） ──────────────────────────────────
    # 次日上午开工 09:30 首呼，其后每隔 3 小时次呼
    4: Absolute(days_offset=1, hour=9, minute=30),
    5: Relative(hours=3),
    6: Relative(hours=3),

    # ── Day 2（第 2 日，每日三呼） ───────────────────────────────
    # 第 2 日上午开工 09:30 首呼，其后每隔 3 小时次呼
    7: Absolute(days_offset=2, hour=9, minute=30),
    8: Relative(hours=3),
    9: Relative(hours=3),

    # ── Day 7（第 7 日，每日三呼，中间第 3~6 日自然静默） ────────
    # 第 7 日上午开工 09:30 首呼，其后每隔 3 小时次呼
    10: Absolute(days_offset=7, hour=9, minute=30),
    11: Relative(hours=3),
    12: Relative(hours=3),
}
