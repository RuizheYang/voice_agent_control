from __future__ import annotations

from dataclasses import dataclass
from app.strategy.contracts.base import BaseStrategyConfig


@dataclass
class SingleCallStrategyConfig(BaseStrategyConfig):
    """单通外呼策略配置：以打通一通为目标，未接通允许重试，真正接通即结案"""
    work_start_time: str = "09:00:00"  # 工作时间起始时刻
    work_end_time: str = "21:00:00"    # 工作时间结束时刻
    call_max_time: int = 24            # 单通最长生命周期限制 (小时)
    noanswer_num: int = 1              # 未接通熔断阈值
    max_call_num: int = 1              # 最大外呼总次数限制
    retry_interval_minutes: int = 60   # 未接通重试间隔 (分钟)
