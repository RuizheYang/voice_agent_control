from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from app.strategy.contracts.context import ClueContext


@dataclass
class ScheduleResult:
    """策略的统一排期结果契约"""
    success: bool = False
    next_call_time: Optional[datetime] = None
    priority: Optional[int] = None
    reason: str = ""
    use_work_phone: bool = False
    voice_agent_directive: Optional[str] = None
    business_status: Optional[str] = None
    master_decision: Optional[Any] = None

    @classmethod
    def schedule(
        cls,
        call_time: datetime,
        priority: int,
        reason: str,
        use_work_phone: bool = False,
        voice_agent_directive: Optional[str] = None,
        business_status: Optional[str] = None,
        master_decision: Optional[Any] = None,
    ) -> ScheduleResult:
        return cls(
            success=True,
            next_call_time=call_time,
            priority=priority,
            reason=reason,
            use_work_phone=use_work_phone,
            voice_agent_directive=voice_agent_directive,
            business_status=business_status,
            master_decision=master_decision,
        )

    @classmethod
    def reject(
        cls,
        reason: str,
        voice_agent_directive: Optional[str] = None,
        business_status: Optional[str] = None,
        master_decision: Optional[Any] = None,
    ) -> ScheduleResult:
        return cls(
            success=False,
            next_call_time=None,
            priority=None,
            reason=reason,
            use_work_phone=False,
            voice_agent_directive=voice_agent_directive,
            business_status=business_status,
            master_decision=master_decision,
        )


@dataclass
class BaseStrategyConfig:
    """策略配置基类，定义策略强制依赖的核心工时与熔断参数"""
    work_start_time: str = "09:00:00"  # 工作时间起始时刻
    work_end_time: str = "21:00:00"    # 工作时间结束时刻
    call_max_time: int = 48            # 线索生命周期最长限制 (小时)
    noanswer_num: int = 3              # 连续未接通次数熔断限制
    max_call_num: int = 6              # 最大外呼次数上限


class BaseStrategyEngine(ABC):
    """外呼排期引擎抽象基类 (所有新老版本策略引擎共同实现的顶层协议)"""

    @property
    @abstractmethod
    def strategy_config(self) -> BaseStrategyConfig:
        pass

    @abstractmethod
    def calculate(self, context: ClueContext) -> ScheduleResult:
        """
        核心策略排期算法入口
        :param context: 线索动态上下文 (包含呼叫历史、连续未接通指标、客户预约等)
        :return: ScheduleResult 排期结果契约 (包含 success, next_call_time, priority, use_work_phone, reason)
        """
        pass
