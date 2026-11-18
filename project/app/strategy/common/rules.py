from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, date, timedelta, time
from typing import Optional, Dict, Any

from app.utils.time_utils import ensure_tz_aware
from app.strategy.common.models import CallOutcome, InflowSlot


class TimeRule(ABC):
    """时间规则抽象基类 (上下文驱动计算)"""
    use_work_phone: bool = False

    @abstractmethod
    def calculate(self, context_data: dict[str, Any]) -> Optional[datetime]:
        pass

    def check_use_work_phone(self, context_data: dict[str, Any]) -> bool:
        """判断当前规则是否指定使用工作手机"""
        return self.use_work_phone


class Relative(TimeRule):
    """相对时间规则 (例如: 参考时间 + 30分钟)"""
    def __init__(self, use_work_phone: bool = False, **kwargs):
        self.use_work_phone = use_work_phone
        self.delta = timedelta(**kwargs)

    def calculate(self, context_data: dict[str, Any]) -> datetime:
        base_time = context_data.get("reference_time") or datetime.now()
        base_time = ensure_tz_aware(base_time)
        return ensure_tz_aware(base_time + self.delta)


class Absolute(TimeRule):
    """绝对时间规则 (例如: 进件基准日 + days_offset 的 10:30)"""
    def __init__(self, days_offset: int, hour: int, minute: int = 0, use_work_phone: bool = False):
        self.use_work_phone = use_work_phone
        self.days = days_offset
        self.hour = hour
        self.minute = minute

    def calculate(self, context_data: dict[str, Any]) -> datetime:
        anchor_val = context_data.get("anchor_date") or date.today()
        if isinstance(anchor_val, datetime):
            anchor_date = anchor_val.date()
        else:
            anchor_date = anchor_val

        target_date = anchor_date + timedelta(days=self.days)
        target_dt = datetime.combine(target_date, time(self.hour, self.minute))
        return ensure_tz_aware(target_dt)


class Immediate(TimeRule):
    """立即呼叫规则 (推入定时任务队列，通常保障1分钟内拨打，默认延迟10秒入池)"""
    def __init__(self, delay_seconds: int = 10, use_work_phone: bool = False):
        self.use_work_phone = use_work_phone
        self.delay_seconds = delay_seconds

    def calculate(self, context_data: dict[str, Any]) -> datetime:
        base_time = context_data.get("reference_time") or datetime.now()
        base_time = ensure_tz_aware(base_time)
        return ensure_tz_aware(base_time + timedelta(seconds=self.delay_seconds))


class Reject(TimeRule):
    """拒绝排期规则 (返回 None，代表阻断)"""
    def calculate(self, context_data: dict[str, Any]) -> None:
        return None


class OutcomeBranch(TimeRule):
    """根据上一通电话的通话意图/结果（CallOutcome）进行多分支排期决策"""
    def __init__(self, branches: Dict[CallOutcome, TimeRule], default: Optional[TimeRule] = None):
        self.branches = branches
        self.default = default

    def _resolve_rule(self, outcome: CallOutcome) -> Optional[TimeRule]:
        if outcome in self.branches:
            return self.branches[outcome]
        # 兼容降级：如果是 UNANSWERED_NEED_WORK_PHONE 但分支中未单独配置，自动使用 UNANSWERED 分支规则
        if outcome == CallOutcome.UNANSWERED_NEED_WORK_PHONE and CallOutcome.UNANSWERED in self.branches:
            return self.branches[CallOutcome.UNANSWERED]
        if self.default is not None:
            return self.default
        # 兜底防御：若未配置 default 且 outcome 未命中任何分支，回退至 UNANSWERED 分支，避免异常阻断
        return self.branches.get(CallOutcome.UNANSWERED)

    def calculate(self, context_data: dict[str, Any]) -> Optional[datetime]:
        outcome: CallOutcome = context_data.get("last_call_outcome", CallOutcome.UNANSWERED)
        rule = self._resolve_rule(outcome)
        if not rule:
            return None
        return rule.calculate(context_data)

    def check_use_work_phone(self, context_data: dict[str, Any]) -> bool:
        outcome: CallOutcome = context_data.get("last_call_outcome", CallOutcome.UNANSWERED)
        # 500/501 明确要求使用工作手机
        if outcome == CallOutcome.UNANSWERED_NEED_WORK_PHONE:
            return True
        rule = self._resolve_rule(outcome)
        if not rule:
            return False
        return rule.check_use_work_phone(context_data)


class InflowSlotBranch(TimeRule):
    """根据线索进件时段（工作时段 / 晚间 / 凌晨）进行分支决策"""
    def __init__(self, branches: Dict[str | InflowSlot, TimeRule], default: Optional[TimeRule] = None):
        self.branches = branches
        self.default = default

    def calculate(self, context_data: dict[str, Any]) -> Optional[datetime]:
        slot_name = context_data.get("inflow_slot")
        rule = self.branches.get(slot_name, self.default)
        if not rule:
            return None
        return rule.calculate(context_data)

    def check_use_work_phone(self, context_data: dict[str, Any]) -> bool:
        slot_name = context_data.get("inflow_slot")
        rule = self.branches.get(slot_name, self.default)
        if not rule:
            return False
        return rule.check_use_work_phone(context_data)
