from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Any, Optional, Dict
from pydantic import BaseModel, Field, model_validator, field_validator

from app.dbmodels import crud
from app.dbmodels.crud import get_clue_intake_time
from app.utils.time_utils import get_now, ensure_tz_aware

logger = logging.getLogger(__name__)


class ClueContext(BaseModel):
    """
    线索执行上下文模型 (Strategy Calculation Context)
    负责汇聚和动态推演单条线索在外呼策略执行中所依赖的通话历史、熔断统计与预约状态。
    """
    clue_id: str
    clue_intake_time: datetime
    call_records: List[Any]
    call_end_time: Optional[datetime] = Field(default_factory=lambda: get_now())  # 本次外呼结束时间
    total_call_count: int = 0  # 当前排期总次数
    continuous_no_answer_count: int = 0  # 当前连续未接通次数
    continuous_no_answer_hours: int = 0  # 当前连续未接通小时数
    customer_date: Optional[datetime] = None  # 客户主动约定的外呼时间
    memory_index: Optional[str] = None  # 结构化四桶记忆紧凑索引 (Index-First 加载)
    workflow_id: str = "default"  # 所属业务卡带 workflow_id，用于引擎正确加载业务卡带与工具集
    coalesced_events: List[Any] = Field(default_factory=list)  # 同线索事件合并窗口到达的外部事件
    strategy_followup: Dict[str, Any] = Field(default_factory=dict)  # 策略追呼配额参数 (maxCalls/maxNoAnswerCalls/maxFollowupHours)

    @property
    def first_call_no_answer(self) -> bool:
        return self.total_call_count == 1 and self.continuous_no_answer_count == 1

    @field_validator("customer_date", mode="before")
    @classmethod
    def pre_check_customer_date(cls, v):
        if not v:
            return None
        if isinstance(v, datetime):
            return v

        v_str = str(v).strip()

        # 核心兼容逻辑：定义支持的时间格式列表，依次尝试解析
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return ensure_tz_aware(datetime.strptime(v_str, fmt))
            except ValueError:
                continue

        # 如果循环结束都没解析成功，才触发拦截
        logger.info(f"[ClueContext] 预约时间容错拦截: 收到非法时间格式 '{v_str}'，已自动降级为 None 转大盘排期。")
        return None

    @model_validator(mode="after")
    def validate_and_calculate_metrics(self) -> ClueContext:
        """
        验证并更新指标。客户预约时间若早于当前系统时间，自动作废。
        """
        datenow = get_now()
        if self.customer_date and self.customer_date < datenow:
            logger.info(f"[ClueContext] 预约时间失效: 客户预约时间({self.customer_date}) 早于当前系统时间，该预约已过期，自动作废。")
            self.customer_date = None

        self.refresh_metrics()
        return self

    @classmethod
    async def from_db(cls, clue_id: str, customer_date: Optional[datetime] = None) -> ClueContext:
        """从数据库全量加载线索外呼历史并构建上下文"""
        call_records = await crud.get_call_history(clue_id)
        clue_intake_time = await get_clue_intake_time(clue_id)
        raw_customer_date = customer_date
        db_call_count = await crud.get_total_call_count(clue_id)
        clue = await crud.get_clue_info(clue_id)
        workflow_id = clue.workflow_id if clue and getattr(clue, "workflow_id", None) else "default"
        strategy_followup: Dict[str, Any] = {}
        if clue and isinstance(getattr(clue, "clue_config", None), dict) and isinstance(clue.clue_config.get("followup"), dict):
            strategy_followup = dict(clue.clue_config["followup"])
        elif clue and getattr(clue, "strategy_id", None):
            try:
                from app.dbmodels.models import OutboundStrategy
                st = await OutboundStrategy.filter(strategy_id=clue.strategy_id).first()
                if st and isinstance(st.followup, dict):
                    strategy_followup = dict(st.followup)
            except Exception:
                pass

        obj = cls(
            clue_id=clue_id,
            clue_intake_time=clue_intake_time,
            call_records=call_records,
            customer_date=raw_customer_date,
            total_call_count=db_call_count,
            workflow_id=workflow_id,
            strategy_followup=strategy_followup,
        )

        logger.info(
            f"[ClueContext] 线索:{clue_id} DB装载完成: 客户预约时间({obj.customer_date}), "
            f"接入流入时间({obj.clue_intake_time}), 总呼叫({obj.total_call_count}次)"
        )
        return obj

    def refresh_metrics(self, base_time: Optional[datetime] = None):
        """核心指标动态刷新 (连续未接通次数与小时数)"""
        datenow = get_now()
        if base_time is None:
            base_time = datenow

        if not self.call_records:
            self.continuous_no_answer_count = 0
            self.continuous_no_answer_hours = 0
            return

        continuous_count = 0
        streak_start_time: Optional[datetime] = None

        # 遍历倒序列表（从新到旧），精准计算“连续未接通”
        for call in self.call_records:
            if isinstance(call, dict):
                one_code = call.get("oneLevelCode")
                two_code = call.get("twoLevelCode")
                call_time = call.get("create_time") or call.get("created_at") or call.get("ended_at")
                answered = call.get("answered")
                call_answered = call.get("callAnswered")
                outcome = call.get("outcome")
            else:
                one_code = getattr(call, "oneLevelCode", None)
                two_code = getattr(call, "twoLevelCode", None)
                call_time = getattr(call, "create_time", None) or getattr(call, "created_at", None) or getattr(call, "ended_at", None)
                answered = getattr(call, "answered", None)
                call_answered = getattr(call, "callAnswered", None)
                outcome = getattr(call, "outcome", None)

            # 现代标准数据优先通过 answered / callAnswered / outcome 判定未接通
            if answered is not None:
                is_no_answer = (answered == 0)
            elif call_answered is not None:
                is_no_answer = (call_answered is False)
            elif outcome in {"no_answer", "busy", "rejected", "unanswered"}:
                is_no_answer = True
            else:
                # 兼容旧版本两级码判定 (500/508、500/510、500/501、500/509)
                two_code_str = str(two_code).strip() if two_code is not None else ""
                one_code_str = str(one_code).strip() if one_code is not None else ""
                is_no_answer = (one_code_str == "500" and two_code_str in {"501", "508", "510", "509"})

            if is_no_answer:
                continuous_count += 1
                streak_start_time = call_time
            else:
                break

        self.continuous_no_answer_count = continuous_count

        if streak_start_time:
            s_time_naive = streak_start_time.replace(tzinfo=None) if streak_start_time.tzinfo else streak_start_time
            base_time_naive = base_time.replace(tzinfo=None) if base_time.tzinfo else base_time

            delta = base_time_naive - s_time_naive
            if delta.total_seconds() > 0:
                self.continuous_no_answer_hours = int(delta.total_seconds() // 3600)
        else:
            self.continuous_no_answer_hours = 0
