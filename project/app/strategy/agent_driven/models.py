from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Literal
from pydantic import BaseModel, Field, field_validator, ConfigDict

from dataclasses import dataclass, field
from app.strategy.contracts.base import BaseStrategyConfig


# ==============================================================================
# 1. 核心标准 4 态枚举与动作定义
# ==============================================================================

class StandardBusinessStatus(str, Enum):
    """
    业务全局标准 4 态枚举 (MECE 完备状态空间):
    - SUCCESS: 目标达成 / 达标正向结案 (如客户同意加微/预约到店)
    - FAILURE: 物理触达失败 (未接通/占线/语音信箱/空号)
    - REJECTION: 明确拒绝 / 负向终止 (DNC 拒访黑名单，停止一切跟进)
    - PARTIAL: 部分达成 / 意向存续需再跟进 (预约推迟/比对考虑中/发资料看)
    """
    SUCCESS = "success"
    FAILURE = "failure"
    REJECTION = "rejection"
    PARTIAL = "partial"


class ScheduleActionType(str, Enum):
    """排期决策动作类型"""
    SCHEDULE_CALL = "schedule_call"           # 安排下一次电话追呼
    TERMINATE_SUCCESS = "terminate_success"   # 成功达成目标，正常结案
    TERMINATE_REJECT = "terminate_reject"     # 明确拒绝，终止跟进
    SWITCH_TO_MOBILE = "switch_to_mobile"     # 切换为工作手机呼叫
    SWITCH_TO_WECHAT = "switch_to_wechat"     # 转为微信优先攻坚
    TRIGGER_SMS = "trigger_sms"               # 协同触发补充短信


# ==============================================================================
# 2. 事实凭据模型
# ==============================================================================

class AppointmentEvidence(BaseModel):
    """从对话中抽取的硬性事实凭据 (防止时间推理幻觉)"""
    has_appointment: bool = False
    source_turn_id: Optional[int] = Field(None, description="对话轮次序号")
    verbatim_quote: Optional[str] = Field(None, description="客户口头约定时间的逐字原文引用")
    parsed_time: Optional[datetime] = Field(None, description="解析出的绝对时间")


# ==============================================================================
# 3. 垂直业务子场景中间思考结论契约 (Sub-Prompt Evaluator Outputs)
# ==============================================================================

class IntentAnalysisOutput(BaseModel):
    """意向与画像专家 Prompt 分析结论"""
    intent_level: Literal["high", "medium", "low", "none"] = Field("none", description="意向强弱等级")
    interested_car_models: List[str] = Field(default_factory=list, description="意向车型")
    budget_range: Optional[str] = Field(None, description="预算区间")
    trade_in: bool = Field(False, description="是否有置换意愿")
    purchase_timeframe: Optional[str] = Field(None, description="预计购车时间段")
    summary: str = Field("", description="意向摘要说明")


class ObjectionAnalysisOutput(BaseModel):
    """抗拒与异议专家 Prompt 分析结论"""
    has_objection: bool = Field(False, description="是否存在抗拒/异议")
    objection_category: Optional[Literal["price", "brand", "timing", "family", "competitor", "other"]] = Field(
        None, description="异议分类"
    )
    objection_detail: str = Field("", description="抗拒具体内容与原因")
    attitude_severity: Literal["mild", "moderate", "severe"] = Field("mild", description="抗拒剧烈程度")


class AppointmentAnalysisOutput(BaseModel):
    """时序与预约专家 Prompt 分析结论"""
    has_explicit_time: bool = Field(False, description="客户是否明确表达了预约时间")
    raw_quote: Optional[str] = Field(None, description="约定时间的原话")
    target_time_expression: Optional[str] = Field(None, description="时间表达词(如'明天下午两点')")
    normalized_time: Optional[datetime] = Field(None, description="标准化绝对时间")
    customer_available_period: Optional[str] = Field(None, description="闲暇时段(如'工作日晚上','周末上午')")


class SentimentAnalysisOutput(BaseModel):
    """情绪与风险合规 Prompt 分析结论"""
    sentiment: Literal["positive", "neutral", "negative", "hostile"] = Field("neutral", description="客户情绪倾向")
    churn_risk: bool = Field(False, description="是否有流失风险")
    dnc_requested: bool = Field(False, description="是否要求不要再打电话/投诉倾向 (Do-Not-Call)")
    notes: str = Field("", description="情绪与风险备注")


# ==============================================================================
# 4. 主控汇总 Prompt 决策权威模型 (Master Summary Decision)
# ==============================================================================

class MasterSummaryDecision(BaseModel):
    """
    主控汇总 Prompt 唯一产出的权威决策契约
    聚合多路子 Prompt 的中间事实，并对齐 4 态标准
    """
    status: StandardBusinessStatus = Field(..., description="业务标准 4 态之一")
    reason: str = Field(..., description="综合归因判定与排期依据")
    
    # 策略与动作
    action: ScheduleActionType = Field(..., description="排期动作")
    next_call_time: Optional[datetime] = Field(None, description="建议的下次外呼时间 (时区感知)")
    priority: int = Field(default=5, ge=1, le=9, description="呼叫队列优先级")
    use_work_phone: bool = Field(default=False, description="是否采用工作手机拨打")
    
    # 事实凭据与下轮协同
    appointment: Optional[AppointmentEvidence] = None
    voice_agent_directive: Optional[str] = Field(None, description="输出给下一轮 Voice Agent 的动态开场/攻坚建议")
    
    # 画像打标与可解释性
    business_tags: List[str] = Field(default_factory=list, description="画像与业务标签")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0, description="置信度")

    # 跨业务热切换与多业务定制评估沉淀
    handoff_workflow_id: Optional[str] = Field(None, description="建议切换的下游目标业务线 workflow_id")
    handoff_context_summary: Optional[str] = Field(None, description="跨业务交接给新业务的客户背景摘要")
    custom_evaluator_findings: Dict[str, Any] = Field(default_factory=dict, description="业务卡带专属评估器的提取结论")

    @field_validator("next_call_time")
    def validate_next_call_time_for_terminals(cls, v: Optional[datetime], values) -> Optional[datetime]:
        status = values.data.get("status")
        if status in (StandardBusinessStatus.SUCCESS, StandardBusinessStatus.REJECTION):
            # 终态结案不需要排期时间
            return None
        return v


# ==============================================================================
# 5. Agent 驱动策略配置模型 (AgentStrategyConfig)
# ==============================================================================

@dataclass
class AgentStrategyConfig(BaseStrategyConfig):
    """Agent 策略配置基类，包含工时合规、频次熔断、超时控制等参数"""
    work_start_time: str = "09:00:00"
    work_end_time: str = "21:00:00"
    min_cooldown_minutes: int = 30         # 两次外呼最小冷却间隔 (分钟)
    max_calls_per_day: int = 3             # 单日最大呼叫次数
    max_call_num: int = 10                 # 线索生命周期最大呼叫次数
    noanswer_num: int = 4                  # 连续未接通熔断次数
    call_max_time: int = 72                # 最大跟进周期 (小时)
    timeout_seconds: float = 5.0           # Agent 单次推理超时 (秒)，超时即降级
    shadow_mode: bool = False              # 是否开启影子比对模式
    strategy_prompt: Optional[str] = None  # 定制化策略人设与目标 Prompt
    domain_master_prompt: Optional[str] = None
    allowed_handoff_targets: List[str] = field(default_factory=list)
    custom_evaluators: List[Any] = field(default_factory=list)
    enabled_sub_prompts: List[str] = field(
        default_factory=lambda: ["intent", "objection", "appointment", "sentiment"]
    )
