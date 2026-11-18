import uuid
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator

# 7大标准可订阅事件定义
SUBSCRIBABLE_EVENTS = [
    "call_schedule",   # 外呼排期
    "call_start",      # 通话开始
    "call_end",        # 通话结束
    "action_wechat",   # 微信动作
    "action_sms",      # 短信动作
    "action_email",    # 邮件动作
    "clue_end",        # 线索结束
]

EVENT_ALIAS_MAP = {
    # 标准定义
    "call_schedule": "call_schedule",
    "call_start": "call_start",
    "call_end": "call_end",
    "action_wechat": "action_wechat",
    "action_sms": "action_sms",
    "action_email": "action_email",
    "clue_end": "clue_end",

    # 别名与历史兼容映射
    "call-schedule": "call_schedule",
    "call_scheduled": "call_schedule",
    "call-scheduled": "call_schedule",
    "call-start": "call_start",
    "call_started": "call_start",
    "call-started": "call_start",
    "call-end": "call_end",
    "call_ended": "call_end",
    "call-ended": "call_end",
    "clue-end": "clue_end",
    "clue_ended": "clue_end",
    "clue-ended": "clue_end",
    "wechat": "action_wechat",
    "wechat_requested": "action_wechat",
    "wechat_accepted": "action_wechat",
    "action-wechat": "action_wechat",
    "sms": "action_sms",
    "sms_sent": "action_sms",
    "action-sms": "action_sms",
    "email": "action_email",
    "action-email": "action_email",
}

class FollowupConfig(BaseModel):
    summary_type: Optional[str] = Field(None, description="总结版本/套件类型标识，与策略 schedule_id 配套 (如 single_call_summary)")
    summary_prompt: Optional[str] = Field(None, description="定制化的场景业务 prompt；为空只做固定通用总结，非空额外输出定制业务总结")
    max_followup_hours: int = Field(default=72, gt=0, description="最长追呼时间(小时)")
    max_calls: int = Field(default=10, gt=0, description="单个任务呼叫总次数上限，包含首次呼叫")
    max_call_per_day: int = Field(default=3, gt=0, description="单个任务每天的呼叫次数上限")
    max_no_answer_calls: int = Field(default=3, gt=0, description="单个任务累计无人接听次数上限")
    model_config = ConfigDict(extra="allow")

    @field_validator("summary_type", "summary_prompt", mode="before")
    def validate_summary_strings(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        if not isinstance(v, str):
            return str(v).strip() or None
        return v.strip() or None

class SipConfig(BaseModel):
    accessor_id: Optional[str] = Field(None, description="话务接入标识")
    line_id: Optional[str] = Field(None, description="话务线路标识")
    mobile_line_id: Optional[str] = Field(None, description="移动线路标识(若支持手机拨打则必填)")
    model_config = ConfigDict(extra="allow")

class WebhookSubscription(BaseModel):
    url: str = Field(..., description="事件回推接口的 URL")
    events: List[str] = Field(
        default_factory=lambda: list(SUBSCRIBABLE_EVENTS),
        description="订阅的事件列表 (call_schedule, call_start, call_end, action_wechat, action_sms, action_email, clue_end)"
    )
    model_config = ConfigDict(extra="allow")

    @field_validator("url")
    def validate_url(cls, v: str) -> str:
        v_clean = v.strip()
        if not (v_clean.startswith("http://") or v_clean.startswith("https://")):
            raise ValueError(f"Webhook URL 必须以 http:// 或 https:// 开头: {v}")
        return v_clean

    @field_validator("events")
    def normalize_events(cls, events: List[str]) -> List[str]:
        normalized = []
        for e in events:
            clean = e.strip().lower()
            normalized.append(EVENT_ALIAS_MAP.get(clean, clean))
        return normalized

class StrategyCreateRequest(BaseModel):
    strategy_id: Optional[str] = Field(None, description="外呼策略总体的唯一ID，若传入则覆盖/更新，不传则自动生成")
    schedule_id: Optional[str] = Field("seven_day_three_call", description="内置追呼排期方案 ID，决定下一次呼叫的时间 (single_call, seven_day_three_call, agent_driven)")
    name: Optional[str] = Field(None, description="策略名称")
    followup: FollowupConfig = Field(..., description="通话总结方案和追呼限制配置")
    sip: Optional[SipConfig] = Field(None, description="话务接入和线路配置")
    webhook: Optional[WebhookSubscription] = Field(None, description="外呼事件回推接口")
    model_config = ConfigDict(extra="allow")

    @field_validator("schedule_id")
    def validate_schedule_id(cls, v: Optional[str]) -> str:
        if v is None:
            return "seven_day_three_call"
        clean = v.strip().lower()
        from app.strategy.factory import StrategyEngineFactory, SUPPORTED_SCHEDULE_IDS
        if not StrategyEngineFactory.is_valid_schedule_id(clean):
            raise ValueError(f"未知的排期策略 schedule_id: '{v}'，外部调用必须严格使用系统标准名称: {SUPPORTED_SCHEDULE_IDS}")
        return clean

    @model_validator(mode="after")
    def ensure_strategy_id(self) -> "StrategyCreateRequest":
        if not self.strategy_id:
            self.strategy_id = f"strategy_{uuid.uuid4().hex[:12]}"
        return self

class StrategyConfigResponse(BaseModel):
    code: int = 0
    msg: str = "success"
    data: Optional[Dict[str, Any]] = None
