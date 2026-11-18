from typing import Optional, Any, Dict, List, Literal
from pydantic import BaseModel, Field, ConfigDict, model_validator

from app.schemas.voice_agent_config import VoiceAgentCreateRequest
from app.schemas.strategy_config import StrategyCreateRequest

class WorkflowCreateRequest(BaseModel):
    workflow_id: str = Field(..., description="业务线/流程唯一标识，如 car_loan")
    agent_id: Optional[str] = Field(None, description="关联创建好的 Voice Agent ID")
    strategy_id: Optional[str] = Field(None, description="关联创建好的外呼跟进策略 ID")
    name: Optional[str] = Field(None, description="业务线显示名称")
    version: str = Field("v1.0", description="版本号")
    description: Optional[str] = Field(None, description="业务线流程描述")
    is_active: bool = Field(True, description="是否启用该流程")
    voice_agent: Optional[VoiceAgentCreateRequest] = Field(None, description="Voice Agent 完整创建配置（用于一站式聚合创建）")
    strategy: Optional[StrategyCreateRequest] = Field(None, description="外呼策略完整创建配置（用于一站式聚合创建）")
    domain_config: Optional[Dict[str, Any]] = Field(None, description="Multi-Agent 业务领域卡带配置 (评估器/工具白名单/Handoff规则)")
    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def validate_agent_and_strategy(self) -> "WorkflowCreateRequest":
        # 1. 验证 agent 来源：必须有 voice_agent 配置对象 或 agent_id
        if not self.voice_agent and not self.agent_id:
            raise ValueError("此workflow_id业务线没有对应的agent可以配套")
        if self.voice_agent and not self.agent_id:
            self.agent_id = self.voice_agent.agent_id

        # 2. 验证 strategy 来源：必须有 strategy 配置对象 或 strategy_id
        if not self.strategy and not self.strategy_id:
            raise ValueError("此workflow_id业务线没有对应的agent可以配套")
        if self.strategy and not self.strategy_id:
            self.strategy_id = self.strategy.strategy_id

        return self


class WorkflowBundleCreateRequest(WorkflowCreateRequest):
    """
    一站式业务线聚合创建请求模型：
    支持在单个接口中同时传入 workflow_id 基础信息、Voice Agent 完整配置与外呼跟进策略配置。
    """
    pass


class WorkflowConfigResponse(BaseModel):
    code: int = 0
    msg: str = "success"
    data: Optional[Dict[str, Any]] = None


class WorkflowDryRunRequest(BaseModel):
    """业务线卡带上线前模拟预检请求"""
    conversation: List[Dict[str, Any]] = Field(default_factory=list, description="模拟对话轮次列表")
    total_call_count: int = Field(default=1, ge=0)
    continuous_no_answer_count: int = Field(default=0, ge=0)
    handoff_count: int = Field(default=0, ge=0)


class ControlPauseRequest(BaseModel):
    scope: Literal["inflow", "schedule", "all"] = Field("all", description="暂停范围: inflow(新线索流入), schedule(次呼定时排期), all(全部)")
    reason: Optional[str] = Field(None, max_length=512, description="暂停原因")
    model_config = ConfigDict(extra="allow")


class ControlResumeRequest(BaseModel):
    scope: Literal["inflow", "schedule", "all"] = Field("all", description="恢复范围: inflow, schedule, all")
    model_config = ConfigDict(extra="allow")


class ControlStatusResponse(BaseModel):
    code: int = 0
    msg: str = "success"
    data: Optional[Dict[str, Any]] = None
