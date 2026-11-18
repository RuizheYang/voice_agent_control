from typing import Any, Optional, Dict
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict, model_validator
from app.utils.time_utils import get_now

class AgentData(BaseModel):
    variables: Dict[str, Any] = Field(default_factory=dict, description="动态变量")
    extras: Dict[str, Any] = Field(default_factory=dict, description="补充信息")
    model_config = ConfigDict(extra="allow")

class ClueOverrides(BaseModel):
    tts_id: Optional[str] = Field(None, description="TTS音色ID")
    voice_agent_id: Optional[str] = Field(None, description="Voice Agent ID")
    followup_strategy_id: Optional[str] = Field(None, description="跟进策略ID")
    callback_url: Optional[str] = Field(None, description="事件回推URL")
    model_config = ConfigDict(extra="allow")

class NormalizedClue(BaseModel):
    """系统内部统一的线索数据结构"""
    clueId: str = Field(..., description="线索ID")
    phoneNumber: str = Field(..., description="用户手机号")
    workflowId: Optional[str] = Field("default", description="业务线流程定义ID")
    agentId: Optional[str] = Field(None, description="绑定的 Voice Agent ID")
    strategyId: Optional[str] = Field(None, description="绑定的外呼策略 ID")
    externalTaskId: Optional[str] = Field(None, description="外部业务任务唯一ID")
    requestId: Optional[str] = Field(None, description="请求ID")
    customerName: Optional[str] = Field(None, description="客户姓名")
    customerSex: Optional[str] = Field(None, description="客户性别")
    customerCity: Optional[str] = Field(None, description="客户所在城市")
    orderTime: Optional[str] = Field(None, description="订单时间")
    clueSource: Optional[str] = Field(None, description="线索来源")
    accessReceiveTime: str = Field(default_factory=lambda: get_now().strftime("%Y-%m-%d %H:%M:%S"))
    agentData: Dict[str, Any] = Field(default_factory=dict)
    overrides: Dict[str, Any] = Field(default_factory=dict)
    extra: Dict[str, Any] = Field(default_factory=dict)
    
    # 兼容性字段访问
    @property
    def customerPhoneNum(self) -> str:
        return self.phoneNumber

class ClueCreateRequest(BaseModel):
    """
    线索创建请求体：
    同时兼容新规范 (clue_id, workflow_id, phone_number, agent_data, overrides)
    及兼容旧字段 (clueId, customerPhoneNum, customerName 等)
    """
    clue_id: Optional[str] = Field(None, description="线索全局唯一 ID")
    clueId: Optional[str] = Field(None, description="兼容旧线索ID")
    request_id: Optional[str] = Field(None, description="请求唯一 ID")
    requestId: Optional[str] = Field(None, description="兼容旧请求ID")
    workflow_id: Optional[str] = Field(None, description="业务线/流程定义")
    workflowId: Optional[str] = Field(None, description="兼容旧workflowId")
    external_task_id: Optional[str] = Field(None, description="外部业务任务ID")
    phone_number: Optional[str] = Field(None, description="待拨打手机号")
    customerPhoneNum: Optional[str] = Field(None, description="兼容旧客户手机号")
    
    customerName: Optional[str] = Field(None, description="客户姓名")
    customerSex: Optional[str] = Field(None, description="客户性别")
    customerCity: Optional[str] = Field(None, description="客户所在城市")
    orderTime: Optional[str] = Field(None, description="留资时间")
    clueSource: Optional[str] = Field(None, description="线索来源")
    callBackUrl: Optional[str] = Field(None, description="回调地址")
    accessReceiveTime: Optional[str] = Field(None, description="接入时间")
    
    agent_data: Optional[Dict[str, Any]] = Field(default_factory=dict, description="业务数据体")
    overrides: Optional[Dict[str, Any]] = Field(default_factory=dict, description="配置覆盖")

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def populate_unified_fields(self) -> "ClueCreateRequest":
        # 统一 clue_id
        effective_id = self.clue_id or self.clueId
        if not effective_id:
            raise ValueError("clue_id 或 clueId 不能为空")
        self.clue_id = effective_id
        self.clueId = effective_id

        # 统一 phone_number
        effective_phone = self.phone_number or self.customerPhoneNum
        if not effective_phone:
            raise ValueError("phone_number 或 customerPhoneNum 不能为空")
        self.phone_number = effective_phone
        self.customerPhoneNum = effective_phone

        # 统一 workflow_id
        self.workflow_id = self.workflow_id or self.workflowId or "default"
        self.workflowId = self.workflow_id

        # 统一 request_id
        self.request_id = self.request_id or self.requestId
        self.requestId = self.request_id

        return self

    def to_normalized(self, agent_id: Optional[str] = None, strategy_id: Optional[str] = None) -> NormalizedClue:
        extra_vars = {}
        if self.agent_data and isinstance(self.agent_data, dict):
            extra_vars = self.agent_data.get("variables", {})

        cust_name = self.customerName or extra_vars.get("name")
        cust_sex = self.customerSex or extra_vars.get("sex")
        cust_city = self.customerCity or extra_vars.get("city")

        receive_time = self.accessReceiveTime or get_now().strftime("%Y-%m-%d %H:%M:%S")

        return NormalizedClue(
            clueId=self.clue_id,
            phoneNumber=self.phone_number,
            workflowId=self.workflow_id,
            agentId=agent_id,
            strategyId=strategy_id,
            externalTaskId=self.external_task_id,
            requestId=self.request_id,
            customerName=cust_name,
            customerSex=cust_sex,
            customerCity=cust_city,
            orderTime=self.orderTime,
            clueSource=self.clueSource,
            accessReceiveTime=receive_time,
            agentData=self.agent_data or {},
            overrides={},
            extra={
                "orderTime": self.orderTime,
                "clueSource": self.clueSource,
                "callBackUrl": self.callBackUrl,
            }
        )

class ClueCreateResponse(BaseModel):
    code: int = Field(0, description="响应码值: 0 代表成功")
    msg: str = Field("success", description="成功/失败描述")
    data: Optional[Dict[str, Any]] = Field(None, description="业务数据")

##### 次呼与动作相关的请求和响应模型定义 #####
class ScheduledTaskRequest(BaseModel):
    clueId: str = Field(..., description="线索ID")
    model_config = ConfigDict(extra="allow")

class ScheduledTaskResponse(BaseModel):
    code: int = Field(0, description="响应码")
    msg: str = Field("success", description="响应描述")

class CancelClueRequest(BaseModel):
    clueId: str = Field(..., description="线索ID")
    closeReason: Optional[str] = Field(default="", description="关闭原因")

class StartCallRequest(BaseModel):
    clueId: str = Field(..., description="线索ID")
    callId: Optional[str] = Field(None, max_length=128, description="内部callId或送号响应已关联的平台通话编号")
    providerCallId: Optional[str] = Field(None, max_length=128, description="已关联的平台通话编号；可代替callId")
    batchId: Optional[str] = Field(None, max_length=32, description="本轮送号批次，可与clueId一起关联呼叫")

class StartCallResponse(BaseModel):
    code: int = Field(0, description="响应码")
    msg: str = Field("success", description="响应信息")

class BatchClueRecord(BaseModel):
    clue_id: str = Field(..., description="线索全局唯一 ID")
    external_task_id: Optional[str] = Field(None, description="外部业务任务唯一 ID")
    agent_data: Dict[str, Any] = Field(default_factory=dict, description="业务数据体")
    phone_number: str = Field(..., description="待拨打的用户手机号")
    model_config = ConfigDict(extra="allow")

class BatchClueCreateRequest(BaseModel):
    request_id: str = Field(..., description="本次请求唯一 ID")
    workflow_id: str = Field(..., description="业务线 / 流程定义")
    records: list[BatchClueRecord] = Field(..., description="线索数组")
    model_config = ConfigDict(extra="allow")

