from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo
from app.utils.time_utils import get_now

def now_datetime():
    datenow = get_now()
    return datenow.strftime("%Y-%m-%d %H:%M:%S")

class VoiceAgentRequest(BaseModel):
    clueId: str = Field(..., description="线索ID")
    clueSource: str = Field(..., description="线索来源")
    orderTime: str = Field(default_factory=now_datetime, description="订单时间")
    customerSex: Optional[str] = Field("男", description="客户性别")
    customerName: str = Field(..., description="客户姓名")
    customerPhoneNum: str = Field(..., description="客户电话号码")
    slotCallPolicy: str = Field('loan_first_call_agent_v4', description="话术策略，默认为loan_first_call_agent_v4")
    slotCallTts: str = Field('voice_v0', description="话术TTS，默认为voice_v0")
    slotQualifiedCallPolicy: str = Field('default', description="前置预审话术策略")
    slotRejectReferralCallPolicy: str = Field('default', description="拒量话术策略")
    preCheckMode: str = Field("0", description="预审标签: 0 非前置, 1 前置")
    preCheckResult: Optional[str] = Field(None, description="前置预审结果")
    
    
class VoiceAgentResponse(BaseModel):
    success: bool = Field(0, description="是否成功")
    message: Optional[str] = Field("success", description="返回消息")
    
    