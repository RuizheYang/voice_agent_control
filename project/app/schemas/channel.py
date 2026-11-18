from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict

class WechatNoticeData(BaseModel):
    type: Optional[str] = Field("notice_addF", description="事件类型")
    resultCode: str = Field(..., description="微信添加进度: 1 - 未找到用户, 3 - 添加成功, 4 - 添加失败, 434 - 无可用资源")
    resultMessage: Optional[str] = Field(None, description="失败时此处为失败原因")
    pipeNickName: Optional[str] = Field(None, description="分配的通道昵称（添加成功时才有值）")
    custId: str = Field(..., description="客户业务id")
    customerAdditionType: Optional[str] = Field(None, description="客户添加通过方式")
    model_config = ConfigDict(extra="allow")

class WechatNoticeRequest(BaseModel):
    msgType: Optional[str] = Field("notice", description="消息类型")
    data: WechatNoticeData = Field(..., description="数据")
    model_config = ConfigDict(extra="allow")
    
    @property
    def success(self) -> bool:
        return self.data.resultCode == "3"
    
    @property
    def failed(self) -> bool:
        return self.data.resultCode in {"1", "4", "434"}

class WechatNoticeResponse(BaseModel):
    code: int = Field(0, description="响应码，0表示成功，非0表示失败")
    message: str = Field("success", description="响应消息")