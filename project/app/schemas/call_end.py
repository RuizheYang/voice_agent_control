from __future__ import annotations
from typing import Any, Optional

from pydantic import BaseModel, Field, ConfigDict, AliasChoices
import json
from .examples import CALL_END_REQUEST_EXAMPLE


##### 通话结束回调接口相关的请求和响应模型定义 #####
class CallEndCheckDetails(BaseModel):
    name_check: Optional[Any] = Field(None, description="是否完成姓名核验")
    need_financial_service: Optional[Any]= Field(None, description="是否有资金服务需求")
    is_car_owner: Optional[Any] = Field(None, description="是否为车主")
    under_repayment: Optional[Any] = Field(None, description="是否处于还款中")
    has_driving_license: Optional[Any] = Field(None, description="是否持有行驶证")

    model_config = ConfigDict(extra="allow")  # 允许额外字段，避免因字段缺失导致解析失败

class CallEndOutboundReport(BaseModel):
    userQualified: Optional[Any] = Field(None, description="用户是否符合条件")
    conversationSummary: Optional[Any] = Field(None, description="通话摘要")
    checkDetails: Optional[CallEndCheckDetails | None] = Field(None, description="关键信息核验结果")
    isPhoneNumber: Optional[Any] = Field(None, description="当前号码是否可用")
    wechatAccount: Optional[Any] = Field(None, description="微信账号")
    isUserOperable: Optional[Any] = Field(None, description="用户是否可继续操作")
    nextCallTime: Optional[Any] = Field(None, description="下次呼叫时间")
    isAngry: Optional[Any] = Field(None, description="用户是否表现出愤怒情绪")
    wechatRequested: Optional[Any] = Field(None, description="是否请求添加微信")
    wechatAccepted: Optional[Any] = Field(None, description="是否接受添加微信")
    memory: Optional[Any] = Field(None, description="通话记忆内容，JSON 字符串")
    oneLevelCode: Optional[Any] = Field(None, description="一级结果码")
    twoLevelCode: Optional[Any] = Field(None, description="二级结果码")
    callAnswered: Optional[Any] = Field(None, description="是否接通")
    recordFile: Optional[Any] = Field(None, description="录音文件地址")
    model_config = ConfigDict(extra="allow")  # 允许额外字段，避免因字段缺失导致解析失败


class CallEndCurrentCall(BaseModel):
    id: Optional[int] = Field(None, description="当前通话记录 ID")
    transCode: Optional[str] = Field(None, description="交易码")
    transMsg: Optional[str] = Field(None, description="交易描述")
    calledPhone: Optional[str] = Field(None, description="被叫号码")
    businessId: Optional[str] = Field(None, description="业务 ID")
    batchId: Optional[str] = Field(None, description="批次 ID")
    ringingStartTime: Optional[int] = Field(None, description="振铃开始时间戳")
    ringingStartTimeDisplay: Optional[str] = Field(None, description="振铃开始时间")
    talkStartTime: Optional[int] = Field(None, description="接通开始时间戳")
    talkStartTimeDisplay: Optional[str] = Field(None, description="接通开始时间")
    aiCallCompleteTime: Optional[int] = Field(None, description="AI 外呼完成时间戳")
    recordFile: Optional[str] = Field(None, description="录音文件地址")
    resultCode: Optional[str] = Field(None, description="结果码")
    resultMsg: Optional[str] = Field(None, description="结果描述")
    callId: Optional[str] = Field(None, description="通话 ID")
    aiCallSeconds: Optional[int] = Field(None, description="AI 通话时长，秒")
    aiCallContextList: Optional[str] = Field(None, description="通话上下文，JSON 字符串")
    intentionResult: Optional[int] = Field(None, description="意向结果")
    extraInfo: Optional[str] = Field(None, description="扩展信息，JSON 字符串")
    ringingSeconds: Optional[int] = Field(None, description="振铃时长，秒")
    ringEnterTime: Optional[int] = Field(None, description="进入振铃队列时间戳")
    provinceCode: Optional[str] = Field(None, description="省份编码")
    provinceName: Optional[str] = Field(None, description="省份名称")
    cityCode: Optional[str] = Field(None, description="城市编码")
    cityName: Optional[str] = Field(None, description="城市名称")
    displayNumber: Optional[str] = Field(None, description="外显号码")
    hangupPart: Optional[int] = Field(None, description="挂断方")
    aiRealtimeDuplexInteractive: Optional[bool] = Field(None, description="是否开启实时双工")
    callSheetId: Optional[str] = Field(None, description="通话工单 ID")
    gateway: Optional[str] = Field(None, description="网关编码")
    gatewayName: Optional[str] = Field(None, description="网关名称")
    intentionResultName: Optional[str] = Field(None, description="意向结果名称")
    createTime: Optional[str] = Field(None, description="创建时间")
    updateTime: Optional[str] = Field(None, description="更新时间")

class CallEndInfoExtra(BaseModel):
    wechat_requested: Optional[bool] = Field(None, description="是否发送加微")
    wechat_accepted: Optional[bool] = Field(None, description="用户是否同意")
    model_config = ConfigDict(extra="allow")

class CallEndMetasExtra(BaseModel):
    wechat_requested_code: Optional[int] = Field(None, description="发送加微请求码")
    wechat_accepted_code: Optional[int] = Field(None, description="加微结果码")
    model_config = ConfigDict(extra="allow")

class CallEndExtra(BaseModel):
    end_call_info: Optional[CallEndInfoExtra] = Field(None)
    metas: Optional[CallEndMetasExtra] = Field(None)
    model_config = ConfigDict(extra="allow")

class CallEndData(BaseModel):
    clueId: str = Field(..., description="线索 ID")
    currentCall: CallEndCurrentCall = Field(..., description="当前通话信息")
    historyCalls: list[Any] = Field(default_factory=list, validation_alias=AliasChoices("historyCalls", "historicalCalls"), description="历史通话记录列表")
    extra: Optional[CallEndExtra] = Field(None, description="业务扩展信息")
    
    # 兼容老逻辑：在 Summary 生成后内部赋值挂载
    outboundReport: Optional[CallEndOutboundReport] = Field(None, description="外呼通话报告")

class CallEndRequest(BaseModel):
    callId: Optional[str] = Field(None, max_length=64, description="独立调度生成的内部callId，区别于currentCall.callId")
    
    msgType: Optional[str] = Field("pre_agent", description="消息类型")
    data: Optional[CallEndData] = Field(None, description="回调消息主体")
    requestId: Optional[str] = Field(None, description="请求 ID，唯一标识一次回调请求")
    
    def to_report(self):
        report = self.data.outboundReport
        return {
            "clueId": self.data.clueId,
            "one_level_code": self.one_level_code,
            "two_level_code": self.two_level_code,
            "memory": report.memory if report else None,
            "call_answered": report.callAnswered if report else None,
            "call_finished": True,
            "next_call_time": report.nextCallTime if report else None,
            "wechat_requested": report.wechatRequested if report else None,
            "wechat_accepted": report.wechatAccepted if report else None,
            "record_file": report.recordFile if report else None,
            "historical_calls": self.data.historyCalls,
            "remark": report.conversationSummary if report else None,
            "currentCall": self.data.currentCall.model_dump()
        }
    
    def to_record(self):
        report = self.data.outboundReport
        data = {
            "clueId": self.data.clueId,
            "oneLevelCode": self.one_level_code,
            "twoLevelCode": self.two_level_code,
            "userQualified": report.userQualified if report else None,
            "isPhoneNumber": report.isPhoneNumber if report else None,
            "wechatAccount": report.wechatAccount if report else None,
            "isUserOperable": report.isUserOperable if report else None,
            "nextCallTime": report.nextCallTime if report else None,
            "isAngry": report.isAngry if report else None,
            "wechatRequested": report.wechatRequested if report else None,
            "wechatAccepted": report.wechatAccepted if report else None,
            "callAnswered": report.callAnswered if report else None,
            "recordFile": report.recordFile if report else None
        }
        return data
    
    @property
    def one_level_code(self):
        if not self.data.outboundReport or self.data.outboundReport.oneLevelCode is None:
            return 300
        one_level_code = self.data.outboundReport.oneLevelCode
        return int(one_level_code) if str(one_level_code).isdigit() else 300
    
    @property
    def two_level_code(self):
        if not self.data.outboundReport or self.data.outboundReport.twoLevelCode is None:
            return 323
        two_level_code = self.data.outboundReport.twoLevelCode
        return int(two_level_code) if str(two_level_code).isdigit() else 323
    
    # provide example data for api testing
    # model_config = ConfigDict(
    #     json_schema_extra= {
    #         "examples":[CALL_END_REQUEST_EXAMPLE]
    #     }
    # )
    
class CallEndResponse(BaseModel):
    code: int = Field(default = 0, description="响应码值，0 表示通知成功，非 0 表示通知失败")
    msg: str = Field(default = "success", description="响应信息")
    
