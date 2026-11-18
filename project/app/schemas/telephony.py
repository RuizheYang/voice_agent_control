"""话务平台原生送号结构；不带业务默认线路、话术或测试手机号。"""
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field


class Agent(BaseModel):
    agentId: str = Field(..., min_length=1, description="人工坐席号，不是Voice Agent ID")


class PhoneNumber(BaseModel):
    calledPhone: str = Field(..., min_length=1)
    businessId: str = Field(..., min_length=1)
    callParams: str = Field(..., description="按平台要求序列化一次的运行参数JSON")
    agentList: list[Agent] | None = None
    transferParams: str | None = None


class TelephonyAPI(BaseModel):
    accessorId: str = Field(..., min_length=1, max_length=32)
    tradeNo: str = Field(..., min_length=1, max_length=32)
    timestamp: str = Field(..., pattern=r"^\d{17}$")
    strategyId: str = Field(..., min_length=1, description="话务策略ID，区别于跟进策略ID")
    speechcraftId: str = Field(..., min_length=1)
    batchId: str = Field(..., min_length=1, max_length=32)
    execStartTime: str
    execEndTime: str | None = None
    execLimit: int = Field(..., strict=True, ge=1)
    phoneList: list[PhoneNumber] = Field(..., min_length=1, max_length=1, description="独立调度按轮次单条送号")

    model_config = ConfigDict(extra="forbid")


@dataclass(frozen=True)
class SubmitResult:
    status: str  # accepted / retry / deferred / failed / unknown
    code: str = ""
    provider_call_id: str | None = None
