"""到期派发的内部请求；执行令牌只在进程内传递，不参与日志序列化。"""
from pydantic import ConfigDict, Field

from app.schemas.clue_create import ScheduledTaskRequest


class ScheduledCallRequest(ScheduledTaskRequest):
    taskId: int = Field(..., strict=True, gt=0)
    executionToken: str = Field(..., min_length=1, exclude=True, repr=False)

    model_config = ConfigDict(extra="forbid")
