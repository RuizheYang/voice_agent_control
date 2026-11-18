from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Union
from app.call_schedule.contracts import ScheduledCallRequest
from app.schemas.clue_create import ScheduledTaskRequest


class BaseScheduleHandler(ABC):
    """排期到期触发外呼阶段处理器抽象契约"""

    @abstractmethod
    async def handle(
        self,
        scheduled_task_info: Union[ScheduledCallRequest, ScheduledTaskRequest, str],
    ) -> bool:
        """处理排期任务到期触发 (状态流转、线路选择、送号外呼)"""
        pass
