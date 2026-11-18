from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional, Union
from app.schemas import CallEndRequest


class BasePostCallHandler(ABC):
    """外呼挂机后生命周期阶段处理器抽象契约"""

    @abstractmethod
    async def handle(
        self,
        end_call_info: Union[CallEndRequest, str],
        request_id: Optional[str] = None,
    ) -> bool:
        """处理通话挂机后全流程 (落库、AI总结分析、意图检测、排期推演与终态流转)"""
        pass
