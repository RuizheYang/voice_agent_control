from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class CancelClueResult:
    """线索取消跟进结果对象"""
    success: bool
    reason: str = ""


class BaseCancelHandler(ABC):
    """线索取消生命周期阶段处理器抽象契约"""

    @abstractmethod
    async def handle(
        self,
        clue_id: str,
        closeReason: Optional[str] = None,
    ) -> CancelClueResult:
        """取消线索跟进并撤销在途定时任务"""
        pass


# 历史别名兼容
BaseCancelCallHandler = BaseCancelHandler
