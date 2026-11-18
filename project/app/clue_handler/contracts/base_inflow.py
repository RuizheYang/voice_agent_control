from __future__ import annotations
from abc import ABC, abstractmethod
from app.schemas.clue_create import NormalizedClue


class BaseInflowHandler(ABC):
    """线索流入生命周期阶段处理器抽象契约"""

    @abstractmethod
    async def handle(self, clue: NormalizedClue) -> bool:
        """处理新线索流入 (校验、首呼决策与状态初始化)"""
        pass


# 历史别名兼容
BaseNewClueHandler = BaseInflowHandler
