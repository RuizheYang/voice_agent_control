from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Type

from app.strategy.agent_driven.engine import AgentDrivenStrategyEngine
from app.strategy.contracts.base import BaseStrategyEngine

logger = logging.getLogger(__name__)

# 策略解析器类型规范，用于依赖注入 (Dependency Injection)
StrategyEngineResolver = Callable[[Optional[str]], BaseStrategyEngine]

# ==============================================================================
# schedule_id 核心注册表：全面收敛为 Agent 驱动智能外呼排期引擎
# ==============================================================================
SCHEDULE_REGISTRY: Dict[str, Type[BaseStrategyEngine]] = {
    "agent_driven": AgentDrivenStrategyEngine,
}

SUPPORTED_SCHEDULE_IDS: List[str] = list(SCHEDULE_REGISTRY.keys())


class StrategyEngineFactory:
    """策略引擎统一创建工厂（收敛为 AgentDrivenStrategyEngine）"""

    _cache: Dict[Type[BaseStrategyEngine], BaseStrategyEngine] = {}

    @classmethod
    def normalize_schedule_id(cls, schedule_id: Optional[str]) -> str:
        """规范化排期策略 ID（默认 agent_driven；非支持标识抛出 ValueError）"""
        if not schedule_id:
            return "agent_driven"
        clean = schedule_id.strip()
        if clean not in SCHEDULE_REGISTRY:
            raise ValueError(
                f"[StrategyEngineFactory] 未知或不支持的排期策略 schedule_id: '{schedule_id}'，"
                f"当前仅支持标准策略: {SUPPORTED_SCHEDULE_IDS}"
            )
        return clean

    @classmethod
    def get_engine(cls, schedule_id: Optional[str] = None) -> BaseStrategyEngine:
        canonical_id = cls.normalize_schedule_id(schedule_id)
        engine_cls = SCHEDULE_REGISTRY[canonical_id]
        if engine_cls not in cls._cache:
            cls._cache[engine_cls] = engine_cls()
        return cls._cache[engine_cls]

    @classmethod
    def create(cls, strategy_name: str = "agent_driven") -> BaseStrategyEngine:
        return cls.get_engine(strategy_name)

    @classmethod
    def is_valid_schedule_id(cls, schedule_id: Optional[str]) -> bool:
        if not schedule_id:
            return False
        return schedule_id.strip() in SCHEDULE_REGISTRY
