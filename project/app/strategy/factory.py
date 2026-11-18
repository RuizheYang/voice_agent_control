from __future__ import annotations

import logging
from typing import Dict, Type, Optional, List, Callable
from app.strategy.contracts.base import BaseStrategyEngine
from app.strategy.single_call.engine import SingleCallStrategyEngine
from app.strategy.sevenday.engine import SevenDayStrategyEngine
from app.strategy.agent_driven.engine import AgentDrivenStrategyEngine

logger = logging.getLogger(__name__)

# 策略解析器类型规范，用于依赖注入 (Dependency Injection)
StrategyEngineResolver = Callable[[Optional[str]], BaseStrategyEngine]

# ==============================================================================
# schedule_id 核心注册表
# 规范要求：策略名称统一采用包含下划线、清晰明了的标准命名 (snake_case)
# 系统无历史兼容别名逻辑，外部调用必须严格使用并遵守标准名称：
# 1. single_call (单通即结)
# 2. seven_day_three_call (0-1-2-7 每日三呼通用追呼)
# 3. agent_driven (完全由 Agent 驱动的智能多场景排期策略)
# ==============================================================================
SCHEDULE_REGISTRY: Dict[str, Type[BaseStrategyEngine]] = {
    # 1. 单通即结束策略
    "single_call": SingleCallStrategyEngine,

    # 2. 七日 0-1-2-7 每日三呼策略 (12呼 / 5次连续未接通熔断 / 168h生命周期)
    "seven_day_three_call": SevenDayStrategyEngine,

    # 3. 完全由 Agent 驱动的智能多场景排期策略
    "agent_driven": AgentDrivenStrategyEngine,
}

# 官方支持的标准策略名称列表 (用于 API 文档展示与参数校验)
SUPPORTED_SCHEDULE_IDS: List[str] = list(SCHEDULE_REGISTRY.keys())


class StrategyEngineFactory:
    """策略引擎统一创建工厂 (基于标准 schedule_id 唯一路由与单例缓存)"""

    _cache: Dict[Type[BaseStrategyEngine], BaseStrategyEngine] = {}

    @classmethod
    def normalize_schedule_id(cls, schedule_id: Optional[str]) -> str:
        """规范化排期策略 ID (统一去除空白，默认 seven_day_three_call；若非标准名称抛出 ValueError)"""
        if not schedule_id:
            return "seven_day_three_call"
        clean = schedule_id.strip()
        if clean not in SCHEDULE_REGISTRY:
            raise ValueError(
                f"[StrategyEngineFactory] 未知或不支持的排期策略 schedule_id: '{schedule_id}'，"
                f"外部调用必须严格使用系统标准名称: {SUPPORTED_SCHEDULE_IDS}"
            )
        return clean

    @classmethod
    def get_engine(cls, schedule_id: Optional[str] = None) -> BaseStrategyEngine:
        """
        根据 schedule_id 获取唯一对应的排期策略引擎实例。
        外部调用必须遵守标准名称规范 ('single_call', 'seven_day_three_call')。
        若未提供 (None)，默认采用 'seven_day_three_call'；
        若传入非标准或未知策略名称，直接抛出 ValueError 拒绝。
        """
        canonical_id = cls.normalize_schedule_id(schedule_id)
        engine_cls = SCHEDULE_REGISTRY[canonical_id]

        if engine_cls not in cls._cache:
            cls._cache[engine_cls] = engine_cls()

        return cls._cache[engine_cls]

    @classmethod
    def create(cls, strategy_name: str) -> BaseStrategyEngine:
        """根据策略名称创建/获取引擎"""
        return cls.get_engine(strategy_name)

    @classmethod
    def is_valid_schedule_id(cls, schedule_id: Optional[str]) -> bool:
        """校验 schedule_id 是否合法 (必须为官方标准策略名称)"""
        if not schedule_id:
            return False
        return schedule_id.strip() in SCHEDULE_REGISTRY
