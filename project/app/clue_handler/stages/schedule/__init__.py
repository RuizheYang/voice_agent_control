from typing import Dict, Type
from app.clue_handler.contracts.base_schedule import BaseScheduleHandler
from .default import DefaultScheduleHandler, ScheduleHandler

# 排期执行阶段处理器注册表
SCHEDULE_REGISTRY: Dict[str, Type[BaseScheduleHandler]] = {
    "default": DefaultScheduleHandler,
}


def get_schedule_handler_cls(name: str = "default") -> Type[BaseScheduleHandler]:
    """根据注册名称获取对应的 ScheduleHandler 类"""
    clean_name = (name.strip().lower() if name else "default")
    if clean_name not in SCHEDULE_REGISTRY:
        raise ValueError(
            f"[ScheduleStage] 未知的 ScheduleHandler 标识: '{name}'，"
            f"可选方案包括: {list(SCHEDULE_REGISTRY.keys())}"
        )
    return SCHEDULE_REGISTRY[clean_name]


__all__ = [
    "DefaultScheduleHandler",
    "ScheduleHandler",
    "SCHEDULE_REGISTRY",
    "get_schedule_handler_cls",
]
