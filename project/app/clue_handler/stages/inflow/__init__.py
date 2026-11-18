from typing import Dict, Type
from app.clue_handler.contracts.base_inflow import BaseInflowHandler
from .default import DefaultInflowHandler, NewClueHandler

# 线索流入阶段处理器注册表
INFLOW_REGISTRY: Dict[str, Type[BaseInflowHandler]] = {
    "default": DefaultInflowHandler,
}


def get_inflow_handler_cls(name: str = "default") -> Type[BaseInflowHandler]:
    """根据注册名称获取对应的 InflowHandler 类"""
    clean_name = (name.strip().lower() if name else "default")
    if clean_name not in INFLOW_REGISTRY:
        raise ValueError(
            f"[InflowStage] 未知的 InflowHandler 标识: '{name}'，"
            f"可选方案包括: {list(INFLOW_REGISTRY.keys())}"
        )
    return INFLOW_REGISTRY[clean_name]


__all__ = [
    "DefaultInflowHandler",
    "NewClueHandler",
    "INFLOW_REGISTRY",
    "get_inflow_handler_cls",
]
