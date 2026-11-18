from typing import Dict, Type
from app.clue_handler.contracts.base_postcall import BasePostCallHandler
from .default import (
    DefaultPostCallHandler,
    PostCallHandler,
    extract_conversation_messages,
)

# 通话挂机后阶段处理器注册表
POSTCALL_REGISTRY: Dict[str, Type[BasePostCallHandler]] = {
    "default": DefaultPostCallHandler,
}


def get_postcall_handler_cls(name: str = "default") -> Type[BasePostCallHandler]:
    """根据注册名称获取对应的 PostCallHandler 类"""
    clean_name = (name.strip().lower() if name else "default")
    if clean_name not in POSTCALL_REGISTRY:
        raise ValueError(
            f"[PostCallStage] 未知的 PostCallHandler 标识: '{name}'，"
            f"可选方案包括: {list(POSTCALL_REGISTRY.keys())}"
        )
    return POSTCALL_REGISTRY[clean_name]


__all__ = [
    "DefaultPostCallHandler",
    "PostCallHandler",
    "extract_conversation_messages",
    "POSTCALL_REGISTRY",
    "get_postcall_handler_cls",
]
