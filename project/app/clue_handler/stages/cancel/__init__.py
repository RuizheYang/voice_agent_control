from typing import Dict, Type
from app.clue_handler.contracts.base_cancel import BaseCancelHandler, CancelClueResult
from .default import DefaultCancelHandler, CancelCallHandler

# 取消跟进阶段处理器注册表
CANCEL_REGISTRY: Dict[str, Type[BaseCancelHandler]] = {
    "default": DefaultCancelHandler,
}


def get_cancel_handler_cls(name: str = "default") -> Type[BaseCancelHandler]:
    """根据注册名称获取对应的 CancelHandler 类"""
    clean_name = (name.strip().lower() if name else "default")
    if clean_name not in CANCEL_REGISTRY:
        raise ValueError(
            f"[CancelStage] 未知的 CancelHandler 标识: '{name}'，"
            f"可选方案包括: {list(CANCEL_REGISTRY.keys())}"
        )
    return CANCEL_REGISTRY[clean_name]


__all__ = [
    "DefaultCancelHandler",
    "CancelCallHandler",
    "CancelClueResult",
    "CANCEL_REGISTRY",
    "get_cancel_handler_cls",
]
