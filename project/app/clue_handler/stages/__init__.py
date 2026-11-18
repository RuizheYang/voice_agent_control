from .inflow import (
    DefaultInflowHandler,
    NewClueHandler,
    INFLOW_REGISTRY,
    get_inflow_handler_cls,
)
from .postcall import (
    DefaultPostCallHandler,
    PostCallHandler,
    POSTCALL_REGISTRY,
    get_postcall_handler_cls,
)
from .schedule import (
    DefaultScheduleHandler,
    ScheduleHandler,
    SCHEDULE_REGISTRY,
    get_schedule_handler_cls,
)
from .cancel import (
    DefaultCancelHandler,
    CancelCallHandler,
    CancelClueResult,
    CANCEL_REGISTRY,
    get_cancel_handler_cls,
)

__all__ = [
    # 阶段 1: 流入
    "DefaultInflowHandler",
    "NewClueHandler",
    "INFLOW_REGISTRY",
    "get_inflow_handler_cls",
    # 阶段 2: 挂机后
    "DefaultPostCallHandler",
    "PostCallHandler",
    "POSTCALL_REGISTRY",
    "get_postcall_handler_cls",
    # 阶段 3: 排期到期
    "DefaultScheduleHandler",
    "ScheduleHandler",
    "SCHEDULE_REGISTRY",
    "get_schedule_handler_cls",
    # 阶段 4: 取消跟进
    "DefaultCancelHandler",
    "CancelCallHandler",
    "CancelClueResult",
    "CANCEL_REGISTRY",
    "get_cancel_handler_cls",
]
