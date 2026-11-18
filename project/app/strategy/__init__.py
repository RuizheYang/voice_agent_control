from app.strategy.contracts import (
    BaseStrategyEngine,
    ScheduleResult,
    BaseStrategyConfig,
    ClueContext,
)
from app.strategy.common import (
    TimeRule,
    Relative,
    Absolute,
    Immediate,
    Reject,
    OutcomeBranch,
    InflowSlotBranch,
    CallOutcome,
    CallOutcomeClassifier,
    InflowSlot,
)
from app.strategy.single_call import (
    SingleCallStrategyConfig,
    SingleCallStrategyEngine,
)
from app.strategy.sevenday import (
    SevenDayStrategyConfig,
    SevenDayStrategyEngine,
    SevenDayThreeCallStrategyEngine,
    SEVENDAY_MATRIX,
    SEVENDAY_WORKING_PRIORITY,
    SEVENDAY_NON_WORKING_PRIORITY,
)
from app.strategy.agent_driven import (
    AgentStrategyConfig,
    AgentDrivenStrategyEngine,
    PolicyHarness,
    StandardBusinessStatus,
    MasterSummaryDecision,
)
from app.strategy.factory import (
    StrategyEngineFactory,
    StrategyEngineResolver,
    SCHEDULE_REGISTRY,
    SUPPORTED_SCHEDULE_IDS,
)

# 历史向后兼容别名
OutboundStrategyEngine = SevenDayStrategyEngine

__all__ = [
    # 顶层契约与上下文
    "BaseStrategyEngine",
    "ScheduleResult",
    "BaseStrategyConfig",
    "ClueContext",
    # 规则 DSL 与模型
    "TimeRule",
    "Relative",
    "Absolute",
    "Immediate",
    "Reject",
    "OutcomeBranch",
    "InflowSlotBranch",
    "CallOutcome",
    "CallOutcomeClassifier",
    "InflowSlot",
    # 策略 1: 单通即结束策略 (single_call)
    "SingleCallStrategyConfig",
    "SingleCallStrategyEngine",
    # 策略 2: 七日每日三呼策略 (seven_day_three_call)
    "SevenDayStrategyConfig",
    "SevenDayStrategyEngine",
    "SevenDayThreeCallStrategyEngine",
    "SEVENDAY_MATRIX",
    "SEVENDAY_WORKING_PRIORITY",
    "SEVENDAY_NON_WORKING_PRIORITY",
    # 策略 3: 完全由 Agent 驱动的智能多场景排期策略 (agent_driven)
    "AgentStrategyConfig",
    "AgentDrivenStrategyEngine",
    "PolicyHarness",
    "StandardBusinessStatus",
    "MasterSummaryDecision",
    # 工厂与注册表
    "StrategyEngineFactory",
    "StrategyEngineResolver",
    "SCHEDULE_REGISTRY",
    "SUPPORTED_SCHEDULE_IDS",
    "OutboundStrategyEngine",
]
