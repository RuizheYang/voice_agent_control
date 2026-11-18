"""内置业务模块注册表；新增业务只需增加实现和一条注册。"""
from app.summary.scenarios.single_call import SingleCallScenario

SUMMARY_SCENARIOS = {
    "single_call": SingleCallScenario,
    "single_call_summary": SingleCallScenario,
    "recommend-common-interest": SingleCallScenario,
}


def normalize_summary_type(value: str | None) -> str:
    if value is not None and not isinstance(value, str):
        raise ValueError("summary_type 必须是字符串")
    name = (value or "").strip() or "single_call"
    if name not in SUMMARY_SCENARIOS:
        return "single_call"
    return name
