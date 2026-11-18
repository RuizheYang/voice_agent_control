"""Registry for analyzer definitions available to summary pipelines."""

from app.summary.analyzers.common_call_events import ANALYZER as COMMON_CALL_EVENTS


ANALYZERS_BY_NAME = {COMMON_CALL_EVENTS.name: COMMON_CALL_EVENTS}

__all__ = [
    "ANALYZERS_BY_NAME",
    "COMMON_CALL_EVENTS",
]
