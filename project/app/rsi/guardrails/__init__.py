"""Global Action Outbox guardrails, kill switch, working window, and approvals package."""
from app.rsi.guardrails.approvals import (
    list_pending_outbox_approvals,
    resolve_outbox_approval,
)
from app.rsi.guardrails.kill_switch import (
    is_killed,
    list_kill_switches,
    set_kill_switch,
)
from app.rsi.guardrails.outbox_guardrail import (
    DEFAULT_OUTBOUND_FORBIDDEN_WORDS,
    cancel_entity_outbox,
    dispatch_due_outbox_actions,
    evaluate_outbox_guardrails,
    execute_outbox_record,
    resolve_permission_level,
    scan_outbound_forbidden_word,
    submit_action_proposal,
)
from app.rsi.guardrails.time_window import (
    is_within_working_window,
    next_working_window_opening,
    parse_time_str,
    resolve_working_window,
    snap_to_working_window,
)

__all__ = [
    "DEFAULT_OUTBOUND_FORBIDDEN_WORDS",
    "parse_time_str",
    "resolve_working_window",
    "is_within_working_window",
    "next_working_window_opening",
    "snap_to_working_window",
    "set_kill_switch",
    "is_killed",
    "list_kill_switches",
    "scan_outbound_forbidden_word",
    "resolve_permission_level",
    "evaluate_outbox_guardrails",
    "execute_outbox_record",
    "submit_action_proposal",
    "dispatch_due_outbox_actions",
    "cancel_entity_outbox",
    "list_pending_outbox_approvals",
    "resolve_outbox_approval",
]
