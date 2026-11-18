# Clue State Machine

Source of truth: `project/app/clue_handler/state.py` (`ClueState` & `VALID_TRANSITIONS`).

## States

- `created`: Newly ingested clue before scheduling.
- `scheduled`: Queued for outbound dialing (`next_call_at` / `ExecutionTask(task_type="call")`).
- `wait_for_calling`: Claimed for dialing, awaiting telephony gateway submission/start.
- `calling`: Active outbound call in progress.
- `summary`: Post-call Agent evaluation and scheduling decision in progress.
- `pending`: Target reached, waiting on external channel action confirmation (e.g. WeChat / SMS).
- `accepted`: Terminal success state.
- `rejected`: Terminal closure/circuit-breaker/cancellation state.
- `completed`, `cancelled`, `failed`: Legacy-compatible terminal states.

All state transitions are guarded by Redis distributed locks (`redis_manager.get_lock`) and validated against `VALID_TRANSITIONS`.
