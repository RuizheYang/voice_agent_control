# External API Reference

## 1. Configuration & Cartridge Management

- `POST /voice-agent/create` — Create or update a Voice Agent definition.
- `GET /voice-agent/{agent_identifier}` — Fetch a Voice Agent configuration.
- `POST /strategy/create` — Create or update an `OutboundStrategy` (`schedule_id="agent_driven"`).
- `GET /strategy/{strategy_id}` — Fetch an `OutboundStrategy`.
- `POST /workflow/bundle/create` — Provision workflow binding, agent, strategy, and `domain_config` cartridge atomically.
- `POST /workflow/create` / `GET /workflow/{workflow_id}` — Manage workflow bindings and domain cartridges.
- `POST /workflow/{workflow_id}/dry-run` — Simulate evaluators, visible tools, and `PolicyHarness` without mutating DB state.
- `POST /control/pause`, `POST /control/resume`, `GET /control/status` — Pause/resume inflow or dispatch loops.
- `POST /auth/register` — Register HMAC app credentials (`app_id`, `app_secret`, `allowed_workflows`).

## 2. Clue Ingestion (`/clue/inflow/*`)

Requires HMAC signature headers (`X-App-Id`, `X-Timestamp`, `X-Nonce`, `X-Signature`). Supports `workflowId="auto"` for automatic `ClueSupervisorAgent` triage.

- `POST /clue/inflow/create` — Single clue ingestion.
- `POST /clue/inflow/create/batch` — Batch ingestion.
- `POST /clue/inflow/create/file` — Async CSV/JSON file import.
- `GET /clue/inflow/import-tasks/{task_id}` & `/errors` — Poll file import status and row errors.

## 3. Telephony Lifecycle Webhooks (`/clue/*`)

- `POST /clue/call-start/notify` — Mark clue as `calling` when outbound dialing starts.
- `POST /clue/call-end/notify?sync=false` — Fast-ACK Event Inbox ingestion (persists idempotent `call_end_inbox` task and wakes `InboxWakeupWorker`; pass `sync=true` for synchronous execution).
- `POST /clue/cancel` — Cancel active follow-up and close clue.

## 4. Observability & Analytics

- `GET /api/observability/clue-trace?clueId=...` — Aggregated clue snapshot, call history, multi-agent memory, handoff history, and timeline events.
- `GET|POST /api/observability/clue-events?clueId=...` — Query event stream for a clue.
- `GET|POST /api/observability/clue-context?clueId=...` — Inspect `ClueContext`.
- `GET /api/analytics/dashboard-stats` — Aggregated clue, call, inbox, and wakeup metrics.
