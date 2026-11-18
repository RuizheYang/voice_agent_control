# Voice Agent Control

Always-On **Agent-Driven Outbound Voice Scheduling & Multi-Business Cartridge Control Plane** built with **FastAPI**, **PydanticAI**, **Tortoise ORM**, and **Redis**.

---

## Core Architecture

1. **Agent-Driven Scheduling Engine (`AgentDrivenStrategyEngine`)**
   - Parallel **Map-Reduce Evaluators**: concurrent specialized evaluators (`intent`, `objection`, `appointment`, `sentiment`) + declarative YAML/DB custom evaluators (`pydantic.create_model`) feeding a master `MasterDecisionSynthesizer`.
   - **PolicyHarness Guardrails**: deterministic safety boundary enforcing working-window snapping, max call/no-answer circuit breakers, appointment corridor snapping, and verbatim transcript evidence checks.
   - **5s Heuristic Safety Fallback**: zero-blocking deterministic fallback when LLM inference times out or errors.
2. **"Game Cartridge" Multi-Business Switching (`BusinessDomainPack` + `ClueSupervisorAgent`)**
   - Stateless `ClueSupervisorAgent` dynamically loads per-workflow YAML cartridges (`project/app/domains/*.yaml`) or DB overrides (`WorkflowDomainConfig`) with `PydanticAI` `CombinedToolset` isolation (`.filtered()` / `.prepared()` / `.approval_required()`).
   - **Atomic Cross-Workflow Handoff** (`atomic_handoff_clue_workflow`) with anti-ping-pong depth guardrails (`max_handoffs`), pending task cleanup, and automatic `OutboundStrategy` snapshot refresh.
   - **Auto-Triage (`workflowId="auto"`)** and pre-release **`POST /workflow/{workflow_id}/dry-run`** validation.
3. **Always-On Event Inbox + Wakeup Primitives**
   - **Fast-ACK Event Inbox (`POST /clue/call-end/notify`)**: idempotent persistence into `ExecutionTask(task_type="call_end_inbox")` + instant `asyncio.Event` signal for millisecond response (`sync=true` optional for synchronous debugging).
   - **Background Wakeup Engine (`InboxWakeupWorker`)**: lease-locked execution of `call_end_inbox` and agent-scheduled `agent_wakeup` self-timers with exponential backoff on lock/transient errors.
   - **4-Bucket Memory + Async Dreaming (`MemoryDreamer`)**: `instructions`, `customer_notes`, `interaction_digest`, and `progress` compacted asynchronously off the hot path.
   - **Global Cross-Workflow Outbox Pacer**: phone-level 24h touch-cap throttling across workflows.
4. **Pluggable Telephony Gateway (`BaseTelephonyGateway`)**
   - Ships with **`MockTelephonyGateway`** (zero external telephony setup required) and **`GenericWebhookGateway`** for arbitrary HTTP JSON outbound providers.

---

## Quick Start (Local Dev / Sandbox)

### 1. Install Dependencies

```bash
uv sync
# or
pip install -r project/requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Optional JSON config:
cp project/app/configs/config.example.json project/app/configs/dev.json
```

Default local settings run out-of-the-box with SQLite (`sqlite://dbfiles/voice_agent_hub.db`) and `TELEPHONY_GATEWAY_MODE=mock`.

### 3. Run the Service

```bash
cd project
ENV=dev uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

- Interactive Simulator: `http://localhost:8001/simulator`
- Clue Event Trace Monitor: `http://localhost:8001/event-monitor`
- Analytics Dashboard: `http://localhost:8001/analytics`
- Health Check: `http://localhost:8001/voice-agent-hub/monitor`

### 4. Docker Compose

```bash
docker compose up --build
```

---

## Project Layout

```text
voice_agent_control/
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── docs/
│   ├── ARCHITECTURE.md
│   ├── EXTERNAL_API.md
│   └── STATE_MACHINE.md
└── project/
    ├── app/
    │   ├── main.py                  # FastAPI entrypoint & background workers lifespan
    │   ├── config.py                # Env/JSON config loader (with DSN masking)
    │   ├── agents/                  # PydanticAI Supervisor, ToolBus, MemoryDreamer, InboxWakeupWorker
    │   ├── domains/                 # Built-in YAML business cartridges (new_car_sales, car_loan)
    │   ├── strategy/agent_driven/   # AgentDrivenStrategyEngine, Evaluators, Synthesizer, Harness, Fallback
    │   ├── clue_handler/            # Lifecycle state machine & stage handlers (inflow, postcall, schedule, cancel)
    │   ├── clue_inflow/             # Single/batch/file inflow ingestion & ExecutionTask outbox
    │   ├── call_schedule/           # Lease-based call dispatch & reconciliation workers
    │   ├── apis/telephony.py        # BaseTelephonyGateway (MockTelephonyGateway / GenericWebhookGateway)
    │   ├── routers/                 # inflow, config_admin, clue_webhook, observability, analytics
    │   └── dbmodels/                # Tortoise ORM models & atomic CRUD operations
    ├── static/                      # Simulator, Event Monitor, Analytics HTML panels
    └── tests/                       # Automated pytest suite
```

---

## Running Tests

```bash
PYTHONPATH=project .venv/bin/pytest project/tests/ -v
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
