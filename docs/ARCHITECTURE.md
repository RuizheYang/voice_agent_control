# Architecture Overview

## End-to-End Flow

```mermaid
flowchart LR
    A["Clue Inflow /clue/inflow/create"] --> B["ExecutionTask (new_clue)"]
    B --> C["DefaultInflowHandler (+ auto triage)"]
    C --> D["Call Schedule Pool (call)"]
    D --> E["BaseTelephonyGateway (Mock / Webhook)"]
    E --> F["Call End Webhook /clue/call-end/notify"]
    F --> G["Fast-ACK Event Inbox (call_end_inbox)"]
    G --> H["InboxWakeupWorker"]
    H --> I["DefaultPostCallHandler -> AgentDrivenStrategyEngine"]
    I --> J["Sub-Evaluators + Master Synthesizer + PolicyHarness"]
    J -->|schedule| D
    J -->|wakeup timer| K["ExecutionTask (agent_wakeup)"]
    J -->|handoff| L["atomic_handoff_clue_workflow"]
    J -->|success / rejection| M["Terminal State + Webhook Dispatch"]
    I -->|async| N["MemoryDreamer (4-Bucket Memory)"]
```

## Key Subsystems

- **Business Cartridges (`project/app/agents/domain_registry.py`)**: YAML definitions in `project/app/domains/` overridden dynamically by `WorkflowDomainConfig` rows in DB.
- **Tool Isolation (`project/app/agents/tool_bus.py`)**: `PydanticAI` toolsets filtered per business domain with SSRF-protected `DeclarativeHttpToolFactory` and `CredentialProxy`.
- **Transactional Tasks (`ExecutionTask`)**: Unified outbox/inbox table for `import`, `new_clue`, `call`, `call_end_inbox`, `agent_wakeup`, `webhook`, and `internal_event`.
