"""Unified RSI V3 data-plane event ingestion & read/replay API router."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.rsi.adapters.base import ack_action_result, query_action_results
from app.rsi.memory_reducer import (
    get_entity_state,
    get_entity_timeline,
    ingest_event,
    replay_entity_state,
)
from app.rsi.schemas import (
    ActionResultListResponse,
    BatchEventIngestResponse,
    EntityReplayResponse,
    EntityStateResponse,
    EntityTimelineResponse,
    EventIngestResult,
    RSIEventBatchRequest,
    RSIEventEnvelope,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["RSI Events & Memory"])


@router.post("/events", response_model=EventIngestResult, summary="统一客户事件摄入入口")
async def post_event(envelope: RSIEventEnvelope) -> EventIngestResult:
    """Ingest a single standardized customer event idempotently into L1/L2 RSI memory."""
    return await ingest_event(envelope)


@router.post("/events/batch", response_model=BatchEventIngestResponse, summary="批量客户事件摄入入口")
async def post_events_batch(payload: RSIEventBatchRequest) -> BatchEventIngestResponse:
    """Ingest a batch of standardized customer events sequentially with per-event isolation."""
    accepted = 0
    duplicates = 0
    failed = 0
    results = []

    for ev in payload.events:
        try:
            res = await ingest_event(ev)
            if res.duplicate:
                duplicates += 1
            else:
                accepted += 1
            results.append(res.model_dump(mode="json"))
        except Exception as err:
            failed += 1
            logger.warning("[RSI-Events] Batch item failed event_id=%s: %s", ev.event_id, err)
            results.append(
                {
                    "event_id": ev.event_id,
                    "tenant_id": ev.tenant_id,
                    "error": f"{type(err).__name__}: {err}",
                }
            )

    return BatchEventIngestResponse(
        accepted=accepted,
        duplicates=duplicates,
        failed=failed,
        results=results,
    )


@router.get("/entities/{entity_id}/state", response_model=EntityStateResponse, summary="查询实体结构化记忆状态")
async def get_state(
    entity_id: str,
    tenant_id: str = Query(default="default", min_length=1, max_length=64),
) -> EntityStateResponse:
    """Return canonical L2+L3 entity memory state and bound cross-channel aliases."""
    state = await get_entity_state(tenant_id, entity_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"Entity '{entity_id}' not found")
    return state


@router.get("/entities/{entity_id}/timeline", response_model=EntityTimelineResponse, summary="查询实体L1不可变事件时间线")
async def get_timeline(
    entity_id: str,
    tenant_id: str = Query(default="default", min_length=1, max_length=64),
    limit: int = Query(default=50, ge=1, le=500),
    category: Optional[str] = Query(default=None, max_length=64),
) -> EntityTimelineResponse:
    """Return chronological immutable L1 event history for an entity."""
    timeline = await get_entity_timeline(tenant_id, entity_id, limit=limit, category=category)
    if timeline is None:
        raise HTTPException(status_code=404, detail=f"Entity '{entity_id}' not found")
    return timeline


@router.post("/entities/{entity_id}/replay", response_model=EntityReplayResponse, summary="从L1事件账本确定性重放实体状态")
async def post_replay(
    entity_id: str,
    tenant_id: str = Query(default="default", min_length=1, max_length=64),
) -> EntityReplayResponse:
    """Deterministically rebuild L2 EntityMemoryState from immutable L1 EntityEventLog."""
    replayed = await replay_entity_state(tenant_id, entity_id)
    if replayed is None:
        raise HTTPException(status_code=404, detail=f"Entity '{entity_id}' not found")
    return replayed


@router.get("/outbox/results", response_model=ActionResultListResponse, summary="查询动作护栏裁决与边缘执行结果(供CRM决策)")
async def get_outbox_results(
    tenant_id: str = Query(default="default", min_length=1, max_length=64),
    entity_id: Optional[str] = Query(default=None, max_length=128),
    channel: Optional[str] = Query(default=None, max_length=64),
    verdict: Optional[str] = Query(default=None, max_length=32),
    execution_status: Optional[str] = Query(default=None, max_length=32),
    limit: int = Query(default=50, ge=1, le=500),
) -> ActionResultListResponse:
    """Read-only ActionResultStore endpoint exposed to upstream CRM for decisioning."""
    return await query_action_results(
        tenant_id,
        entity_id=entity_id,
        channel=channel,
        verdict=verdict,
        execution_status=execution_status,
        limit=limit,
    )


@router.post("/outbox/results/{result_id}/ack", summary="上层CRM确认已消费动作执行结果")
async def post_ack_outbox_result(
    result_id: int,
    tenant_id: str = Query(default="default", min_length=1, max_length=64),
) -> dict:
    """Mark an ActionResultStore entry as acknowledged by upstream CRM."""
    row = await ack_action_result(result_id, tenant_id=tenant_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"ActionExecutionResult '{result_id}' not found")
    return {
        "result_id": row.id,
        "tenant_id": row.tenant_id,
        "outbox_id": row.outbox_id,
        "crm_acked_at": row.crm_acked_at.isoformat() if row.crm_acked_at else None,
    }

