"""Per-entity concurrency lock and 3-level messaging debounce / preemption controller."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from app.rsi.models import EntityEventLog, FollowupEntity, RsiTask
from app.rsi.schemas import CoalesceRunOutcome
from app.rsi.timers import schedule_timer

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _db(queryset, connection):
    return queryset.using_db(connection) if connection is not None else queryset


@dataclass
class InFlightRun:
    task: asyncio.Task
    started_msg_seq: int
    preempted: bool = False
    reason: str = ""


class MessageCoalescer:
    """
    Coordinates per-entity locking and 3-level inbound message backlog control:
    - Level 1: Short-window debounce coalescing in RsiTask
    - Level 2: Cancel in-flight cognitive task on newer inbound customer message
    - Level 3: Optimistic inbound_msg_seq check before committing outbound reply
    """

    def __init__(self) -> None:
        self._entity_locks: Dict[Tuple[str, str], asyncio.Lock] = {}
        self._inflight_runs: Dict[Tuple[str, str], InFlightRun] = {}

    def reset(self) -> None:
        """Clear runtime lock/in-flight registries (primarily for test isolation)."""
        self._entity_locks.clear()
        self._inflight_runs.clear()

    def get_lock(self, tenant_id: str, entity_id: str) -> asyncio.Lock:
        key = (tenant_id, entity_id)
        lock = self._entity_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._entity_locks[key] = lock
        return lock

    async def enqueue_debounced_message_wakeup(
        self,
        tenant_id: str,
        entity_id: str,
        event_id: str,
        *,
        debounce_ms: int = 2500,
        priority: int = 8,
        now: Optional[datetime] = None,
        connection=None,
    ) -> RsiTask:
        """Level 1: Merge bursty inbound messages into a single debounced wakeup task."""
        now_dt = now or _utc_now()
        due_at = now_dt + timedelta(milliseconds=max(0, int(debounce_ms)))

        ent = await _db(
            FollowupEntity.filter(tenant_id=tenant_id, entity_id=entity_id),
            connection,
        ).first()
        cur_seq = ent.inbound_msg_seq if ent else 0

        pending = await _db(
            RsiTask.filter(
                tenant_id=tenant_id,
                entity_id=entity_id,
                task_type="cognitive_wakeup",
                status="pending",
            ).order_by("id"),
            connection,
        ).all()

        existing: Optional[RsiTask] = None
        for item in pending:
            if (item.payload or {}).get("channel_mode") == "messaging_debounce":
                existing = item
                break

        if existing is not None:
            payload = dict(existing.payload or {})
            event_ids = list(payload.get("event_ids") or [])
            if event_id not in event_ids:
                event_ids.append(event_id)
            payload["event_ids"] = event_ids
            payload["latest_msg_seq"] = cur_seq
            existing.payload = payload
            existing.run_at = due_at
            existing.priority = max(existing.priority, int(priority))
            if connection is not None:
                await existing.save(
                    using_db=connection,
                    update_fields=["payload", "run_at", "priority", "update_time"],
                )
            else:
                await existing.save(
                    update_fields=["payload", "run_at", "priority", "update_time"]
                )
            return existing

        return await schedule_timer(
            tenant_id=tenant_id,
            entity_id=entity_id,
            timer_type="cognitive_wakeup",
            run_at=due_at,
            payload={
                "channel_mode": "messaging_debounce",
                "event_ids": [event_id],
                "latest_msg_seq": cur_seq,
            },
            priority=priority,
            replace_existing=False,
            connection=connection,
        )

    def preempt_inflight_run(
        self,
        tenant_id: str,
        entity_id: str,
        *,
        reason: str = "newer_inbound_message",
    ) -> bool:
        """Level 2: Interrupt active cognitive run when newer customer message arrives."""
        run = self._inflight_runs.get((tenant_id, entity_id))
        if run is None or run.task.done():
            return False
        if not run.preempted:
            run.preempted = True
            run.reason = reason
            run.task.cancel()
        return True

    async def verify_msg_seq_before_send(
        self,
        tenant_id: str,
        entity_id: str,
        started_msg_seq: int,
        *,
        connection=None,
    ) -> bool:
        """Level 3: Optimistic guard ensuring no newer customer message arrived mid-inference."""
        ent = await _db(
            FollowupEntity.filter(tenant_id=tenant_id, entity_id=entity_id),
            connection,
        ).first()
        return ent is not None and ent.inbound_msg_seq == started_msg_seq

    async def execute_with_preemption(
        self,
        tenant_id: str,
        entity_id: str,
        task: RsiTask,
        handler_fn: Callable[[Dict[str, Any]], Awaitable[Any]],
    ) -> CoalesceRunOutcome:
        """Run cognitive handler under per-entity lock with Level 2 preemption & Level 3 validation."""
        key = (tenant_id, entity_id)
        lock = self.get_lock(tenant_id, entity_id)

        async with lock:
            ent = await FollowupEntity.filter(
                tenant_id=tenant_id, entity_id=entity_id
            ).first()
            started_msg_seq = ent.inbound_msg_seq if ent else 0

            unconsumed = await EntityEventLog.filter(
                tenant_id=tenant_id,
                entity_id=entity_id,
                ingest_status="ingested",
            ).order_by("occurred_at", "id").all()

            batch_context: Dict[str, Any] = {
                "tenant_id": tenant_id,
                "entity_id": entity_id,
                "task_id": task.id,
                "task_type": task.task_type,
                "task_payload": dict(task.payload or {}),
                "started_msg_seq": started_msg_seq,
                "events": [
                    {
                        "event_id": r.event_id,
                        "event_type": r.event_type,
                        "category": r.category,
                        "occurred_at": r.occurred_at.isoformat() if r.occurred_at else None,
                        "actor_type": r.actor_type,
                        "actor_id": r.actor_id,
                        "summary": r.summary,
                        "facts": dict(r.facts or {}),
                        "extra": dict(r.extra or {}),
                    }
                    for r in unconsumed
                ],
            }

            cur_task = asyncio.current_task()
            run = InFlightRun(task=cur_task, started_msg_seq=started_msg_seq) if cur_task else None
            if run is not None:
                self._inflight_runs[key] = run

            try:
                output = await handler_fn(batch_context)
            except asyncio.CancelledError:
                if run is not None and run.preempted:
                    if cur_task is not None and hasattr(cur_task, "uncancel"):
                        while cur_task.cancelling() > 0:
                            cur_task.uncancel()
                    latest_ent = await FollowupEntity.filter(
                        tenant_id=tenant_id, entity_id=entity_id
                    ).first()
                    latest_seq = latest_ent.inbound_msg_seq if latest_ent else started_msg_seq
                    logger.info(
                        "[RSI-Coalescer] Level-2 preempted run: entity=%s seq=%s->%s reason=%s",
                        entity_id,
                        started_msg_seq,
                        latest_seq,
                        run.reason,
                    )
                    return CoalesceRunOutcome(
                        status="preempted",
                        started_msg_seq=started_msg_seq,
                        latest_msg_seq=latest_seq,
                    )
                raise
            finally:
                if run is not None and self._inflight_runs.get(key) is run:
                    self._inflight_runs.pop(key, None)

            # Level 3: pre-send optimistic inbound_msg_seq verification
            is_fresh = await self.verify_msg_seq_before_send(
                tenant_id, entity_id, started_msg_seq
            )
            if not is_fresh:
                latest_ent = await FollowupEntity.filter(
                    tenant_id=tenant_id, entity_id=entity_id
                ).first()
                latest_seq = latest_ent.inbound_msg_seq if latest_ent else started_msg_seq
                has_pending = await RsiTask.filter(
                    tenant_id=tenant_id,
                    entity_id=entity_id,
                    task_type="cognitive_wakeup",
                    status="pending",
                ).exists()
                if not has_pending:
                    await schedule_timer(
                        tenant_id=tenant_id,
                        entity_id=entity_id,
                        timer_type="cognitive_wakeup",
                        run_at=_utc_now(),
                        payload={
                            "reason": "rescheduled_after_stale_msg_seq",
                            "latest_msg_seq": latest_seq,
                        },
                        priority=8,
                        replace_existing=False,
                    )
                logger.info(
                    "[RSI-Coalescer] Level-3 discarded stale output: entity=%s started_seq=%s latest_seq=%s",
                    entity_id,
                    started_msg_seq,
                    latest_seq,
                )
                return CoalesceRunOutcome(
                    status="discarded_stale",
                    started_msg_seq=started_msg_seq,
                    latest_msg_seq=latest_seq,
                )

            consumed_ids = [r.event_id for r in unconsumed]
            if consumed_ids:
                await EntityEventLog.filter(
                    tenant_id=tenant_id,
                    event_id__in=consumed_ids,
                    ingest_status="ingested",
                ).update(ingest_status="processed")

            return CoalesceRunOutcome(
                status="committed",
                started_msg_seq=started_msg_seq,
                latest_msg_seq=started_msg_seq,
                processed_event_ids=consumed_ids,
                output=output,
            )


message_coalescer = MessageCoalescer()
