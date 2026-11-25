"""Unified RSI durable background worker with retry backoff and DLQ."""
from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

from tortoise.expressions import Q
from tortoise.transactions import in_transaction

from app.rsi.coalescer import MessageCoalescer, message_coalescer
from app.rsi.models import RsiTask
from app.rsi.timers import evaluate_watchdog

logger = logging.getLogger(__name__)

_RSI_WAKEUP_SIGNAL = asyncio.Event()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def notify_rsi_worker() -> None:
    """Wake background RSI worker immediately when a due task is scheduled."""
    try:
        _RSI_WAKEUP_SIGNAL.set()
    except Exception:
        pass


async def _default_cognitive_handler(batch_context: Dict[str, Any]) -> Dict[str, Any]:
    """Placeholder cognitive handler until Steward & SubFlowRuntime are attached in Step 4/5."""
    return {
        "handled_by": "default_rsi_stub",
        "event_count": len(batch_context.get("events") or []),
    }


class RsiInboxWorker:
    """Consumes due RsiTask records with lease locking, exponential retry, and DLQ."""

    def __init__(
        self,
        *,
        cognitive_handler: Optional[Callable[[Dict[str, Any]], Awaitable[Any]]] = None,
        steward: Optional[Any] = None,
        coalescer: Optional[MessageCoalescer] = None,
        dream_model: Any = None,
        dreamer: Optional[Any] = None,
        lease_seconds: int = 60,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self.clock = clock or _utc_now
        self.steward = steward
        if cognitive_handler is not None:
            self.cognitive_handler = cognitive_handler
        elif steward is not None:
            self.cognitive_handler = steward.as_worker_handler(clock=self.clock)
        else:
            self.cognitive_handler = _default_cognitive_handler
        self.coalescer = coalescer or message_coalescer
        self.dream_model = dream_model
        self.dreamer = dreamer
        self.lease_seconds = max(1, int(lease_seconds))
        self.owner_id = f"{socket.gethostname()}-{os.getpid()}"

    async def claim_next_task(self) -> Optional[RsiTask]:
        now = self.clock()
        lease_until = now + timedelta(seconds=self.lease_seconds)
        token = uuid.uuid4().hex

        async with in_transaction() as conn:
            eligible = (
                Q(status__in=["pending", "retry"], run_at__lte=now)
                | Q(status="processing", locked_until__lt=now)
            )
            candidate = (
                await RsiTask.filter(eligible)
                .using_db(conn)
                .order_by("-priority", "run_at", "id")
                .first()
            )
            if candidate is None:
                return None

            updated = (
                await RsiTask.filter(eligible, id=candidate.id)
                .using_db(conn)
                .update(
                    status="processing",
                    lock_owner=self.owner_id,
                    lock_token=token,
                    locked_until=lease_until,
                )
            )
            if not updated:
                return None

            return await RsiTask.filter(id=candidate.id).using_db(conn).first()

    async def _finish_task(
        self,
        task_id: int,
        lock_token: str,
        *,
        status: str,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None,
    ) -> str:
        now = self.clock()
        async with in_transaction() as conn:
            task = (
                await RsiTask.filter(id=task_id)
                .using_db(conn)
                .select_for_update()
                .first()
            )
            if task is None or task.lock_token != lock_token:
                return task.status if task else "missing"

            task.lock_owner = None
            task.lock_token = None
            task.locked_until = None

            if status in ("succeeded", "superseded", "cancelled"):
                task.status = status
                task.result = dict(result or {"ok": True})
                task.finished_at = now
                if status == "succeeded":
                    task.last_error = None
            else:
                task.last_error = (
                    f"{type(error).__name__}: {str(error)[:500]}"
                    if error is not None
                    else "unknown_error"
                )
                if task.retry_count < task.max_retries:
                    task.retry_count += 1
                    backoff = min(60, 2 ** task.retry_count)
                    task.status = "retry"
                    task.run_at = now + timedelta(seconds=backoff)
                else:
                    task.status = "dead_letter"
                    task.finished_at = now

            await task.save(using_db=conn)
            return task.status

    async def process_task(self, task: RsiTask) -> str:
        token = task.lock_token or ""
        try:
            if task.task_type == "timer_watchdog":
                res = await evaluate_watchdog(
                    task.tenant_id, task.entity_id, task.payload or {}
                )
                return await self._finish_task(
                    task.id, token, status="succeeded", result=res
                )

            if task.task_type in (
                "cognitive_wakeup",
                "timer_self_check",
                "timer_commitment",
                "timer_pre_call_notice",
            ):
                outcome = await self.coalescer.execute_with_preemption(
                    task.tenant_id,
                    task.entity_id,
                    task,
                    self.cognitive_handler,
                )
                if outcome.status in ("preempted", "discarded_stale"):
                    return await self._finish_task(
                        task.id,
                        token,
                        status="superseded",
                        result=outcome.model_dump(mode="json"),
                    )
                return await self._finish_task(
                    task.id,
                    token,
                    status="succeeded",
                    result={
                        "processed_event_ids": outcome.processed_event_ids,
                        "output": outcome.output,
                    },
                )

            if task.task_type == "memory_dream":
                from app.rsi.memory_dreamer import RsiMemoryDreamer

                dreamer = self.dreamer or RsiMemoryDreamer(model=self.dream_model)
                res = await dreamer.dream_entity(
                    task.tenant_id,
                    task.entity_id,
                    now=self.clock(),
                )
                return await self._finish_task(
                    task.id, token, status="succeeded", result=res
                )

            return await self._finish_task(
                task.id,
                token,
                status="succeeded",
                result={"ignored_task_type": task.task_type},
            )
        except Exception as err:
            logger.error(
                "[RSI-Worker] Task execution error task_id=%s type=%s err=%s",
                task.id,
                task.task_type,
                err,
                exc_info=True,
            )
            return await self._finish_task(
                task.id, token, status="error", error=err
            )

    async def run_once(self) -> bool:
        task = await self.claim_next_task()
        if task is None:
            return False
        await self.process_task(task)
        return True


async def list_dead_letter_tasks(
    tenant_id: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Query tasks that exhausted retries and entered dead_letter state."""
    qs = RsiTask.filter(status="dead_letter")
    if tenant_id:
        qs = qs.filter(tenant_id=tenant_id)
    rows = await qs.order_by("-finished_at", "-id").limit(limit).all()
    return [
        {
            "task_id": r.id,
            "tenant_id": r.tenant_id,
            "entity_id": r.entity_id,
            "task_type": r.task_type,
            "priority": r.priority,
            "retry_count": r.retry_count,
            "max_retries": r.max_retries,
            "last_error": r.last_error,
            "payload": r.payload or {},
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        }
        for r in rows
    ]


async def requeue_dead_letter_task(task_id: int) -> Optional[RsiTask]:
    """Manually reset a dead_letter task back to pending for replay execution."""
    now = _utc_now()
    async with in_transaction() as conn:
        task = (
            await RsiTask.filter(id=task_id, status="dead_letter")
            .using_db(conn)
            .select_for_update()
            .first()
        )
        if task is None:
            return None
        task.status = "pending"
        task.retry_count = 0
        task.run_at = now
        task.last_error = None
        task.finished_at = None
        task.lock_owner = None
        task.lock_token = None
        task.locked_until = None
        await task.save(using_db=conn)
    notify_rsi_worker()
    return task


async def rsi_worker_loop() -> None:
    """Always-on RSI worker loop sleeping cleanly until signal or 1.0s interval."""
    worker = RsiInboxWorker()
    logger.info("[RSI-Worker] Unified RSI background worker started")
    while True:
        try:
            processed_any = False
            while await worker.run_once():
                processed_any = True
            if not processed_any:
                _RSI_WAKEUP_SIGNAL.clear()
                try:
                    await asyncio.wait_for(_RSI_WAKEUP_SIGNAL.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            logger.info("[RSI-Worker] Unified RSI background worker stopped")
            raise
        except Exception as err:
            logger.error("[RSI-Worker] Loop error, retrying in 5s: %s", err, exc_info=True)
            await asyncio.sleep(5.0)
