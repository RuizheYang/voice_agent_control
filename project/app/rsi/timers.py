"""Durable RSI timers, priority boosting, cancellation, and anti-leak watchdogs."""
from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

from app.rsi.identity import resolve_canonical_entity
from app.rsi.models import EntityMemoryState, RsiTask

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _db(queryset, connection):
    return queryset.using_db(connection) if connection is not None else queryset


async def schedule_timer(
    tenant_id: str,
    entity_id: str,
    timer_type: str,
    run_at: datetime,
    *,
    payload: Optional[Dict[str, Any]] = None,
    priority: int = 5,
    replace_existing: bool = True,
    task_key: Optional[str] = None,
    max_retries: int = 3,
    connection=None,
) -> RsiTask:
    """Persist a durable wakeup/timer/watchdog task in RsiTask."""
    now = _utc_now()
    due_at = _to_utc(run_at)

    if replace_existing:
        await _db(
            RsiTask.filter(
                tenant_id=tenant_id,
                entity_id=entity_id,
                task_type=timer_type,
                status__in=["pending", "retry"],
            ),
            connection,
        ).update(
            status="superseded",
            last_error=f"superseded_by:{timer_type}",
            finished_at=now,
        )

    final_key = (
        task_key[:128]
        if task_key
        else hashlib.sha256(
            f"{timer_type}:{tenant_id}:{entity_id}:{uuid.uuid4().hex}".encode("utf-8")
        ).hexdigest()
    )
    kwargs = dict(
        tenant_id=tenant_id,
        task_key=final_key,
        task_type=timer_type,
        entity_id=entity_id,
        priority=max(1, min(10, int(priority))),
        status="pending",
        run_at=due_at,
        payload=dict(payload or {}),
        result={},
        retry_count=0,
        max_retries=max(0, int(max_retries)),
    )
    if connection is not None:
        return await RsiTask.create(using_db=connection, **kwargs)
    return await RsiTask.create(**kwargs)


async def cancel_entity_tasks(
    tenant_id: str,
    entity_id: str,
    *,
    task_types: Optional[Iterable[str]] = None,
    reason: str = "cancelled",
    connection=None,
) -> int:
    """Deterministically cancel pending/retry tasks for an entity (0-LLM withdrawal)."""
    qs = RsiTask.filter(
        tenant_id=tenant_id,
        entity_id=entity_id,
        status__in=["pending", "retry"],
    )
    if task_types is not None:
        types_list = list(task_types)
        if not types_list:
            return 0
        qs = qs.filter(task_type__in=types_list)

    count = await _db(qs, connection).update(
        status="cancelled",
        last_error=str(reason)[:500],
        finished_at=_utc_now(),
    )
    return int(count)


async def boost_entity_tasks(
    tenant_id: str,
    entity_id: str,
    *,
    new_priority: int = 9,
    run_at: Optional[datetime] = None,
    reason: str = "active_engagement_boost",
    connection=None,
) -> int:
    """Elevate priority and advance run_at of queued wakeup/self-check tasks."""
    target_due = _to_utc(run_at) if run_at is not None else _utc_now()
    tasks = await _db(
        RsiTask.filter(
            tenant_id=tenant_id,
            entity_id=entity_id,
            task_type__in=["cognitive_wakeup", "timer_self_check"],
            status__in=["pending", "retry"],
        ),
        connection,
    ).all()

    boosted = 0
    for task in tasks:
        task.priority = max(task.priority, int(new_priority))
        existing_due = _to_utc(task.run_at)
        if target_due < existing_due:
            task.run_at = target_due
        new_payload = dict(task.payload or {})
        new_payload["boosted"] = True
        new_payload["boost_reason"] = reason
        task.payload = new_payload
        if connection is not None:
            await task.save(using_db=connection, update_fields=["priority", "run_at", "payload", "update_time"])
        else:
            await task.save(update_fields=["priority", "run_at", "payload", "update_time"])
        boosted += 1
    return boosted


async def schedule_watchdog(
    tenant_id: str,
    entity_id: str,
    expected_stage: str,
    delay_seconds: int,
    *,
    reason: Optional[str] = None,
    connection=None,
) -> RsiTask:
    """Schedule a durable anti-leak watchdog timer for a lifecycle stage."""
    timeout_secs = max(1, int(delay_seconds))
    due_at = _utc_now() + timedelta(seconds=timeout_secs)
    timeout_reason = reason or f"watchdog_timeout:{expected_stage}"
    return await schedule_timer(
        tenant_id=tenant_id,
        entity_id=entity_id,
        timer_type="timer_watchdog",
        run_at=due_at,
        payload={
            "expected_stage": expected_stage,
            "reason": timeout_reason,
            "delay_seconds": timeout_secs,
        },
        priority=7,
        replace_existing=True,
        connection=connection,
    )


async def evaluate_watchdog(
    tenant_id: str,
    entity_id: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Check whether entity stalled in expected_stage; emit recovery wakeup if stalled."""
    info = dict(payload or {})
    entity = await resolve_canonical_entity(tenant_id, entity_id)
    if entity is None:
        return {"triggered": False, "skipped": True, "reason": "entity_missing"}

    if entity.status in ("converted", "blacklisted", "closed", "merged"):
        return {"triggered": False, "skipped": True, "reason": "entity_terminal"}

    state = await EntityMemoryState.filter(
        tenant_id=tenant_id, entity_id=entity.entity_id
    ).first()
    expected_stage = info.get("expected_stage")
    current_stage = (state.lifecycle or {}).get("stage") if state and isinstance(state.lifecycle, dict) else None

    if expected_stage and current_stage != expected_stage:
        return {
            "triggered": False,
            "skipped": True,
            "current_stage": current_stage,
            "expected_stage": expected_stage,
        }

    recovery_reason = str(info.get("reason") or f"watchdog_timeout:{expected_stage}")
    recovery_task = await schedule_timer(
        tenant_id=tenant_id,
        entity_id=entity.entity_id,
        timer_type="cognitive_wakeup",
        run_at=_utc_now(),
        payload={
            "trigger_reason": recovery_reason,
            "watchdog_triggered": True,
            "expected_stage": expected_stage,
        },
        priority=8,
        replace_existing=False,
    )
    logger.warning(
        "[RSI-Timers] Watchdog fired recovery wakeup: tenant=%s entity=%s stage=%s task_id=%s",
        tenant_id,
        entity.entity_id,
        expected_stage,
        recovery_task.id,
    )
    return {
        "triggered": True,
        "expected_stage": expected_stage,
        "recovery_task_id": recovery_task.id,
    }
