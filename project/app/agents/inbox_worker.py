from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import socket
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from tortoise.exceptions import IntegrityError
from tortoise.expressions import Q
from tortoise.transactions import in_transaction

from app.schemas.call_end import CallEndRequest
from app.utils.exceptions import LockTimeoutError, StateTransitionError

logger = logging.getLogger(__name__)

_WAKEUP_SIGNAL = asyncio.Event()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def notify_inbox_worker() -> None:
    """唤醒后台 Inbox Worker 立即消费新事件"""
    try:
        _WAKEUP_SIGNAL.set()
    except Exception:
        pass


async def enqueue_call_end_inbox(request: CallEndRequest) -> Dict[str, Any]:
    """
    Event Inbox 秒级落盘入口 (/clue/call-end/notify 专用):
    仅执行幂等键计算与 ExecutionTask 持久化落库，不跑 LLM、不抢长锁，毫秒级返回 ACK。
    """
    from app.dbmodels.models import ExecutionTask

    clue_id = request.data.clueId
    current_call = getattr(request.data, "currentCall", None)
    call_id = getattr(current_call, "callId", None) or request.callId or ""
    batch_id = getattr(current_call, "batchId", None) or ""

    if request.requestId:
        raw_key = f"call_end_inbox:req:{clue_id}:{request.requestId}"
    else:
        raw_key = f"call_end_inbox:call:{clue_id}:{call_id}:{batch_id}"

    task_key = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    now = _utc_now()
    payload = request.model_dump(mode="json")

    try:
        task = await ExecutionTask.create(
            task_key=task_key,
            task_type="call_end_inbox",
            request_id=request.requestId,
            clue_id=clue_id,
            call_id=call_id or None,
            payload=payload,
            status="pending",
            run_at=now,
            max_retries=5,
        )
        # 顺带记录到 Redis 事件合并缓冲并触发后台瞬间唤醒
        try:
            from app.clue_handler.clue import ClueHandler
            await ClueHandler.enqueue_inbox_event_static(
                clue_id, {"type": "call_end", "task_id": task.id, "call_id": call_id}
            )
        except Exception:
            pass
        notify_inbox_worker()
        logger.info(
            f"[InboxWorker] 【秒回入箱】call_end 已入 Event Inbox: clue_id={clue_id}, task_id={task.id}"
        )
        return {"accepted": True, "duplicate": False, "task_id": task.id}
    except IntegrityError:
        existing = await ExecutionTask.filter(task_key=task_key).first()
        logger.info(
            f"[InboxWorker] 【幂等拦截】重复 call_end 入箱跳过: clue_id={clue_id}, task_key={task_key[:12]}"
        )
        return {
            "accepted": True,
            "duplicate": True,
            "task_id": existing.id if existing else None,
        }


class InboxWakeupWorker:
    """
    Always-On 后台事件收件箱与认知唤醒定时器消费引擎:
    统一消费:
    1. call_end_inbox (异步挂机总结、多专家评估、策略排期与跨业务 Handoff)
    2. agent_wakeup   (Agent 自设定的认知唤醒闹钟)
    """

    def __init__(
        self,
        task_types: Tuple[str, ...] = ("call_end_inbox", "agent_wakeup"),
        lease_seconds: int = 60,
    ) -> None:
        self.task_types = task_types
        self.lease_seconds = lease_seconds
        self.owner_id = f"{socket.gethostname()}-{os.getpid()}"

    async def claim_next_task(self) -> Optional[Any]:
        from app.dbmodels.models import ExecutionTask

        now = _utc_now()
        lease_until = now + timedelta(seconds=self.lease_seconds)
        token = uuid.uuid4().hex

        async with in_transaction() as conn:
            eligible = (
                Q(status__in=["pending", "retry"], run_at__lte=now)
                | Q(status="processing", locked_until__lt=now)
            )
            task = (
                await ExecutionTask.filter(eligible, task_type__in=list(self.task_types))
                .using_db(conn)
                .order_by("run_at", "id")
                .first()
            )
            if not task:
                return None

            updated = (
                await ExecutionTask.filter(
                    eligible,
                    id=task.id,
                    task_type__in=list(self.task_types),
                )
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

            return await ExecutionTask.filter(id=task.id).using_db(conn).first()

    async def _finish_task(
        self,
        task_id: int,
        lock_token: Optional[str],
        *,
        succeeded: bool,
        error: Optional[Exception] = None,
        result_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        from app.dbmodels.models import ExecutionTask

        now = _utc_now()
        async with in_transaction() as conn:
            task = await ExecutionTask.filter(id=task_id).using_db(conn).first()
            if not task or task.lock_token != lock_token:
                return

            if succeeded:
                task.status = "succeeded"
                task.finished_at = now
                task.last_error = None
                task.result = result_data or {"ok": True}
            else:
                max_retries = task.max_retries if task.max_retries is not None else 5
                if not isinstance(error, StateTransitionError) and task.retry_count < max_retries:
                    task.retry_count += 1
                    backoff = min(60, 2 ** task.retry_count)
                    task.status = "retry"
                    task.run_at = now + timedelta(seconds=backoff)
                else:
                    task.status = "failed"
                    task.finished_at = now
                task.last_error = f"{type(error).__name__}: {str(error)[:500]}" if error else "unknown_error"

            task.lock_owner = None
            task.lock_token = None
            task.locked_until = None
            await task.save(using_db=conn)

    async def process_task(self, task: Any) -> bool:
        from app.clue_handler.factory import ClueHandlerFactory

        clue_id = task.clue_id
        token = task.lock_token
        try:
            handler = await ClueHandlerFactory.get_handler_by_clue(clue_id)

            if task.task_type == "call_end_inbox":
                req = CallEndRequest.model_validate(task.payload)
                ok = await handler.on_call_end(req)
                await self._finish_task(
                    task.id, token, succeeded=bool(ok),
                    result_data={"handler_result": bool(ok)},
                )
                return True

            if task.task_type == "agent_wakeup":
                check_goal = (task.payload or {}).get("checkGoal", "")
                ok = await handler.on_wakeup_timer(clue_id, check_goal=check_goal)
                await self._finish_task(
                    task.id, token, succeeded=bool(ok),
                    result_data={"wakeup_handled": bool(ok)},
                )
                return True

            await self._finish_task(task.id, token, succeeded=True, result_data={"ignored": task.task_type})
            return True

        except LockTimeoutError as lock_err:
            logger.warning(f"[InboxWorker] 锁等待超时，稍后退避重试: task_id={task.id}, clue_id={clue_id}")
            await self._finish_task(task.id, token, succeeded=False, error=lock_err)
            return False
        except StateTransitionError as st_err:
            logger.warning(f"[InboxWorker] 状态流转拦截，结束任务: task_id={task.id}, err={st_err}")
            await self._finish_task(task.id, token, succeeded=True, result_data={"skipped": str(st_err)})
            return True
        except Exception as err:
            logger.error(f"[InboxWorker] 后台唤醒处理失败: task_id={task.id}, clue_id={clue_id}, err={err}", exc_info=True)
            await self._finish_task(task.id, token, succeeded=False, error=err)
            return False

    async def run_once(self) -> bool:
        task = await self.claim_next_task()
        if not task:
            return False
        await self.process_task(task)
        return True


async def inbox_wakeup_loop() -> None:
    """后台常驻消费循环：平时休眠零开销，收到信号或到期立即唤醒执行"""
    worker = InboxWakeupWorker()
    logger.info("[InboxWorker] 【启动】Event Inbox & Always-On Wakeup Worker 已启动")
    while True:
        try:
            processed_any = False
            while await worker.run_once():
                processed_any = True

            if not processed_any:
                _WAKEUP_SIGNAL.clear()
                try:
                    await asyncio.wait_for(_WAKEUP_SIGNAL.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            logger.info("[InboxWorker] 【停止】Event Inbox Worker 已停止")
            raise
        except Exception as err:
            logger.error(f"[InboxWorker] 主循环异常，5s后恢复: {err}", exc_info=True)
            await asyncio.sleep(5.0)
