from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import socket
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

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
    Unified Event Inbox 秒级落盘入口 (/clue/call-end/notify 专用):
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


async def enqueue_channel_inbox(clue_id: str, notice_payload: Dict[str, Any], request_id: Optional[str] = None) -> Dict[str, Any]:
    """Unified Event Inbox 渠道事件入口 (/clue/channel/notify 专用)"""
    from app.dbmodels.models import ExecutionTask

    raw_key = (
        f"channel_inbox:req:{clue_id}:{request_id}"
        if request_id
        else f"channel_inbox:{clue_id}:{uuid.uuid4().hex}"
    )
    task_key = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    try:
        task = await ExecutionTask.create(
            task_key=task_key,
            task_type="channel_inbox",
            request_id=request_id,
            clue_id=clue_id,
            payload=notice_payload,
            status="pending",
            run_at=_utc_now(),
            max_retries=5,
        )
        notify_inbox_worker()
        return {"accepted": True, "duplicate": False, "task_id": task.id}
    except IntegrityError:
        existing = await ExecutionTask.filter(task_key=task_key).first()
        return {"accepted": True, "duplicate": True, "task_id": existing.id if existing else None}


class InboxWakeupWorker:
    """
    Always-On 后台事件收件箱与认知唤醒定时器消费引擎:
    统一消费并按 clue_id 合并窗口期并发事件:
    1. call_end_inbox   (异步挂机总结、自适应分层评估、工具调用与跨业务 Handoff)
    2. agent_wakeup     (Agent 自设定的认知唤醒闹钟)
    3. channel_inbox    (企微/短信渠道到达回执)
    4. approval_resumed (Deferred Tool Call 人工审批结果唤醒)
    5. memory_dream     (后台 LLM 语义级四桶记忆整理)
    """

    def __init__(
        self,
        task_types: Tuple[str, ...] = (
            "call_end_inbox",
            "agent_wakeup",
            "channel_inbox",
            "approval_resumed",
            "memory_dream",
        ),
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

    async def coalesce_pending_inbox_events(
        self,
        clue_id: str,
        current_task_id: int,
    ) -> List[Dict[str, Any]]:
        """
        Per-Clue Event Coalescing:
        批量拉取同线索已到达的 channel_inbox / approval_resumed 待处理事件并合并标记完成，
        让同一次唤醒 Run 能完整感知所有最新事实，消除竞态拆分。
        """
        from app.dbmodels.models import ExecutionTask

        events: List[Dict[str, Any]] = []
        now = _utc_now()
        async with in_transaction() as conn:
            extra_tasks = (
                await ExecutionTask.filter(
                    clue_id=clue_id,
                    task_type__in=["channel_inbox", "approval_resumed"],
                    status__in=["pending", "retry"],
                    run_at__lte=now,
                )
                .exclude(id=current_task_id)
                .using_db(conn)
                .order_by("create_time", "id")
            )
            for et in extra_tasks:
                payload = dict(et.payload or {})
                ev_type = "channel_notify" if et.task_type == "channel_inbox" else et.task_type
                events.append({"type": ev_type, "task_id": et.id, **payload})
                et.status = "succeeded"
                et.finished_at = now
                et.result = {"coalesced_into": current_task_id}
                await et.save(using_db=conn)
        return events

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
                extra_events = await self.coalesce_pending_inbox_events(clue_id, task.id)
                if extra_events:
                    ok = await handler.on_call_end(req, coalesced_events=extra_events)
                else:
                    ok = await handler.on_call_end(req)
                await self._finish_task(
                    task.id, token, succeeded=bool(ok),
                    result_data={"handler_result": bool(ok), "coalesced_events": len(extra_events)},
                )
                return True

            if task.task_type == "agent_wakeup":
                check_goal = (task.payload or {}).get("checkGoal", "")
                extra_events = await self.coalesce_pending_inbox_events(clue_id, task.id)
                if extra_events:
                    ok = await handler.on_wakeup_timer(
                        clue_id,
                        check_goal=check_goal,
                        coalesced_events=extra_events,
                    )
                else:
                    ok = await handler.on_wakeup_timer(clue_id, check_goal=check_goal)
                await self._finish_task(
                    task.id, token, succeeded=bool(ok),
                    result_data={"wakeup_handled": bool(ok), "coalesced_events": len(extra_events)},
                )
                return True

            if task.task_type == "channel_inbox":
                from app.schemas.channel import WechatNoticeRequest
                notice = WechatNoticeRequest.model_validate(task.payload)
                await handler.on_channel_notify(notice)
                await self._finish_task(
                    task.id, token, succeeded=True,
                    result_data={"channel_handled": True},
                )
                return True

            if task.task_type == "approval_resumed":
                payload = dict(task.payload or {})
                extra_events = await self.coalesce_pending_inbox_events(clue_id, task.id)
                all_events = [{"type": "approval_resumed", "task_id": task.id, **payload}, *extra_events]
                ok = await handler.on_wakeup_timer(
                    clue_id,
                    check_goal=f"审批恢复执行: {payload.get('tool_name', '')}",
                    coalesced_events=all_events,
                )
                await self._finish_task(
                    task.id, token, succeeded=bool(ok),
                    result_data={"approval_resumed": bool(ok)},
                )
                return True

            if task.task_type == "memory_dream":
                from app.agents.memory_dreamer import MemoryDreamer
                from app.clue_handler.clue import process_lock
                async with process_lock(clue_id, "memory_dream"):
                    await MemoryDreamer.dream_semantic_curation(clue_id)
                await self._finish_task(
                    task.id, token, succeeded=True,
                    result_data={"dreamed": True},
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
    logger.info("[InboxWorker] 【启动】Unified Event Inbox & Always-On Wakeup Worker 已启动")
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
