from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from tortoise.transactions import in_transaction

from app.clue_handler.state import ClueState
from app.dbmodels import crud
from app.dbmodels.models import ClueInfo, ExecutionTask
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def create_deferred_approval_task(
    clue_id: str,
    workflow_id: str,
    tool_name: str,
    tool_args: Dict[str, Any],
    reason: str = "",
) -> ExecutionTask:
    """
    Deferred Tool Calls 原语持久化：
    当 Agent 决定调用卡带 approval_required_tools 中的高危工具时，
    暂停当前 Run，将工具调用请求持久化至 ExecutionTask(task_type='tool_approval')，
    并将线索置为 AWAITING_APPROVAL 等待人工审批后通过 Inbox 唤醒恢复。
    """
    now = _utc_now()
    task_key = hashlib.sha256(
        f"tool_approval:{clue_id}:{tool_name}:{uuid.uuid4().hex}".encode("utf-8")
    ).hexdigest()

    async with in_transaction() as conn:
        task = await ExecutionTask.create(
            using_db=conn,
            task_key=task_key,
            task_type="tool_approval",
            workflow_id=workflow_id,
            clue_id=clue_id,
            payload={
                "tool_name": tool_name,
                "tool_args": tool_args,
                "reason": reason,
                "requested_at": get_now().isoformat(),
            },
            result={"approvalStatus": "pending"},
            status="pending",
            run_at=now,
        )

    try:
        cur_state = await crud.get_clue_state(clue_id)
        if cur_state not in (ClueState.ACCEPTED.value, ClueState.REJECTED.value, ClueState.AWAITING_APPROVAL.value):
            await crud.update_clue_state_or_raise(clue_id, ClueState.AWAITING_APPROVAL.value)
    except Exception as err:
        logger.warning(f"[Approvals] 线索 {clue_id} 流转至 AWAITING_APPROVAL 跳过: {err}")

    logger.info(
        f"[Approvals] 【挂起审批】clue_id={clue_id}, task_id={task.id}, tool={tool_name}"
    )
    return task


async def list_pending_approvals(
    clue_id: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """查询待审批的 Deferred Tool Call 列表"""
    query = ExecutionTask.filter(task_type="tool_approval", status="pending")
    if clue_id:
        query = query.filter(clue_id=clue_id)
    tasks = await query.order_by("create_time").limit(limit)
    return [
        {
            "task_id": t.id,
            "clue_id": t.clue_id,
            "workflow_id": t.workflow_id,
            "tool_name": (t.payload or {}).get("tool_name"),
            "tool_args": (t.payload or {}).get("tool_args", {}),
            "reason": (t.payload or {}).get("reason", ""),
            "status": t.status,
            "create_time": t.create_time.isoformat() if t.create_time else None,
        }
        for t in tasks
    ]


async def resolve_deferred_approval(
    task_id: int,
    *,
    approved: bool,
    reviewer: str = "operator",
    comment: str = "",
) -> Dict[str, Any]:
    """
    人工审批结果落库并推入 Unified Event Inbox 唤醒线索继续执行。
    """
    now = _utc_now()
    async with in_transaction() as conn:
        approval_task = (
            await ExecutionTask.filter(id=task_id, task_type="tool_approval")
            .using_db(conn)
            .select_for_update()
            .first()
        )
        if not approval_task:
            raise ValueError(f"tool_approval task {task_id} not found")
        if approval_task.status != "pending":
            return {
                "task_id": approval_task.id,
                "clue_id": approval_task.clue_id,
                "already_resolved": True,
                "status": approval_task.status,
                "result": approval_task.result,
            }

        approval_task.status = "succeeded" if approved else "cancelled"
        approval_task.finished_at = now
        approval_task.result = {
            "approvalStatus": "approved" if approved else "rejected",
            "approved": bool(approved),
            "reviewer": reviewer,
            "comment": comment,
            "resolvedAt": get_now().isoformat(),
        }
        await approval_task.save(using_db=conn)

        payload = dict(approval_task.payload or {})
        inbox_key = hashlib.sha256(
            f"approval_resumed:{approval_task.id}:{uuid.uuid4().hex}".encode("utf-8")
        ).hexdigest()
        resume_task = await ExecutionTask.create(
            using_db=conn,
            task_key=inbox_key,
            task_type="approval_resumed",
            workflow_id=approval_task.workflow_id,
            clue_id=approval_task.clue_id,
            payload={
                "approvalTaskId": approval_task.id,
                "approved": bool(approved),
                "reviewer": reviewer,
                "comment": comment,
                "tool_name": payload.get("tool_name"),
                "tool_args": payload.get("tool_args", {}),
            },
            status="pending",
            run_at=now,
        )

    from app.agents.inbox_worker import notify_inbox_worker
    notify_inbox_worker()

    logger.info(
        f"[Approvals] 【审批完成并唤醒】approval_task_id={task_id}, clue_id={approval_task.clue_id}, "
        f"approved={approved}, resume_inbox_task_id={resume_task.id}"
    )
    return {
        "task_id": approval_task.id,
        "clue_id": approval_task.clue_id,
        "approved": bool(approved),
        "resume_inbox_task_id": resume_task.id,
        "status": approval_task.status,
    }
