"""处理真实通话进度：开始时计数并进入 calling，结束时保存 ended 和接通结果。

先用内部/平台通话标识或批次定位轮次，再更新状态；重复、旧轮次回调不能推进当前线索。
真实回调可确认 processing/unknown 送号任务成功；结束后的 summary 流转由 ClueHandler 完成。
"""
from datetime import datetime, timezone
import json
import logging

from tortoise.expressions import Q
from tortoise.transactions import in_transaction

from app.dbmodels.models import CallRecord, ClueInfo, ExecutionTask
from app.utils.exceptions import StateTransitionError

logger = logging.getLogger(__name__)


def _has_user_turn(raw_context) -> bool:
    """判断通话上下文中是否存在客户实际发言轮次（排除纯机器人开场白/空内容）"""
    if isinstance(raw_context, str):
        try:
            raw_context = json.loads(raw_context)
        except Exception:
            return False
    if not isinstance(raw_context, list):
        return False
    for item in raw_context:
        if not isinstance(item, dict):
            continue
        sender = str(item.get("sender") or item.get("role") or "").strip().lower()
        text = str(item.get("text") or item.get("content") or "").strip()
        if not text or text.startswith("Call the "):
            continue
        if sender not in ("robot", "assistant", "system"):
            return True
    return False


def infer_telephony_facts(current_call, outbound_report=None) -> dict:
    """从 currentCall 与 outboundReport 统一提取 answered / duration_seconds / outcome"""
    raw_duration = getattr(current_call, "aiCallSeconds", None) if current_call else None
    duration_seconds = int(raw_duration) if isinstance(raw_duration, (int, float)) and raw_duration >= 0 else None
    result_code = getattr(current_call, "resultCode", None) if current_call else None
    result_msg = getattr(current_call, "resultMsg", None) if current_call else None

    if outbound_report and getattr(outbound_report, "callAnswered", None) is not None:
        answered = int(bool(outbound_report.callAnswered))
    elif current_call and _has_user_turn(getattr(current_call, "aiCallContextList", None)):
        answered = 1
    elif current_call and ((duration_seconds is not None and duration_seconds > 0) or getattr(current_call, "talkStartTime", None)):
        answered = 1
    elif current_call is not None:
        answered = 0
    else:
        answered = None

    outcome = "answered" if answered == 1 else ("no_answer" if answered == 0 else None)
    return {
        "answered": answered,
        "duration_seconds": duration_seconds,
        "outcome": outcome,
        "provider_result_code": str(result_code)[:64] if result_code is not None else None,
        "provider_result_message": str(result_msg)[:512] if result_msg is not None else None,
    }


async def resolve_record(connection, clue_id, call_id, provider_call_id=None, batch_id=None):
    """只定位受管通话；旧通话返回 None，受管通话标识缺失或冲突则拒绝。"""
    managed = await ExecutionTask.filter(task_type="call", clue_id=clue_id).using_db(connection).exists()
    if not managed:
        return None
    identifier = call_id or provider_call_id
    records = []
    if identifier:
        records = await CallRecord.filter(
            Q(call_id=identifier) | Q(provider_call_id=identifier),
            clue_id=clue_id, call_task_id__gt=0,
        ).using_db(connection).limit(2)
    if len(records) > 1:
        raise StateTransitionError("回调通话标识无法唯一关联呼叫轮次", clue_id=clue_id)
    if batch_id:
        from app.apis.telephony import batch_id_for_call
        candidates = await CallRecord.filter(clue_id=clue_id, call_task_id__gt=0).using_db(connection)
        batch_matches = [record for record in candidates if batch_id_for_call(record.call_id) == batch_id]
        if (len(batch_matches) != 1 or (records and records[0].id != batch_matches[0].id)
                or (call_id and not records)):
            raise StateTransitionError("回调批次与通话标识不一致", clue_id=clue_id)
        return batch_matches[0]
    if not records:
        raise StateTransitionError("回调缺少可唯一关联的内部/平台通话标识或批次", clue_id=clue_id)
    return records[0]


async def bind_provider(record, provider_id, connection):
    """校验并绑定平台 ID；由调用方在当前事务中保存 record。"""
    if not provider_id or provider_id == record.call_id:
        return
    if (not isinstance(provider_id, str) or len(provider_id) > 128
            or (record.provider_call_id and record.provider_call_id != provider_id)
            or await CallRecord.filter(clue_id=record.clue_id, provider_call_id=provider_id)
                .exclude(id=record.id).using_db(connection).exists()):
        raise StateTransitionError("平台通话标识与已有呼叫记录冲突", clue_id=record.clue_id)
    record.provider_call_id = provider_id


async def confirm_submission(record, connection):
    """收到真实通话回调即证明平台已受理；不会重新送号。"""
    await ExecutionTask.filter(id=record.call_task_id, status__in=["processing", "unknown"]).using_db(connection).update(
        status="succeeded", result={"code": "confirmed_by_callback"}, finished_at=datetime.now(timezone.utc),
        lock_token=None, locked_until=None, lock_owner=None)


async def register_call_start(clue_id, call_id, provider_call_id=None, batch_id=None):
    """原子记录开始并计数一次；True=已推进，False=重复/过时，None=旧通话。"""
    async with in_transaction() as connection:
        clue = await ClueInfo.filter(clue_id=clue_id).using_db(connection).select_for_update().first()
        record = await resolve_record(connection, clue_id, call_id, provider_call_id, batch_id)
        if record is None:
            return None  # 旧通话继续走既有处理器。
        if clue is None:
            return False
        if (clue.last_call_id != record.call_id
                or clue.status in ("accepted", "rejected", "cancelled", "completed", "failed")):
            return False
        if record.started_at or record.status in ("ended", "failed", "cancelled"):
            return False
        await bind_provider(record, provider_call_id, connection)
        now = datetime.now(timezone.utc)
        record.started_at, record.status = now, "ringing"
        record.submitted_at = record.submitted_at or now
        await record.save(using_db=connection, update_fields=["started_at", "status", "submitted_at", "provider_call_id", "update_time"])
        clue.total_call_count += 1
        clue.status = "calling"
        clue.state_version += 1
        await clue.save(using_db=connection, update_fields=["total_call_count", "status", "state_version", "update_time"])
        await confirm_submission(record, connection)
        logger.info("[Call-Schedule] 【通话开始】clue_id=%r, call_id=%r", clue_id, call_id)
        return True


async def register_call_end(request):
    """保存该轮结束及接通计数，返回原记录；None 表示继续使用旧通话处理链。"""
    clue_id = request.data.clueId
    provider_id = request.data.currentCall.callId
    async with in_transaction() as connection:
        clue = await ClueInfo.filter(clue_id=clue_id).using_db(connection).select_for_update().first()
        internal_id = request.callId
        # 部分平台可直接把内部callId作为currentCall.callId回传。
        if not internal_id and provider_id and await CallRecord.filter(call_id=provider_id, clue_id=clue_id,
                call_task_id__gt=0).using_db(connection).exists():
            internal_id = provider_id
        record = await resolve_record(connection, clue_id, internal_id, provider_id, request.data.currentCall.batchId)
        if record is None:
            return None
        await bind_provider(record, provider_id, connection)
        now = datetime.now(timezone.utc)
        if record.status != "ended":
            record.status, record.ended_at = "ended", now
            record.submitted_at = record.submitted_at or now
            if record.started_at is None and clue and clue.last_call_id == record.call_id:
                clue.total_call_count += 1  # 无开始通知也不能漏计；结束记录使重复回调不再累加。
            report = request.data.outboundReport
            facts = infer_telephony_facts(request.data.currentCall, report)
            if report and report.callAnswered is not None:
                record.answered = int(bool(report.callAnswered))
            elif record.answered is None and facts["answered"] is not None:
                record.answered = facts["answered"]
            if record.duration_seconds is None and facts["duration_seconds"] is not None:
                record.duration_seconds = facts["duration_seconds"]
            if record.outcome is None and record.answered is not None:
                record.outcome = "answered" if record.answered == 1 else "no_answer"
            if record.provider_result_code is None and facts["provider_result_code"] is not None:
                record.provider_result_code = facts["provider_result_code"]
            if record.provider_result_message is None and facts["provider_result_message"] is not None:
                record.provider_result_message = facts["provider_result_message"]
            if clue and clue.last_call_id == record.call_id:
                if record.answered == 1:
                    clue.no_answer_count = 0
                elif record.answered == 0:
                    clue.no_answer_count += 1
                await clue.save(using_db=connection, update_fields=["total_call_count", "no_answer_count", "update_time"])
        await record.save(using_db=connection)
        await confirm_submission(record, connection)
        logger.info("[Call-Schedule] 【通话结束】clue_id=%r, call_id=%r", clue_id, record.call_id)
        return record
