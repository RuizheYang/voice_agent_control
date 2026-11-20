import logging
import json
import asyncio
import hashlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Literal, Optional

from tortoise.expressions import F
from tortoise.exceptions import IntegrityError
from tortoise.transactions import in_transaction

from app.redis_client.redis_manager import redis_manager
from app.schemas.call_end import CallEndRequest, CallEndOutboundReport
from app.schemas.clue_create import NormalizedClue
from app.utils.exceptions import LockTimeoutError, StateTransitionError, WorkflowAgentPairingError

import uuid
from pydantic import BaseModel, Field

from .models import (
    VoiceAgent, OutboundStrategy, WorkflowBinding, WorkflowDomainConfig,
    ClueInfo, CallRecord, ExecutionTask, PauseRecord, AppAuth,
    CallRecordPydantic,
)
class ClueEventRecord(BaseModel):
    clue_id: str
    event_type: str
    event_time: Optional[str] = None
    event_data: Dict[str, Any] = Field(default_factory=dict)
    task_type: Optional[str] = None
    workflow_id: Optional[str] = None

ClueEventModel = ClueEventRecord
from app.observability.events_bus import event_bus, StateTransferEvent
from app.utils.time_utils import get_now, ensure_tz_aware
from app.utils.decrypt import crypted_manager, IT_VERSION_LABEL

logger = logging.getLogger(__name__)


# ==============================================================================
# 1. 线索基础创建与查询 (基于 MySQL ClueInfo)
# ==============================================================================
async def create_clue(clue: NormalizedClue) -> ClueInfo:
    """同一事务保存调度线索与外呼服务兼容线索，提交后才允许发布队列。"""
    phone_number = clue.phoneNumber
    if phone_number and len(phone_number) < 20:
        try:
            phone_number = crypted_manager.encrypt_field(phone_number, IT_VERSION_LABEL)
        except Exception as e:
            logger.warning(f"加密手机号失败: {e}")

    customer_name = clue.customerName
    if customer_name and len(customer_name) < 20:
        try:
            customer_name = crypted_manager.encrypt_field(customer_name, IT_VERSION_LABEL)
        except Exception as e:
            logger.warning(f"加密客户姓名失败: {e}")

    agent_data = dict(clue.agentData) if getattr(clue, "agentData", None) else {}
    if getattr(clue, "customerCity", None):
        agent_data["customerCity"] = clue.customerCity
    if getattr(clue, "customerSex", None):
        agent_data["customerSex"] = clue.customerSex
    if getattr(clue, "orderTime", None):
        agent_data["orderTime"] = clue.orderTime
    if getattr(clue, "clueSource", None):
        agent_data.setdefault("clueSource", clue.clueSource)
    if getattr(clue, "requestId", None):
        agent_data["requestId"] = clue.requestId

    workflow_instance_id = f"wi_{uuid.uuid4().hex}"
    received_at = datetime.now(timezone.utc)

    # 优先根据 strategyId 获取快照配置
    clue_config = {}
    strategy = await OutboundStrategy.filter(strategy_id=clue.strategyId).first()
    if strategy:
        from app.clue_inflow.service import normalize_config

        clue_config = {
            "strategyId": strategy.strategy_id,
            "scheduleId": strategy.schedule_id,
            "followup": normalize_config(strategy.followup),
            "sip": normalize_config(strategy.sip),
        }

    followup = strategy.followup if strategy and isinstance(strategy.followup, dict) else {}
    max_followup_hours = followup.get("max_followup_hours", followup.get("maxFollowupHours", 72))
    if type(max_followup_hours) is not int or max_followup_hours <= 0:
        raise ValueError("max_followup_hours must be a positive integer")

    async with in_transaction() as connection:
        clue_record, created = await ClueInfo.get_or_create(
            using_db=connection,
            clue_id=clue.clueId,
            defaults={
                "workflow_instance_id": workflow_instance_id,
                "source_app_id": clue.requestId or "",
                "external_task_id": clue.externalTaskId,
                "workflow_id": clue.workflowId or "default",
                "agent_id": clue.agentId or "default",
                "strategy_id": clue.strategyId or "default",
                "import_task_id": 0,
                "phone_number": phone_number,
                "customer_name": customer_name,
                "agent_data": agent_data,
                "overrides": clue.overrides or None,
                "clue_config": clue_config,
                "status": "created",
                "state_version": 0,
                "received_at": received_at,
                "expires_at": received_at + timedelta(hours=max_followup_hours),
            }
        )

        if not created:
            clue_record.workflow_id = clue.workflowId or "default"
            clue_record.agent_id = clue.agentId or "default"
            clue_record.strategy_id = clue.strategyId or "default"
            clue_record.agent_data = agent_data
            if clue_config:
                clue_record.clue_config = clue_config
            await clue_record.save(using_db=connection)
        else:
            # 事务发件箱兜底：同一事务写入 new_clue 恢复任务，确保即使 Redis push 失败也可自动入池
            entry_key = hashlib.sha256(
                json.dumps(["new_clue", clue_record.workflow_instance_id]).encode("utf-8")
            ).hexdigest()
            try:
                await ExecutionTask.create(
                    using_db=connection,
                    task_key=entry_key,
                    task_type="new_clue",
                    clue_id=clue_record.clue_id,
                    app_id=clue_record.source_app_id,
                    request_id=clue.requestId,
                    workflow_id=clue_record.workflow_id,
                    strategy_id=clue_record.strategy_id,
                    payload={"importTaskId": 0},
                    result={"entryStatus": "pending"},
                    status="pending",
                    run_at=received_at,
                )
            except IntegrityError:
                pass

    return clue_record


async def get_clue_info(clue_id: str) -> Optional[ClueInfo]:
    """根据 clue_id 查询线索实体"""
    return await ClueInfo.filter(clue_id=clue_id).first()


async def get_clue_intake_time(clueId: str) -> datetime:
    """获取线索接收时间 (替代原 Mongo 查询)"""
    clue = await ClueInfo.filter(clue_id=clueId).first()
    if not clue:
        raise ValueError(f"Clue with clueId {clueId} not found")
    if clue.received_at:
        received_at = clue.received_at
        if clue.import_task_id and received_at.tzinfo is None:
            received_at = received_at.replace(tzinfo=timezone.utc)
        return ensure_tz_aware(received_at)
    return ensure_tz_aware(get_now())


async def get_clue_meta(clueId: str) -> dict[str, Any]:
    """获取线索详情并自动解密敏感数据"""
    clue = await ClueInfo.filter(clue_id=clueId).first()
    if not clue:
        raise ValueError(f"Clue with clueId {clueId} not found")

    phone_num = clue.phone_number
    if phone_num and len(phone_num) > 20:
        try:
            phone_num = crypted_manager.decrypt_field(phone_num)
        except Exception as e:
            logger.warning(f"解密手机号失败: {e}")

    cust_name = clue.customer_name
    if cust_name and len(cust_name) > 20:
        try:
            cust_name = crypted_manager.decrypt_field(cust_name)
        except Exception as e:
            logger.warning(f"解密客户姓名失败: {e}")

    return {
        "clueId": clue.clue_id,
        "workflowId": clue.workflow_id,
        "agentId": clue.agent_id,
        "strategyId": clue.strategy_id,
        "customerName": cust_name,
        "customerPhoneNum": phone_num,
        "agentData": clue.agent_data,
        "overrides": clue.overrides,
        "status": clue.status,
    }


async def prepare_call_params(clueId: str) -> dict[str, Any]:
    """为外呼准备参数"""
    meta = await get_clue_meta(clueId)
    agent_data = meta.get("agentData") or {}
    return {
        "clueId": meta["clueId"],
        "customerPhoneNum": meta["customerPhoneNum"],
        "customerName": meta["customerName"],
        "customerSex": agent_data.get("customerSex", "未知"),
        "customerCity": agent_data.get("customerCity"),
        "clueSource": agent_data.get("clueSource", "default"),
        "orderTime": agent_data.get("orderTime"),
        "agentId": meta.get("agentId"),
        "strategyId": meta.get("strategyId"),
        "agentData": agent_data,
        "overrides": meta.get("overrides") or {},
    }


async def update_clue_next_call_work_phone(clue_id: str, use_work_phone: bool) -> bool:
    """更新线索下一次拨打是否使用工作机"""
    clue = await ClueInfo.filter(clue_id=clue_id).first()
    if not clue:
        return False
    agent_data = dict(clue.agent_data) if clue.agent_data else {}
    agent_data["next_call_use_work_phone"] = bool(use_work_phone)
    clue.agent_data = agent_data
    # 排期可能在读取后并发提交；线路标记不能覆盖 status/last_call_id/state_version。
    await clue.save(update_fields=["agent_data", "update_time"])
    return True


# ==============================================================================
# 2. 状态机流转与并发控制 (ClueInfo.status + stateVersion CAS 乐观锁)
# ==============================================================================
async def update_state_no_lock(clueId: str, state: str) -> bool:
    """更新线索状态 (无锁快速更新)"""
    clue = await ClueInfo.filter(clue_id=clueId).first()
    if not clue:
        logger.warning(f"更新状态失败: 找不到线索 {clueId}")
        return False

    if clue.status == state:
        return True

    clue.status = state
    clue.state_version = F("state_version") + 1
    await clue.save()
    return True


async def update_clue_state_with_version(
    clue_id: str,
    new_state: str,
    expected_version: Optional[int] = None,
    close_reason: Optional[str] = None
) -> bool:
    """CAS 乐观锁原子更新状态"""
    query = ClueInfo.filter(clue_id=clue_id)
    if expected_version is not None:
        query = query.filter(state_version=expected_version)

    update_fields = {
        "status": new_state,
        "state_version": F("state_version") + 1
    }
    if close_reason:
        update_fields["close_reason"] = close_reason

    updated_rows = await query.update(**update_fields)
    return updated_rows > 0


async def update_clue_state(
    clueId: str,
    state: str,
    max_retries: int = 3,
    retry_delay: float = 0.5
) -> bool:
    """带状态机合法性校验与 Redis 分布式排队锁的状态更新"""
    lock = redis_manager.get_lock(clueId)

    for attempt in range(1, max_retries + 1):
        try:
            async with lock:
                clue = await ClueInfo.filter(clue_id=clueId).first()
                if not clue:
                    logger.warning(f"更新状态失败: 找不到线索 {clueId}")
                    raise StateTransitionError(f"Clue {clueId} does not exist in clueInfo.", clue_id=clueId)

                current_state = clue.status

                if current_state == state:
                    await event_bus.emit(StateTransferEvent(
                        clue_id=clueId,
                        event_data={'from': current_state, 'to': state, 'reason': 'identical_transition', 'success': False}
                    ))
                    return True

                from app.clue_handler.state import VALID_TRANSITIONS
                if state not in VALID_TRANSITIONS.get(current_state, []):
                    await event_bus.emit(StateTransferEvent(
                        clue_id=clueId,
                        event_data={'from': current_state, 'to': state, 'reason': 'invalid_transition', 'success': False}
                    ))
                    logger.warning(f"状态流转拒绝: 线索 {clueId} 试图从 {current_state} 转为 {state} 但不被允许。")
                    raise StateTransitionError(f"Rejected transition from {current_state} to {state} for clue {clueId}", clue_id=clueId)

                clue.status = state
                clue.state_version = clue.state_version + 1
                await clue.save()

                await event_bus.emit(StateTransferEvent(
                    clue_id=clueId,
                    event_data={'from': current_state, 'to': state, 'reason': 'success', 'success': True}
                ))
                return True
        except TimeoutError:
            await event_bus.emit(StateTransferEvent(
                clue_id=clueId,
                event_data={'from': None, 'to': state, 'reason': 'lock_timeout', 'success': False}
            ))
            if attempt < max_retries:
                logger.info(f"获取线索 {clueId} 的分布式锁超时，准备第 {attempt} 次重试...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"更新状态失败: 获取线索 {clueId} 的分布式锁超时，在 {max_retries} 次重试后彻底失败")
                raise LockTimeoutError(f"Lock timeout for clue {clueId} after {max_retries} attempts.", clue_id=clueId)


async def update_clue_state_or_raise(
    clueId: str,
    state: str,
    max_retries: int = 3,
    retry_delay: float = 0.5
):
    """强制更新线索状态，失败抛出异常"""
    success = await update_clue_state(clueId, state, max_retries, retry_delay)
    if not success:
        raise Exception(f"严重错误: 无法将线索 {clueId} 状态强行更新为 {state}")


async def get_clue_state(clueId: str) -> str:
    """获取线索状态"""
    clue = await ClueInfo.filter(clue_id=clueId).first()
    return clue.status if clue else "unknown"


async def get_clue_handler_name(clueId: str) -> str:
    return "default"


async def get_clue_state_record(clueId: str):
    """查询线索当前状态机记录 (字典形式)"""
    clue = await ClueInfo.filter(clue_id=clueId).first()
    if not clue:
        return None
    return dict(clue)


async def save_close_reason(clueId: str, closeReason: str):
    """记录线索关闭原因"""
    await ClueInfo.filter(clue_id=clueId).update(close_reason=closeReason)


async def get_close_reason(clueId: str) -> Optional[str]:
    clue = await ClueInfo.filter(clue_id=clueId).first()
    return clue.close_reason if clue else None


async def increment_total_call_count(clueId: str) -> bool:
    """原子累加呼叫次数"""
    try:
        updated = await ClueInfo.filter(clue_id=clueId).update(
            total_call_count=F('total_call_count') + 1
        )
        return updated > 0
    except Exception as e:
        logger.error(f"增加线索 {clueId} 外呼次数异常: {e}")
        return False


async def get_total_call_count(clueId: str) -> int:
    """获取外呼总次数"""
    clue = await ClueInfo.filter(clue_id=clueId).first()
    return clue.total_call_count if clue else 0


# ==============================================================================
# 3. 通话记录管理 (基于 MySQL CallRecord)
# ==============================================================================
async def create_call_record(
    clue_id: str,
    call_id: str,
    agent_id: str = "default",
    strategy_id: str = "default",
    attempt_no: int = 1,
    status: str = "created",
    **kwargs
) -> CallRecord:
    """创建外呼通话记录"""
    record, _ = await CallRecord.get_or_create(
        call_id=call_id,
        defaults={
            "clue_id": clue_id,
            "agent_id": agent_id,
            "strategy_id": strategy_id,
            "attempt_no": attempt_no,
            "status": status,
            "scheduled_at": kwargs.get("scheduled_at", get_now()),
            "config_snapshot": kwargs.get("config_snapshot", {}),
        }
    )
    return record


async def create_call_end_record(call_end_request: CallEndRequest) -> CallRecord:
    """通话结束后落库 CallRecord"""
    from app.call_schedule.callbacks import infer_telephony_facts, register_call_end
    managed_record = await register_call_end(call_end_request)
    data = call_end_request.data
    current_call = getattr(data, "currentCall", None)
    if managed_record is not None:
        if current_call and getattr(current_call, "aiCallContextList", None):
            snap = dict(managed_record.config_snapshot or {})
            snap["aiCallContextList"] = current_call.aiCallContextList
            managed_record.config_snapshot = snap
            await managed_record.save()
        return managed_record
    outbound_report = getattr(data, "outboundReport", None)
    facts = infer_telephony_facts(current_call, outbound_report)

    call_id = getattr(current_call, "callId", None) if current_call else None
    if not call_id:
        call_id = f"call_{data.clueId}_{int(datetime.now().timestamp())}"

    clue = await ClueInfo.filter(clue_id=data.clueId).first()
    agent_id = clue.agent_id if clue else "default"
    strategy_id = clue.strategy_id if clue else "default"
    attempt_no = clue.total_call_count if clue and clue.total_call_count > 0 else 1

    answered_flag = facts["answered"]

    snapshot = dict(clue.clue_config or {}) if clue else {}
    if current_call and getattr(current_call, "aiCallContextList", None):
        snapshot["aiCallContextList"] = current_call.aiCallContextList

    record, created = await CallRecord.get_or_create(
        call_id=call_id,
        defaults={
            "clue_id": data.clueId,
            "agent_id": agent_id,
            "strategy_id": strategy_id,
            "attempt_no": attempt_no,
            "status": "ended",
            "answered": answered_flag,
            "outcome": facts["outcome"],
            "duration_seconds": facts["duration_seconds"],
            "provider_result_code": facts["provider_result_code"],
            "provider_result_message": facts["provider_result_message"],
            "ended_at": get_now(),
            "scheduled_at": get_now(),
            "config_snapshot": snapshot,
        }
    )

    if not created and current_call and getattr(current_call, "aiCallContextList", None):
        existing_snap = dict(record.config_snapshot or {})
        existing_snap["aiCallContextList"] = current_call.aiCallContextList
        record.config_snapshot = existing_snap
        await record.save()

    dirty_fields = []
    if answered_flag is not None and (record.answered is None or (outbound_report and outbound_report.callAnswered is not None)):
        record.answered = answered_flag
        dirty_fields.append("answered")
    if record.outcome is None and facts["outcome"] is not None:
        record.outcome = facts["outcome"]
        dirty_fields.append("outcome")
    if record.duration_seconds is None and facts["duration_seconds"] is not None:
        record.duration_seconds = facts["duration_seconds"]
        dirty_fields.append("duration_seconds")
    if dirty_fields:
        record.status = "ended"
        dirty_fields.append("status")
        await record.save()

    return record

create_call_end_record_mysql = create_call_end_record


async def update_end_call_info_outbound_report(
    clue_id: str,
    outbound_report: CallEndOutboundReport,
    call_id: Optional[str] = None
):
    """更新通话报告 (CallRecord)"""
    query = CallRecord.filter(clue_id=clue_id)
    if call_id:
        query = query.filter(call_id=call_id)
    record = await query.order_by("-create_time").first()
    if record:
        if outbound_report.callAnswered is not None:
            record.answered = 1 if bool(outbound_report.callAnswered) else 0
        if outbound_report.recordFile:
            record.recording_url = outbound_report.recordFile
        await record.save()


async def get_call_history(clue_id: str) -> List[Any]:
    """查询线索的历史通话记录"""
    # 排期时即会建立created记录，策略历史只应读取已经结束的通话。
    call_history = await CallRecord.filter(clue_id=clue_id, status="ended").order_by('-create_time')
    return [CallRecordPydantic.model_validate(record) for record in call_history]


async def has_end_call_code(clue_id: str, one_level_code: int, two_level_code: int) -> bool:
    """判断历史上是否出现过指定的一级/二级结果码"""
    record = await CallRecord.filter(
        clue_id=clue_id,
        decision_result__contains={"oneLevelCode": one_level_code, "twoLevelCode": two_level_code}
    ).first()
    return record is not None


async def get_clue_history_stats(clue_id: str, call_end_time: Optional[datetime] = None) -> dict:
    """获取外呼统计指标 (呼叫总数、连续未接通次数等)"""
    if call_end_time is None:
        call_end_time = get_now()

    clue = await ClueInfo.filter(clue_id=clue_id).first()
    if not clue:
        raise ValueError(f"线索 {clue_id} 不存在")

    clue_in_time = ensure_tz_aware(clue.received_at or get_now())
    call_history = await CallRecord.filter(clue_id=clue_id, status="ended").order_by('-create_time')
    total_call_count = len(call_history)

    if total_call_count == 0:
        return {
            "total_call_count": 0,
            "continuous_no_answer_count": 0,
            "continuous_no_answer_hours": 0,
            "clue_in_time": clue_in_time
        }

    continuous_no_answer_count = 0
    latest_connected_time: Optional[datetime] = None

    for call in call_history:
        if call.answered == 0:
            continuous_no_answer_count += 1
        else:
            latest_connected_time = call.create_time
            break

    continuous_no_answer_hours = 0
    start_calc_time = latest_connected_time or clue_in_time
    if start_calc_time:
        call_end_time_no_tz = call_end_time.replace(tzinfo=None) if call_end_time.tzinfo else call_end_time
        start_calc_time_no_tz = start_calc_time.replace(tzinfo=None) if start_calc_time.tzinfo else start_calc_time
        delta = call_end_time_no_tz - start_calc_time_no_tz
        if delta.total_seconds() > 0:
            continuous_no_answer_hours = int(delta.total_seconds() // 3600)

    return {
        "total_call_count": total_call_count,
        "continuous_no_answer_count": continuous_no_answer_count,
        "continuous_no_answer_hours": continuous_no_answer_hours,
        "clue_in_time": clue_in_time
    }


async def save_call_summary_result(call_id: str, summary_result: BaseModel | dict) -> CallRecord:
    """保存一通明确通话的结构化分析结论（不修改线索状态或排期）。"""
    payload = summary_result.model_dump(mode="json") if isinstance(summary_result, BaseModel) else dict(summary_result)
    async with in_transaction() as connection:
        record = await CallRecord.filter(call_id=call_id).using_db(connection).select_for_update().get()
        details = dict(record.decision_result or {})
        details["summary"] = payload
        record.decision_result = details
        record.common_call_events = payload.get("common_call_events") or payload
        record.summary_status = payload.get("analysis_status", "succeeded")
        await record.save(using_db=connection, update_fields=[
            "decision_result", "common_call_events", "summary_status", "update_time",
        ])
        return record


async def save_call_summary_record(
    clue_id: str,
    request_id: str,
    summary_result: dict,
    business_type: str = "general"
):
    """保存通话大模型分析结果至 CallRecord"""
    latest_call = await CallRecord.filter(clue_id=clue_id).order_by("-create_time").first()
    if latest_call:
        latest_call.common_call_events = summary_result
        latest_call.summary_status = "succeeded"
        await latest_call.save()


async def update_end_call_record_with_summary(
    clue_id: str,
    one_level_code: Optional[int],
    two_level_code: Optional[int],
    user_qualified: Optional[bool],
    wechat_requested: Optional[bool],
    wechat_accepted: Optional[bool],
    call_end_request: Optional[CallEndRequest] = None
):
    """更新通话总结信息至 CallRecord"""
    last_record = await CallRecord.filter(clue_id=clue_id).order_by("-create_time").first()
    if not last_record:
        if call_end_request:
            last_record = await create_call_end_record(call_end_request)
        else:
            last_record = await CallRecord.create(
                call_id=f"call_{clue_id}_{int(datetime.now().timestamp())}",
                clue_id=clue_id,
                agent_id="default",
                strategy_id="default",
                attempt_no=1,
                status="ended",
            )

    decision_dict = dict(last_record.decision_result or {})
    if one_level_code is not None:
        decision_dict["oneLevelCode"] = one_level_code
    if two_level_code is not None:
        decision_dict["twoLevelCode"] = two_level_code
    if user_qualified is not None:
        decision_dict["userQualified"] = user_qualified
    if wechat_requested is not None:
        decision_dict["wechatRequested"] = wechat_requested
    if wechat_accepted is not None:
        decision_dict["wechatAccepted"] = wechat_accepted

    last_record.decision_result = decision_dict
    last_record.summary_status = "succeeded"
    await last_record.save()
    return last_record


# ==============================================================================
# 4. 事件溯源与发件箱任务 (基于 MySQL ExecutionTask)
# ==============================================================================
async def save_clue_event(
    clueId: str,
    eventType: str,
    eventData: Any,
    eventTime: Optional[str] = None,
    taskType: str = "internal_event",
    workflowId: Optional[str] = None,
    eventId: Optional[str] = None,
    status: str = "succeeded",
    retryCount: int = 0,
    lastError: Optional[str] = None,
) -> Optional[ExecutionTask]:
    """保存线索事件至 ExecutionTask (发件箱与事件流)"""
    try:
        # 若未直接提供 workflowId，尝试从 ClueInfo 关联补全
        if not workflowId and clueId:
            clue = await ClueInfo.filter(clue_id=clueId).first()
            if clue:
                workflowId = clue.workflow_id

        if eventId:
            task_key = hashlib.sha256(f"{taskType}_{clueId}_{eventType}_{eventId}".encode()).hexdigest()
        else:
            task_key = hashlib.sha256(f"{taskType}_{clueId}_{eventType}_{datetime.now().timestamp()}_{uuid.uuid4()}".encode()).hexdigest()

        return await ExecutionTask.create(
            task_key=task_key,
            task_type=taskType,
            workflow_id=workflowId,
            clue_id=clueId,
            event_id=eventId,
            event_type=eventType,
            payload=eventData if isinstance(eventData, dict) else {"data": eventData},
            status=status,
            retry_count=retryCount,
            last_error=lastError,
            run_at=get_now(),
            finished_at=get_now() if status in ["succeeded", "failed"] else None,
        )
    except Exception as ex:
        logger.info(f"保存事件失败: clueId={clueId}, eventType={eventType}, error={ex}")
        return None


async def get_clue_events(clueId: str) -> List[ClueEventModel]:
    """查询线索的历史事件"""
    tasks = await ExecutionTask.filter(
        clue_id=clueId,
        task_type__in=["webhook", "internal_event", "event"]
    ).order_by("create_time")
    return [
        ClueEventRecord(
            clue_id=t.clue_id or clueId,
            event_type=t.event_type or "unknown",
            event_time=t.create_time.strftime("%Y-%m-%d %H:%M:%S") if t.create_time else None,
            event_data=t.payload or {},
            task_type=t.task_type,
            workflow_id=t.workflow_id,
        )
        for t in tasks
    ]


async def list_clue_events_timeline(clueId: str) -> List[dict]:
    """查询事件时间线列表"""
    events = await get_clue_events(clueId)
    return [e.model_dump() for e in events]


# ==============================================================================
# 5. 回调幂等控制 (基于 ExecutionTask)
# ==============================================================================
async def try_acquire_call_end_idempotency(request_id: str, clue_id: str) -> str:
    """通过 ExecutionTask 占位 call-end 幂等任务"""
    task_key = hashlib.sha256(f"call_end_idempotency_{request_id}".encode()).hexdigest()
    try:
        await ExecutionTask.create(
            task_key=task_key,
            task_type="summary",
            request_id=request_id,
            clue_id=clue_id,
            status="processing",
            run_at=get_now(),
        )
        return "acquired"
    except IntegrityError:
        pass

    record = await ExecutionTask.filter(task_key=task_key).first()
    if not record:
        return "existing_processing"
    if record.status == "succeeded":
        return "existing_success"
    return "existing_processing"


async def mark_call_end_idempotency_success(request_id: str):
    task_key = hashlib.sha256(f"call_end_idempotency_{request_id}".encode()).hexdigest()
    await ExecutionTask.filter(task_key=task_key).update(status="succeeded", finished_at=get_now())


async def mark_call_end_idempotency_failed(request_id: str, error_message: str):
    task_key = hashlib.sha256(f"call_end_idempotency_{request_id}".encode()).hexdigest()
    await ExecutionTask.filter(task_key=task_key).update(status="failed", last_error=error_message[:1000], finished_at=get_now())


# ==============================================================================
# 6. 配置层 CRUD (VoiceAgent, OutboundStrategy, WorkflowBinding, AppAuth)
# ==============================================================================
async def get_strategy_config(strategy_id: str) -> Optional[OutboundStrategy]:
    return await OutboundStrategy.filter(strategy_id=strategy_id).first()


async def save_strategy_config(data: dict) -> OutboundStrategy:
    strategy_id = data.get("strategy_id")
    existing = None
    if strategy_id:
        existing = await OutboundStrategy.filter(strategy_id=strategy_id).first()

    valid_fields = set(OutboundStrategy._meta.fields_map.keys())
    clean_data = {k: v for k, v in data.items() if k in valid_fields}
    if clean_data.get("name") is None:
        clean_data["name"] = ""

    if existing:
        for k, v in clean_data.items():
            setattr(existing, k, v)
        await existing.save()
        return existing
    return await OutboundStrategy.create(**clean_data)


async def get_voice_agent_config_by_id(agent_id: str) -> Optional[VoiceAgent]:
    return await VoiceAgent.filter(agent_id=agent_id).first()


async def get_voice_agent_config(agent_identifier: str, version: Optional[str] = None) -> Optional[VoiceAgent]:
    res = await VoiceAgent.filter(agent_id=agent_identifier).first()
    if res:
        return res
    query = VoiceAgent.filter(agent_name=agent_identifier)
    if version:
        query = query.filter(version=version)
    return await query.order_by("-create_time").first()


async def save_voice_agent_config(data: dict) -> VoiceAgent:
    agent_id = data.get("agent_id")
    existing = None
    if agent_id:
        existing = await VoiceAgent.filter(agent_id=agent_id).first()
    if not existing:
        agent_name = data.get("agent_name")
        version = data.get("version", "v1.0")
        if agent_name:
            existing = await VoiceAgent.filter(agent_name=agent_name, version=version).first()

    valid_fields = set(VoiceAgent._meta.fields_map.keys())
    clean_data = {k: v for k, v in data.items() if k in valid_fields}

    if existing:
        for k, v in clean_data.items():
            setattr(existing, k, v)
        await existing.save()
        return existing
    return await VoiceAgent.create(**clean_data)


async def get_workflow_config(workflow_id: str) -> Optional[WorkflowBinding]:
    return await WorkflowBinding.filter(workflow_id=workflow_id).first()


async def get_workflow_domain_config(workflow_id: str) -> Optional[WorkflowDomainConfig]:
    return await WorkflowDomainConfig.filter(workflow_id=workflow_id, is_active=True).first()


async def save_workflow_domain_config(workflow_id: str, domain_config: dict, version: str = "v1.0") -> WorkflowDomainConfig:
    existing = await WorkflowDomainConfig.filter(workflow_id=workflow_id).first()
    if existing:
        existing.domain_config = domain_config
        existing.version = version
        existing.is_active = True
        await existing.save()
        return existing
    return await WorkflowDomainConfig.create(
        workflow_id=workflow_id,
        domain_config=domain_config,
        version=version,
        is_active=True,
    )


async def atomic_handoff_clue_workflow(
    clue_id: str,
    target_workflow_id: str,
    target_agent_id: str,
    target_strategy_id: str,
    handoff_summary: str,
    max_handoffs: int = 2,
) -> Dict[str, Any]:
    """在数据库行级悲观锁内原子切换线索归属业务线并继承跨业务记忆。"""
    async with in_transaction() as connection:
        clue = await ClueInfo.filter(clue_id=clue_id).using_db(connection).select_for_update().first()
        if not clue:
            return {"success": False, "reason": f"clue_not_found:{clue_id}"}

        agent_data = dict(clue.agent_data or {})
        history = list(agent_data.get("handoff_history") or [])
        if len(history) >= max_handoffs:
            return {
                "success": False,
                "reason": f"max_handoffs_reached:{len(history)}>={max_handoffs}",
            }

        prev_workflow = clue.workflow_id
        if prev_workflow == target_workflow_id:
            return {"success": False, "reason": "same_workflow_noop"}

        # 清理旧业务线尚未执行的排期与认知唤醒任务
        await ExecutionTask.filter(
            clue_id=clue_id,
            task_type__in=["call", "agent_wakeup"],
            status__in=["pending", "retry"],
        ).using_db(connection).update(status="cancelled", last_error=f"handoff_to:{target_workflow_id}")

        from app.agents.memory_dreamer import MemoryDreamer

        directive_text = f"[跨业务交接:{prev_workflow}->{target_workflow_id}] {handoff_summary}"
        history.append({
            "from_workflow": prev_workflow,
            "to_workflow": target_workflow_id,
            "summary": handoff_summary,
            "timestamp": get_now().isoformat(),
        })
        agent_data["handoff_history"] = history
        agent_data["handoff_context"] = handoff_summary
        agent_data = MemoryDreamer.apply_to_agent_data(
            agent_data,
            directive=directive_text,
            customer_facts={"handoff_context": handoff_summary},
            digest_entry=directive_text,
            progress=f"handoff:{target_workflow_id}",
        )

        # 同步刷新目标策略的 clue_config 快照，防止后续排期仍沿用旧业务线路与频控配置
        target_strategy = await OutboundStrategy.filter(strategy_id=target_strategy_id).using_db(connection).first()
        update_fields = ["workflow_id", "agent_id", "strategy_id", "agent_data", "state_version", "update_time"]
        if target_strategy:
            from app.clue_inflow.service import normalize_config
            clue.clue_config = {
                "strategyId": target_strategy.strategy_id,
                "scheduleId": target_strategy.schedule_id,
                "followup": normalize_config(target_strategy.followup),
                "sip": normalize_config(target_strategy.sip),
            }
            update_fields.append("clue_config")

        clue.workflow_id = target_workflow_id
        clue.agent_id = target_agent_id
        clue.strategy_id = target_strategy_id
        clue.agent_data = agent_data
        clue.state_version += 1
        await clue.save(
            using_db=connection,
            update_fields=update_fields,
        )

        return {
            "success": True,
            "from_workflow": prev_workflow,
            "to_workflow": target_workflow_id,
            "handoff_count": len(history),
        }


async def count_recent_outbound_touches_by_phone(phone_number: str, since_dt: datetime) -> int:
    """按客户手机号跨业务线统计近段时间的总触达次数 (Global Outbox Pacer)。"""
    if not phone_number:
        return 0
    clue_ids = await ClueInfo.filter(phone_number=phone_number).values_list("clue_id", flat=True)
    if not clue_ids:
        return 0
    call_count = await CallRecord.filter(
        clue_id__in=list(clue_ids),
        submitted_at__gte=since_dt,
    ).count()
    action_count = await ExecutionTask.filter(
        clue_id__in=list(clue_ids),
        task_type="action",
        status="succeeded",
        finished_at__gte=since_dt,
    ).count()
    return int(call_count) + int(action_count)


async def save_workflow_config(data: dict) -> WorkflowBinding:
    workflow_id = data.get("workflow_id")
    existing = await WorkflowBinding.filter(workflow_id=workflow_id).first()
    valid_fields = set(WorkflowBinding._meta.fields_map.keys())
    clean_data = {k: v for k, v in data.items() if k in valid_fields}
    if existing:
        for k, v in clean_data.items():
            setattr(existing, k, v)
        await existing.save()
        return existing
    return await WorkflowBinding.create(**clean_data)


async def provision_workflow_bundle(data: dict) -> dict:
    """
    一站式业务线聚合创建或更新 (Facade 模式 + 数据库事务保障)：
    在一个数据库事务内依次执行：
    1. Voice Agent 配置落库 (如果传入 voice_agent，调用 save_voice_agent_config；如果仅传入 agent_id，则校验对应 Agent 是否存在)
    2. Outbound Strategy 配置落库 (如果传入 strategy，调用 save_strategy_config；如果仅传入 strategy_id，则校验对应策略是否存在)
    3. WorkflowBinding 业务线绑定落库 (将 workflow_id 与 agent_id, strategy_id 关联存储)
    
    任何环节异常时自动事务回滚。
    """
    from tortoise.transactions import in_transaction

    async with in_transaction():
        # 1. 处理 Voice Agent
        voice_agent_data = data.get("voice_agent")
        saved_agent = None
        if voice_agent_data:
            if hasattr(voice_agent_data, "model_dump"):
                voice_agent_data = voice_agent_data.model_dump()
            saved_agent = await save_voice_agent_config(voice_agent_data)
            agent_id = saved_agent.agent_id
        else:
            agent_id = data.get("agent_id")
            if agent_id:
                saved_agent = await get_voice_agent_config_by_id(agent_id)
                if not saved_agent:
                    saved_agent = await get_voice_agent_config(agent_id)
                if not saved_agent:
                    raise WorkflowAgentPairingError("此workflow_id业务线没有对应的agent可以配套", workflow_id=data.get("workflow_id"))
            else:
                raise WorkflowAgentPairingError("此workflow_id业务线没有对应的agent可以配套", workflow_id=data.get("workflow_id"))

        # 2. 处理 Outbound Strategy
        strategy_data = data.get("strategy")
        saved_strategy = None
        if strategy_data:
            if hasattr(strategy_data, "model_dump"):
                strategy_data = strategy_data.model_dump()
            saved_strategy = await save_strategy_config(strategy_data)
            strategy_id = saved_strategy.strategy_id
        else:
            strategy_id = data.get("strategy_id")
            if strategy_id:
                saved_strategy = await get_strategy_config(strategy_id)
                if not saved_strategy:
                    raise WorkflowAgentPairingError("此workflow_id业务线没有对应的agent可以配套", workflow_id=data.get("workflow_id"))
            else:
                raise WorkflowAgentPairingError("此workflow_id业务线没有对应的agent可以配套", workflow_id=data.get("workflow_id"))

        # 3. 处理 WorkflowBinding
        workflow_id = data.get("workflow_id")
        if not workflow_id:
            raise ValueError("workflow_id 不能为空")

        workflow_data = {
            "workflow_id": workflow_id,
            "agent_id": agent_id,
            "strategy_id": strategy_id,
            "name": data.get("name") or "",
            "version": data.get("version", "v1.0"),
            "description": data.get("description"),
            "is_active": data.get("is_active", True),
        }
        saved_binding = await save_workflow_config(workflow_data)

        # 3.5 处理 Multi-Agent 领域卡带配置 (WorkflowDomainConfig)
        saved_domain_cfg = None
        domain_cfg_data = data.get("domain_config")
        if domain_cfg_data:
            if hasattr(domain_cfg_data, "model_dump"):
                domain_cfg_data = domain_cfg_data.model_dump()
            if isinstance(domain_cfg_data, dict):
                domain_cfg_data.setdefault("workflow_id", workflow_id)
                domain_cfg_data.setdefault("voice_agent_id", agent_id)
                domain_cfg_data.setdefault("strategy_id", strategy_id)
                saved_domain_cfg = await save_workflow_domain_config(
                    workflow_id=workflow_id,
                    domain_config=domain_cfg_data,
                    version=domain_cfg_data.get("version") or data.get("version", "v1.0"),
                )

        # 4. 组装返回结构
        agent_dict = None
        if saved_agent:
            agent_dict = {
                "agent_id": saved_agent.agent_id,
                "agent_name": saved_agent.agent_name,
                "version": saved_agent.version,
                "description": saved_agent.description,
                "global_settings": saved_agent.globalSettings,
                "agent_type": saved_agent.agent_type,
                "open_message": saved_agent.open_message,
                "call_type": saved_agent.call_type,
                "create_time": saved_agent.create_time.isoformat() if saved_agent.create_time else None,
            }

        strategy_dict = None
        if saved_strategy:
            strategy_dict = {
                "strategy_id": saved_strategy.strategy_id,
                "schedule_id": saved_strategy.schedule_id,
                "name": saved_strategy.name,
                "followup": saved_strategy.followup,
                "sip": saved_strategy.sip,
                "webhook": saved_strategy.webhook,
                "create_time": saved_strategy.create_time.isoformat() if saved_strategy.create_time else None,
            }

        return {
            "workflow_id": saved_binding.workflow_id,
            "name": saved_binding.name,
            "agent_id": saved_binding.agent_id,
            "strategy_id": saved_binding.strategy_id,
            "version": saved_binding.version,
            "description": saved_binding.description,
            "is_active": saved_binding.is_active,
            "create_time": saved_binding.create_time.isoformat() if saved_binding.create_time else None,
            "voice_agent": agent_dict,
            "strategy": strategy_dict,
            "domain_config": saved_domain_cfg.domain_config if saved_domain_cfg else None,
        }


async def get_app_auth(app_id: str) -> Optional[AppAuth]:
    return await AppAuth.filter(app_id=app_id, is_active=True).first()


async def save_app_auth(
    app_id: str,
    app_secret: str,
    allowed_workflows: Optional[List[str]] = None,
    app_name: Optional[str] = None
) -> AppAuth:
    existing = await AppAuth.filter(app_id=app_id).first()
    if existing:
        existing.app_secret = app_secret
        if allowed_workflows is not None:
            existing.allowed_workflows = allowed_workflows
        if app_name is not None:
            existing.app_name = app_name
        existing.is_active = True
        await existing.save()
        return existing
    return await AppAuth.create(
        app_id=app_id,
        app_secret=app_secret,
        allowed_workflows=allowed_workflows or ["*"],
        app_name=app_name,
    )


# ==============================================================================
# 7. 批次调度辅助生成器
# ==============================================================================
async def iter_clue_state_batches(
    state: str,
    upper_threshold: Optional[datetime] = None,
    start_of_day: Optional[datetime] = None,
    batch_size: int = 500
):
    last_id = 0
    while True:
        query = ClueInfo.filter(status=state, id__gt=last_id)
        if upper_threshold:
            query = query.filter(update_time__lte=upper_threshold)
        if start_of_day:
            query = query.filter(update_time__gte=start_of_day)

        clues = await query.order_by("id").limit(batch_size).values("id", "clue_id", "update_time")
        if not clues:
            return
        # 兼容旧代码字段命名 clueId
        for c in clues:
            c["clueId"] = c["clue_id"]
        yield clues
        last_id = clues[-1]["id"]


async def get_clue_stats(
    state: str,
    upper_threshold: Optional[datetime] = None,
    start_of_day: Optional[datetime] = None
):
    query = ClueInfo.filter(status=state)
    if upper_threshold:
        query = query.filter(update_time__lte=upper_threshold)
    if start_of_day:
        query = query.filter(update_time__gte=start_of_day)
    return await query.limit(5000)
