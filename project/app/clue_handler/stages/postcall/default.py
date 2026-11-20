import json
import logging
from typing import Optional, Any, List, Dict, Union
from app.schemas.call_end import CallEndRequest
from app.dbmodels import crud
from app.dbmodels.crud import update_end_call_info_outbound_report
from app.clue_handler.state import ClueState
from app.redis_client.redis_manager import redis_manager
from app.strategy.contracts import BaseStrategyEngine
from app.strategy.factory import StrategyEngineFactory, StrategyEngineResolver
from app.call_schedule.service import schedule_call
from app.clue_handler.contracts.base_postcall import BasePostCallHandler

logger = logging.getLogger(__name__)


def extract_conversation_messages(context_list: Any) -> List[Dict[str, str]]:
    """
    清洗对话历史转换为 SummaryEngine 标准对话格式 [{"role": "...", "content": "..."}]
    """
    messages: List[Dict[str, str]] = []
    if not isinstance(context_list, list):
        return messages

    for item in context_list:
        if not isinstance(item, dict):
            continue
        sender = item.get("sender") or item.get("role")
        role = "assistant" if sender in ("robot", "assistant") else "user"
        content = item.get("text") or item.get("content") or ""
        content_str = str(content).strip()
        if content_str and not content_str.startswith("Call the "):
            messages.append({"role": role, "content": content_str})
    return messages


class DefaultPostCallHandler(BasePostCallHandler):
    """外呼结束后的 Agent 评估决策与流转处理器"""

    def __init__(
        self,
        strategy_engine: Optional[BaseStrategyEngine] = None,
        summary_engine: Optional[Any] = None,
        next_handler=None,
        engine_resolver: Optional[StrategyEngineResolver] = None,
        summary_type: Optional[str] = None,
        summary_prompt: Optional[str] = None,
    ):
        self.strategy_engine = strategy_engine
        self.summary_engine = summary_engine
        self.next_handler = next_handler
        self.engine_resolver = engine_resolver
        self.summary_type = summary_type
        self.summary_prompt = summary_prompt

    def resolve_engine(self, schedule_id: Optional[str] = None) -> BaseStrategyEngine:
        """解析外呼策略引擎 (优先使用注入的固定引擎，其次使用注入的 resolver，兜底使用 StrategyEngineFactory)"""
        if self.strategy_engine is not None:
            return self.strategy_engine
        resolver = self.engine_resolver or StrategyEngineFactory.get_engine
        return resolver(schedule_id)

    async def persist(self, clue_id: str, request_id: str, modified_request: CallEndRequest) -> None:
        """持久化总结分析结果"""
        outbound_report = modified_request.data.outboundReport
        if not outbound_report:
            return

        report_dict = outbound_report.model_dump()
        one_level = report_dict.get("oneLevelCode")
        two_level = report_dict.get("twoLevelCode")

        await crud.update_end_call_record_with_summary(
            clue_id=clue_id,
            one_level_code=int(one_level) if one_level is not None else None,
            two_level_code=int(two_level) if two_level is not None else None,
            user_qualified=report_dict.get("userQualified"),
            wechat_requested=report_dict.get("wechatRequested"),
            wechat_accepted=report_dict.get("wechatAccepted"),
            call_end_request=modified_request,
        )

        current_call = modified_request.data.currentCall
        call_id = current_call.callId if current_call else None
        await update_end_call_info_outbound_report(
            clue_id=clue_id,
            outbound_report=outbound_report,
            call_id=call_id,
        )

    async def handle(
        self,
        end_call_info: Union[CallEndRequest, str],
        request_id: Optional[str] = None,
        coalesced_events: Optional[List[Dict[str, Any]]] = None,
    ) -> bool:
        clue_id = ""
        try:
            resolved_record = None
            if isinstance(end_call_info, CallEndRequest):
                clue_id = end_call_info.data.clueId
                request_id = end_call_info.requestId or request_id or "req"

                # 1. 写入外呼结束记录至 CallRecord
                from app.dbmodels.crud import create_call_end_record
                record = await create_call_end_record(end_call_info)
                resolved_record = record
                from app.dbmodels.models import CallRecord
                if isinstance(record, CallRecord) and record.call_task_id:
                    clue_record = await crud.get_clue_info(clue_id)
                    already_summarized = (
                        clue_record is not None
                        and clue_record.status == "summary"
                        and getattr(record, "summary_status", None) == "succeeded"
                    )
                    if (
                        not clue_record
                        or clue_record.last_call_id != record.call_id
                        or clue_record.status in ("accepted", "rejected", "completed", "cancelled", "failed")
                        or already_summarized
                    ):
                        logger.info("[PostCallHandler] 【跳过回调】重复、历史或已关闭通话: clue_id=%r, call_id=%r", clue_id, record.call_id)
                        return True

                # 2. 将线索状态更新为 SUMMARY (若已在 SUMMARY 重试恢复则跳过转移)
                try:
                    cur_st = await crud.get_clue_state(clue_id)
                    if cur_st != ClueState.SUMMARY.value:
                        await crud.update_clue_state_or_raise(clue_id, ClueState.SUMMARY.value)
                except Exception as e:
                    from app.utils.exceptions import LockTimeoutError
                    if isinstance(e, LockTimeoutError):
                        raise
                    logger.warning(f"[PostCallHandler] 更新线索 {clue_id} 至 SUMMARY 状态异常: {e}")
            else:
                clue_id = str(end_call_info)
                request_id = request_id or "req"

            logger.info(f"[PostCallHandler] 开始处理线索 Summary: clue_id={clue_id}, request_id={request_id}")
            state = await crud.get_clue_state(clue_id)
            if state not in (ClueState.SUMMARY.value, ClueState.CALLING.value, ClueState.WAIT_FOR_CALLING.value):
                logger.warning(f"[PostCallHandler] 线索 {clue_id} 当前状态为 {state}，非 SUMMARY/CALLING/WAIT_FOR_CALLING，跳过")
                return False

            # 移除外呼兜底定时任务
            try:
                await redis_manager.get_delayed_queue().remove_task(f"summary_timeout:{clue_id}")
                await redis_manager.get_delayed_queue().remove_task(f"calling:{clue_id}")
                await redis_manager.get_delayed_queue().remove_task(f"wait_for_calling:{clue_id}")
            except Exception as rm_err:
                logger.debug(f"[PostCallHandler] 移除外呼兜底定时任务失败 (可忽略): {rm_err}")

            # 1. 查询本次通话记录与对话上下文 (优先精确复用刚刚解析出的 CallRecord)
            from app.dbmodels.models import CallRecord
            latest_call = resolved_record if isinstance(resolved_record, CallRecord) else None
            if latest_call is None:
                try:
                    latest_call = await CallRecord.filter(clue_id=clue_id).order_by("-create_time").first()
                except Exception as e:
                    logger.debug(f"[PostCallHandler] 查询 CallRecord 异常 (单元测试或无活跃数据库): {e}")

            raw_context = None
            if latest_call and getattr(latest_call, "config_snapshot", None) and isinstance(latest_call.config_snapshot, dict):
                raw_context = latest_call.config_snapshot.get("aiCallContextList")
            if isinstance(raw_context, str):
                try:
                    raw_context = json.loads(raw_context)
                except Exception:
                    raw_context = []
            messages = extract_conversation_messages(raw_context or [])

            # 2. 话后基础事实与报告解析（统一交由 AgentDrivenStrategyEngine 执行深度业务分析与排期）
            common_events: Dict[str, Any] = {}
            summary_reason: Optional[str] = None
            if latest_call and getattr(latest_call, "common_call_events", None) and isinstance(latest_call.common_call_events, dict):
                common_events = dict(latest_call.common_call_events)

            if isinstance(end_call_info, CallEndRequest) and end_call_info.data.outboundReport:
                report_data = end_call_info.data.outboundReport.model_dump(exclude_none=True)
                if report_data.get("conversationSummary") and not common_events.get("call_summary"):
                    common_events["call_summary"] = report_data["conversationSummary"]
                    summary_reason = str(report_data["conversationSummary"])
                if report_data.get("nextCallTime") and not common_events.get("next_call_time"):
                    common_events["has_next_call_appointment"] = "yes"
                    common_events["next_call_time"] = str(report_data["nextCallTime"])
                if report_data.get("wechatRequested") is not None:
                    common_events["wechat_requested"] = bool(report_data["wechatRequested"])

            is_voicemail = (common_events.get("is_voicemail") == "yes") if common_events else False
            call_summary = common_events.get("call_summary") or summary_reason or ""

            answered_flag = latest_call.answered if latest_call else None
            if answered_flag == 1 and not is_voicemail:
                is_connected = True
            elif len(messages) >= 2 and not is_voicemail:
                is_connected = True
            else:
                is_connected = False

            if latest_call:
                latest_call.common_call_events = common_events or (
                    {"is_voicemail": "no", "call_summary": "未接通"} if not is_connected else {}
                )
                latest_call.summary_status = "succeeded" if (common_events or is_connected) else "skipped"
                if call_summary:
                    decision_dict = dict(latest_call.decision_result or {})
                    decision_dict["call_summary"] = call_summary
                    latest_call.decision_result = decision_dict
                await latest_call.save()

            # 4. 触发标准事件回推：call_ended
            try:
                from app.apis.webhook_dispatcher import dispatch_webhook_event
                call_id = latest_call.call_id if latest_call else None
                call_data = {
                    "call_id": call_id,
                    "clue_id": clue_id,
                    "status": latest_call.status if latest_call else "ended",
                    "recording_url": latest_call.recording_url if latest_call else None,
                }
                await dispatch_webhook_event(
                    event="call_ended",
                    clue_id=clue_id,
                    call_id=call_id,
                    data={
                        "call": call_data,
                        "call_analysis_report": latest_call.common_call_events if (latest_call and latest_call.common_call_events) else common_events,
                    }
                )
            except Exception as ev_err:
                logger.warning(f"[PostCallHandler] 触发 call_ended 事件推送异常 (已降级): {ev_err}")

            # ==================== 下一轮追呼排期推演与终态流转 ====================
            try:
                from app.strategy.contracts import ClueContext
                from app.agents.contracts import is_channel_success_event
                from app.dbmodels.models import ClueInfo, OutboundStrategy, ExecutionTask
                from app.observability.events_bus import event_bus, ClueEndEvent
                from app.utils.time_utils import get_now

                # Per-Clue Event Coalescing: 若合并窗口内已有渠道成功事件（如企微通过），直接收口 ACCEPTED 并跳过排期
                for ev in (coalesced_events or []):
                    if is_channel_success_event(ev):
                        await ExecutionTask.filter(
                            clue_id=clue_id,
                            task_type__in=["agent_wakeup", "call"],
                            status__in=["pending", "retry"],
                        ).update(status="cancelled", last_error="cancelled_by_coalesced_channel_success")

                        clue_obj = await ClueInfo.filter(clue_id=clue_id).first()
                        if clue_obj:
                            from app.agents.memory_dreamer import MemoryDreamer
                            clue_obj.agent_data = MemoryDreamer.apply_to_agent_data(
                                clue_obj.agent_data,
                                digest_entry="[事件合并] 通话后窗口内收到渠道添加成功通知，直接收口 ACCEPTED",
                                progress="accepted_via_coalesced_channel",
                            )
                            await clue_obj.save(update_fields=["agent_data", "update_time"])

                        await crud.update_clue_state_or_raise(clue_id, ClueState.ACCEPTED.value)
                        await crud.save_close_reason(clue_id, "渠道添加成功(事件合并收口)")
                        await event_bus.emit(ClueEndEvent(
                            clue_id=clue_id,
                            workflow_id=clue_obj.workflow_id if clue_obj else None,
                            event_data={"code": 200, "reason": "渠道添加成功(事件合并收口)"},
                        ))
                        logger.info(f"[PostCallHandler] 【事件合并收口】线索 {clue_id} 合并渠道成功事件 -> ACCEPTED")
                        return True

                clue = await ClueInfo.filter(clue_id=clue_id).first()
                strategy_id = clue.strategy_id if clue and clue.strategy_id else "default"
                strategy = await OutboundStrategy.filter(strategy_id=strategy_id).first()
                schedule_id = strategy.schedule_id if strategy and strategy.schedule_id else "agent_driven"

                if self.strategy_engine is not None:
                    engine = self.strategy_engine
                else:
                    engine = self.resolve_engine(schedule_id)

                context = await ClueContext.from_db(clue_id)
                if clue and getattr(clue, "workflow_id", None):
                    context.workflow_id = clue.workflow_id
                if coalesced_events:
                    context.coalesced_events = list(coalesced_events)
                if clue and isinstance(getattr(clue, "agent_data", None), dict):
                    try:
                        from app.agents.memory_dreamer import MemoryDreamer
                        context.memory_index = MemoryDreamer.load_memory(clue.agent_data).to_compact_index()
                    except Exception:
                        pass

                # 若外部报告或记录包含客户预约时间，传入上下文供 Agent 排期推演
                if common_events and common_events.get("has_next_call_appointment") == "yes" and common_events.get("next_call_time"):
                    try:
                        from datetime import datetime
                        from app.utils.time_utils import ensure_tz_aware
                        appt_dt = datetime.strptime(common_events["next_call_time"], "%Y-%m-%d %H:%M")
                        context.customer_date = ensure_tz_aware(appt_dt)
                    except Exception as parse_err:
                        logger.debug(f"[PostCallHandler] 解析客户预约时间失败: {parse_err}")

                # 5. 动作与意图检测：判断是否触发后续渠道动作
                has_pending_action = False
                if common_events and (
                    common_events.get("wechat_requested") in (True, "yes", 1)
                    or common_events.get("action") in ("wechat", "sms", "add_wechat")
                ):
                    has_pending_action = True
                if not has_pending_action and isinstance(end_call_info, CallEndRequest):
                    outbound_report = end_call_info.data.outboundReport
                    if outbound_report and (getattr(outbound_report, "wechatRequested", False) or getattr(outbound_report, "wechat_requested", False)):
                        has_pending_action = True

                # 6. 统一由 AgentDrivenStrategyEngine 推演下一步排期或终态流转
                schedule_result = await engine.calculate(context)

                # 消费跨业务动态热切换建议 (Workflow Handoff)
                master_dec = getattr(schedule_result, "master_decision", None)
                handoff_target = getattr(master_dec, "handoff_workflow_id", None) if master_dec else None
                handoff_succeeded = False
                if handoff_target and clue and isinstance(getattr(clue, "workflow_id", None), str) and handoff_target != clue.workflow_id:
                    try:
                        from app.agents.domain_registry import domain_pack_registry
                        cur_pack = await domain_pack_registry.get_pack(clue.workflow_id)
                        if handoff_target in (cur_pack.allowed_handoff_targets or []):
                            target_pack = await domain_pack_registry.get_pack(handoff_target)
                            handoff_summary = getattr(master_dec, "handoff_context_summary", None) or schedule_result.reason or "跨业务意图切换"
                            handoff_res = await crud.atomic_handoff_clue_workflow(
                                clue_id=clue_id,
                                target_workflow_id=handoff_target,
                                target_agent_id=target_pack.voice_agent_id,
                                target_strategy_id=target_pack.strategy_id,
                                handoff_summary=handoff_summary,
                                max_handoffs=cur_pack.max_handoffs,
                            )
                            if handoff_res.get("success"):
                                handoff_succeeded = True
                                logger.info(
                                    f"[PostCallHandler] 【跨业务热切换成功】clue_id={clue_id}: "
                                    f"{handoff_res.get('from_workflow')} -> {handoff_target}"
                                )
                                clue = await ClueInfo.filter(clue_id=clue_id).first()
                    except Exception as hf_err:
                        logger.warning(f"[PostCallHandler] 跨业务热切换处理异常 (保持原业务): {hf_err}")

                # 消费 Agent 决策中的 voice_agent_directive 与 MasterSummaryDecision (方案 A 闭环落地)
                effective_directive = getattr(schedule_result, "voice_agent_directive", None)
                if handoff_succeeded and clue and isinstance(clue.agent_data, dict):
                    handoff_prefix = clue.agent_data.get("agent_directive") or ""
                    if effective_directive and effective_directive not in handoff_prefix:
                        effective_directive = f"{handoff_prefix} | {effective_directive}" if handoff_prefix else effective_directive
                    else:
                        effective_directive = handoff_prefix or effective_directive

                # Hot-Path 事务悲观锁内同步原子合并 4 桶记忆，Cold-Path 投递持久化 memory_dream 任务
                custom_facts = getattr(master_dec, "custom_evaluator_findings", None) if master_dec else None
                if clue:
                    try:
                        from app.agents.memory_dreamer import MemoryDreamer
                        mem_obj = await MemoryDreamer.curate_and_persist(
                            clue_id,
                            directive=effective_directive,
                            customer_facts=custom_facts if isinstance(custom_facts, dict) else None,
                            digest_entry=call_summary or schedule_result.reason,
                            progress=getattr(schedule_result, "business_status", None) or "followup",
                        )
                        if len(mem_obj.interaction_digest) >= 4 or handoff_succeeded:
                            await MemoryDreamer.enqueue_memory_dream_task(
                                clue_id,
                                workflow_id=clue.workflow_id,
                                reason="handoff" if handoff_succeeded else "digest_threshold",
                            )
                    except Exception as dir_err:
                        logger.warning(f"[PostCallHandler] 持久化 Agent 决策与四桶记忆异常: {dir_err}")

                if latest_call and (effective_directive or master_dec):
                    try:
                        dec = dict(latest_call.decision_result or {})
                        if effective_directive:
                            dec["voice_agent_directive"] = effective_directive
                        if master_dec and hasattr(master_dec, "model_dump"):
                            dec["master_decision"] = master_dec.model_dump(mode="json")
                            latest_call.summary_status = "succeeded"
                        latest_call.decision_result = dec
                        await latest_call.save()
                    except Exception as call_dir_err:
                        logger.warning(f"[PostCallHandler] 持久化 CallRecord 决策异常: {call_dir_err}")

                # 若触发高危工具人工审批挂起 (Deferred Tool Call)，流转为 AWAITING_APPROVAL 并结束本次 Run
                deferred_approval = getattr(master_dec, "deferred_approval", None) if master_dec else None
                if deferred_approval:
                    cur_st = await crud.get_clue_state(clue_id)
                    if cur_st != ClueState.AWAITING_APPROVAL.value:
                        await crud.update_clue_state_or_raise(clue_id, ClueState.AWAITING_APPROVAL.value)
                    logger.info(f"[PostCallHandler] 高危工具等待人工审批，线索挂起为 AWAITING_APPROVAL: clue_id={clue_id}")
                    return True

                # 若 SupervisorAgent 在 Single-Pass 中已通过 ToolBus 实际执行终态/渠道/排期工具，避免后置处理器重复流转或重复排期
                tool_execs = getattr(master_dec, "tool_executions", None) or []
                tool_terminated = any(
                    ex.get("tool_name") == "tool_terminate_clue"
                    and isinstance(ex.get("result"), dict)
                    and ex["result"].get("terminated") is True
                    for ex in tool_execs
                )
                tool_scheduled_task_id = next(
                    (
                        ex["result"].get("task_id")
                        for ex in tool_execs
                        if ex.get("tool_name") == "tool_schedule_followup"
                        and isinstance(ex.get("result"), dict)
                        and ex["result"].get("scheduled") is True
                    ),
                    None,
                )
                tool_wechat_pending = any(
                    ex.get("tool_name") == "tool_trigger_channel_action"
                    and isinstance(ex.get("result"), dict)
                    and ex["result"].get("triggered") is True
                    and ex["result"].get("channel") == "wechat"
                    for ex in tool_execs
                )
                current_clue_state = await crud.get_clue_state(clue_id)
                if tool_terminated or current_clue_state in (ClueState.ACCEPTED.value, ClueState.REJECTED.value):
                    logger.info(f"[PostCallHandler] ToolBus 已完成线索终态收口 ({current_clue_state})，跳过重复流转: clue_id={clue_id}")
                    return True
                if tool_wechat_pending or (current_clue_state == ClueState.PENDING.value and not tool_scheduled_task_id):
                    logger.info(f"[PostCallHandler] 线索已由 ToolBus 置为 PENDING 等待渠道结果，跳过重复排期: clue_id={clue_id}")
                    return True
                if tool_scheduled_task_id:
                    logger.info(f"[PostCallHandler] ToolBus 已创建排期任务 task_id={tool_scheduled_task_id}，跳过重复 schedule_call: clue_id={clue_id}")
                    try:
                        scheduled_task = await ExecutionTask.filter(id=tool_scheduled_task_id).first()
                        if scheduled_task and scheduled_task.run_at:
                            await dispatch_webhook_event(
                                event="call_scheduled",
                                clue_id=clue_id,
                                data={"scheduled_timestamp": int(scheduled_task.run_at.timestamp() * 1000)},
                            )
                    except Exception as sch_err:
                        logger.warning(f"[PostCallHandler] 触发 call_scheduled 回推失败: {sch_err}")
                    return True

                # 消费 Agent 驱动的 4 态终态标准
                biz_status = getattr(schedule_result, "business_status", None)
                if biz_status == "success":
                    if has_pending_action:
                        await crud.update_clue_state_or_raise(clue_id, ClueState.PENDING.value)
                        logger.info(f"[PostCallHandler] Agent判定为 success 且触发后续动作，流转为 PENDING: clue_id={clue_id}")
                        return True
                    terminal_state = ClueState.ACCEPTED.value
                    end_code = 200
                    close_reason = schedule_result.reason or "Agent判定目标达成结案"
                    await crud.update_clue_state_or_raise(clue_id, terminal_state)
                    await crud.save_close_reason(clue_id, close_reason)
                    logger.info(f"[PostCallHandler] Agent判定为 success，线索结案: clue_id={clue_id}")
                    try:
                        workflow_id = clue.workflow_id if clue else None
                        await event_bus.emit(ClueEndEvent(
                            clue_id=clue_id,
                            workflow_id=workflow_id,
                            event_data={"code": end_code, "reason": close_reason}
                        ))
                    except Exception as end_err:
                        logger.warning(f"[PostCallHandler] 触发 clue_ended 回推异常: {end_err}")
                    return True

                elif biz_status == "rejection":
                    terminal_state = ClueState.REJECTED.value
                    end_code = 300
                    close_reason = schedule_result.reason or "客户明确拒绝跟进，Agent判定终止"
                    await crud.update_clue_state_or_raise(clue_id, terminal_state)
                    await crud.save_close_reason(clue_id, close_reason)
                    logger.info(f"[PostCallHandler] Agent判定为 rejection，终止排期: clue_id={clue_id}")
                    try:
                        workflow_id = clue.workflow_id if clue else None
                        await event_bus.emit(ClueEndEvent(
                            clue_id=clue_id,
                            workflow_id=workflow_id,
                            event_data={"code": end_code, "reason": close_reason}
                        ))
                    except Exception as end_err:
                        logger.warning(f"[PostCallHandler] 触发 clue_ended 回推异常: {end_err}")
                    return True

                if schedule_result.success and schedule_result.next_call_time:
                    completed_call = await CallRecord.filter(clue_id=clue_id, call_task_id__gt=0,
                                                            status="ended").order_by("-attempt_no").first()
                    round_no = completed_call.attempt_no + 1 if completed_call else 1
                    from app.dbmodels.crud import update_clue_next_call_work_phone
                    await update_clue_next_call_work_phone(clue_id, schedule_result.use_work_phone)

                    # 当准备排期，先将状态改变为 SCHEDULED 状态
                    try:
                        await crud.update_clue_state_or_raise(clue_id, ClueState.SCHEDULED.value)
                    except Exception as sch_state_err:
                        logger.warning(f"[PostCallHandler] 更新线索 {clue_id} 至 SCHEDULED 状态异常: {sch_state_err}")

                    task = await schedule_call(
                        clue_id, schedule_result.next_call_time,
                        priority=schedule_result.priority if schedule_result.priority is not None else 5,
                        round_no=round_no,
                    )
                    logger.info("[PostCallHandler] 追呼排期已持久化: clue_id=%r, task_id=%s, round=%s, run_at=%s",
                                clue_id, task.id, round_no, task.run_at.isoformat())

                    # 触发 call_scheduled Webhook
                    try:
                        next_ts = int(task.run_at.timestamp() * 1000)
                        await dispatch_webhook_event(
                            event="call_scheduled",
                            clue_id=clue_id,
                            data={"scheduled_timestamp": next_ts}
                        )
                    except Exception as sch_err:
                        logger.warning(f"[PostCallHandler] 触发 call_scheduled 回推失败: {sch_err}")

                else:
                    # 策略判定终止排期 (超出追呼限制或熔断)
                    last_record = context.call_records[0] if context.call_records else None
                    decision_res = last_record.decision_result if last_record and hasattr(last_record, 'decision_result') and isinstance(last_record.decision_result, dict) else {}
                    is_qualified = decision_res.get("userQualified") or decision_res.get("wechatAccepted")

                    if is_qualified or is_connected:
                        if has_pending_action:
                            await crud.update_clue_state_or_raise(clue_id, ClueState.PENDING.value)
                            logger.info(f"[PostCallHandler] 通话达标且触发加微/短信动作，状态流转为 PENDING: clue_id={clue_id}")
                            return True

                        terminal_state = ClueState.ACCEPTED.value
                        end_code = 200
                        close_reason = schedule_result.reason or "通话达标完成"
                    else:
                        terminal_state = ClueState.REJECTED.value
                        end_code = 300
                        close_reason = schedule_result.reason or "线索跟进结束"

                    await crud.update_clue_state_or_raise(clue_id, terminal_state)
                    await crud.save_close_reason(clue_id, close_reason)
                    logger.info(f"[PostCallHandler] 线索 {clue_id} 终止排期并结案: status={terminal_state}, reason={close_reason}")

                    # 触发标准事件回推：clue_ended (写入 MySQL ExecutionTask 并且 Webhook 回推)
                    try:
                        workflow_id = clue.workflow_id if clue else None
                        await event_bus.emit(ClueEndEvent(
                            clue_id=clue_id,
                            workflow_id=workflow_id,
                            event_data={
                                "code": end_code,
                                "reason": close_reason,
                            }
                        ))
                    except Exception as end_err:
                        logger.warning(f"[PostCallHandler] 触发 clue_ended 回推异常: {end_err}")

            except Exception as sched_err:
                from app.utils.exceptions import LockTimeoutError
                if isinstance(sched_err, LockTimeoutError):
                    raise
                logger.error(f"[PostCallHandler] 下一轮排期推演异常: {sched_err}", exc_info=True)
                try:
                    await crud.update_clue_state_or_raise(clue_id, ClueState.REJECTED.value)
                    await crud.save_close_reason(clue_id, "排期推演异常关闭")
                except Exception:
                    pass

            return True

        except Exception as e:
            from app.utils.exceptions import LockTimeoutError
            if isinstance(e, LockTimeoutError):
                raise
            logger.error(f"[PostCallHandler] 处理 Summary 失败: clue_id={clue_id}, error={e}", exc_info=True)
            try:
                await redis_manager.get_delayed_queue().remove_task(f"summary_timeout:{clue_id}")
                await crud.update_clue_state_or_raise(clueId=clue_id, state=ClueState.REJECTED.value)
            except Exception:
                pass
            return False


# 历史兼容别名
PostCallHandler = DefaultPostCallHandler

