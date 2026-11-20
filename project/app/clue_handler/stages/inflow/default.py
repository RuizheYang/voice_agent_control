from __future__ import annotations

import logging
import time
from typing import Any, Awaitable, Callable, Optional
from datetime import datetime, timedelta, timezone

from app.strategy.contracts import ClueContext, BaseStrategyEngine
from app.strategy.factory import StrategyEngineFactory, StrategyEngineResolver
from app.clue_handler.state import ClueState
from app.clue_handler.contracts.base_inflow import BaseInflowHandler
from app.dbmodels import crud
from app.call_schedule.service import schedule_call
from app.schemas.clue_create import NormalizedClue
from app.utils.time_utils import ensure_tz_aware, get_now
from app.observability.events_bus import event_bus, ClueIntakeEvent
from app.apis.webhook_dispatcher import dispatch_webhook_event

logger = logging.getLogger(__name__)


class DefaultInflowHandler(BaseInflowHandler):
    """
    新线索流入阶段默认处理器 (DefaultInflowHandler):
    负责线索流入后的首呼判断与排期控制：
    1. 工作时间 (9:00 - 21:00)：立即安排首呼 (5秒内入池唤醒送号)。
    2. 非工作时间：根据策略 schedule_id 计算下一次可用工作时间，并通过统一 schedule_call 进行次呼排期。
    """

    def __init__(
        self,
        send_call_fn: Optional[Callable[..., Awaitable[Any]]] = None,
        strategy_engine: Optional[BaseStrategyEngine] = None,
        engine_resolver: Optional[StrategyEngineResolver] = None,
        call_voice_agent_fn: Optional[Callable[..., Awaitable[Any]]] = None,
    ):
        self.send_call_fn = send_call_fn or call_voice_agent_fn
        self.strategy_engine = strategy_engine
        self.engine_resolver = engine_resolver

    async def get_engine_for_clue(self, clue: NormalizedClue) -> BaseStrategyEngine:
        """获取线索对应的策略引擎实例 (优先复用注入的固定引擎，否则通过 strategy_id -> schedule_id 动态路由解析)"""
        if self.strategy_engine is not None:
            return self.strategy_engine

        resolver = self.engine_resolver or StrategyEngineFactory.get_engine

        strategy_id = clue.strategyId or "default"
        try:
            from app.dbmodels.models import OutboundStrategy
            strategy = await OutboundStrategy.filter(strategy_id=strategy_id).first()
            schedule_id = strategy.schedule_id if strategy and strategy.schedule_id else "agent_driven"
            return resolver(schedule_id)
        except Exception as e:
            logger.warning(f"[DefaultInflowHandler] 获取线索策略失败，使用兜底引擎: {e}")
            return resolver("agent_driven")

    async def schedule_call(
        self, 
        clue: NormalizedClue, 
        intake_time: datetime
    ) -> bool:
        """非工作时间排期次呼：通过统一 schedule_call 安排到次日工作时间"""
        ctx = await ClueContext.from_db(clue.clueId)
        if clue.workflowId and (not getattr(ctx, "workflow_id", None) or ctx.workflow_id == "default"):
            ctx.workflow_id = clue.workflowId
        engine = await self.get_engine_for_clue(clue)
        strategy_result = await engine.calculate(ctx)

        if strategy_result.success and strategy_result.next_call_time:
            from app.dbmodels.crud import update_clue_next_call_work_phone
            await update_clue_next_call_work_phone(clue.clueId, strategy_result.use_work_phone)

            # 当准备排期，先将状态改变为 SCHEDULED 状态
            try:
                await crud.update_clue_state_or_raise(clue.clueId, state=ClueState.SCHEDULED.value)
            except Exception as st_err:
                logger.warning(f"[DefaultInflowHandler] 更新线索 {clue.clueId} 至 SCHEDULED 状态异常: {st_err}")

            # 统一排期脚本调用
            await schedule_call(
                clue.clueId,
                strategy_result.next_call_time,
                priority=(
                    strategy_result.priority
                    if strategy_result.priority is not None
                    else 5
                ),
            )
            
            logger.info("[DefaultInflowHandler] 【完成】非工作时间线索次呼排期已入池: clue_id=%r, next_call_at=%s",
                        clue.clueId, strategy_result.next_call_time.isoformat())

            # 触发标准事件回推：call_scheduled
            try:
                next_time = strategy_result.next_call_time
                scheduled_ts = int(next_time.timestamp() * 1000) if hasattr(next_time, "timestamp") else int(time.time() * 1000)
                await dispatch_webhook_event(
                    event="call_scheduled",
                    clue_id=clue.clueId,
                    data={"scheduled_timestamp": scheduled_ts}
                )
            except Exception as ev_err:
                logger.warning(f"[DefaultInflowHandler] 触发 call_scheduled 事件推送异常 (已降级): {ev_err}")

            return True
        else:
            await crud.update_clue_state_or_raise(clue.clueId, state=ClueState.REJECTED.value)
            logger.info("[DefaultInflowHandler] 【拒绝】策略未生成呼叫排期: clue_id=%r, reason=%r",
                        clue.clueId, strategy_result.reason)
            return False

    async def schedule_immediate_call(
        self, 
        clue: NormalizedClue, 
        intake_time: datetime
    ) -> bool:
        """工作时间首呼：入池延迟 5 秒发起外呼送号"""
        from app.dbmodels.crud import update_clue_next_call_work_phone
        await update_clue_next_call_work_phone(clue.clueId, False)

        # 当准备排期，先将状态改变为 SCHEDULED 状态
        try:
            await crud.update_clue_state_or_raise(clue.clueId, state=ClueState.SCHEDULED.value)
        except Exception as st_err:
            logger.warning(f"[DefaultInflowHandler] 更新线索 {clue.clueId} 至 SCHEDULED 状态异常: {st_err}")

        run_at = get_now() + timedelta(seconds=5)
        await schedule_call(
            clue.clueId,
            run_at,
            priority=5,
        )
        logger.info("[DefaultInflowHandler] 【完成】工作时间首呼已入池: clue_id=%r, run_at=%s", clue.clueId, run_at.isoformat())
        return True

    async def handle(self, clue: NormalizedClue) -> bool:
        state = await crud.get_clue_state(clue.clueId)
        if state in [ClueState.ACCEPTED.value, ClueState.REJECTED.value, ClueState.CALLING.value,
                     ClueState.WAIT_FOR_CALLING.value, ClueState.SUMMARY.value, ClueState.PENDING.value]:
            logger.info("[DefaultInflowHandler] 【跳过】线索已进入后续阶段或终态: clue_id=%r, state=%s", clue.clueId, state)
            return True
        if state in (ClueState.CREATED.value, ClueState.SCHEDULED.value):
            current = await crud.get_clue_info(clue.clueId)
            if current and current.next_call_at:
                due = current.next_call_at
                if due.tzinfo is None:
                    due = due.replace(tzinfo=timezone.utc)
                await schedule_call(clue.clueId, due, priority=5)
                logger.info("[DefaultInflowHandler] 【恢复】沿用既有排期完成入池: clue_id=%r, next_call_at=%s",
                            clue.clueId, due.isoformat())
                return True
            if state == ClueState.SCHEDULED.value:
                if current and current.import_task_id:
                    raise RuntimeError("新线索已标记排期但缺少排期时间")
                return True

        # 自动分诊路由: 当 workflowId="auto" 时，交由 ClueSupervisorAgent 匹配最佳业务卡带
        if (clue.workflowId or "").strip().lower() == "auto":
            try:
                from app.agents.supervisor import clue_supervisor_agent
                from app.agents.domain_registry import domain_pack_registry

                routed_wf = await clue_supervisor_agent.triage_inflow_workflow(
                    agent_data=clue.agentData if isinstance(clue.agentData, dict) else {},
                    fallback_workflow_id="default",
                )
                if routed_wf and routed_wf != "auto":
                    pack = await domain_pack_registry.get_pack(routed_wf)
                    clue.workflowId = routed_wf
                    clue.agentId = pack.voice_agent_id
                    clue.strategyId = pack.strategy_id
                    clue_record = await crud.get_clue_info(clue.clueId)
                    if clue_record:
                        clue_record.workflow_id = routed_wf
                        clue_record.agent_id = pack.voice_agent_id
                        clue_record.strategy_id = pack.strategy_id
                        await clue_record.save(update_fields=["workflow_id", "agent_id", "strategy_id", "update_time"])
                    logger.info(f"[DefaultInflowHandler] 【自动分诊】clue_id={clue.clueId} 路由至业务线: {routed_wf}")
            except Exception as triage_err:
                logger.warning(f"[DefaultInflowHandler] 自动分诊降级 (保留 default): {triage_err}")

        intake_time = clue.accessReceiveTime
        if isinstance(intake_time, str):
            try:
                intake_time = ensure_tz_aware(datetime.strptime(intake_time, "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                intake_time = get_now()

        await event_bus.emit(ClueIntakeEvent(
            clue_id=clue.clueId,
            workflow_id=clue.workflowId,
            event_data={
                "intake_time": str(intake_time),
                "customer_name": clue.customerName,
                "workflow_id": clue.workflowId,
            }
        ))

        datenow = get_now()
        if intake_time < datenow:
            intake_time = datenow

        # 合规工时窗口校验 (动态读取策略/卡带工时，消除硬编码 9-21)
        from app.strategy.agent_driven.tools import is_within_working_window, resolve_working_window
        work_start, work_end = "09:00:00", "21:00:00"
        try:
            clue_rec = await crud.get_clue_info(clue.clueId)
            if clue_rec and isinstance(clue_rec.clue_config, dict):
                work_start, work_end = resolve_working_window(clue_rec.clue_config, work_start, work_end)
            else:
                engine = await self.get_engine_for_clue(clue)
                cfg = getattr(engine, "strategy_config", None)
                if cfg:
                    work_start = getattr(cfg, "work_start_time", work_start)
                    work_end = getattr(cfg, "work_end_time", work_end)
        except Exception:
            pass

        if is_within_working_window(intake_time, work_start, work_end):
            success = await self.schedule_immediate_call(clue, intake_time)
        else:
            try:
                await dispatch_webhook_event(
                    event="wechat_requested",
                    clue_id=clue.clueId,
                    data={
                        "request_timestamp": int(time.time() * 1000),
                        "method": "offline"
                    }
                )
            except Exception as ev_err:
                logger.warning(f"[DefaultInflowHandler] 触发 wechat_requested 事件推送异常: {ev_err}")

            success = await self.schedule_call(clue, intake_time)

        return success
