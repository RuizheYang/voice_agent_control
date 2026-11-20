from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional

from pydantic_ai import Agent, DeferredToolRequests, RunContext

from app.agents.contracts import (
    AgentRuntimeDeps,
    BusinessDomainPack,
    ClueStructuredMemory,
    SupervisorActionPlan,
    WakeupEvaluationContext,
    is_channel_success_event,
)
from app.agents.domain_registry import DomainPackRegistry, domain_pack_registry
from app.agents.memory_dreamer import MemoryDreamer
from app.agents.tool_bus import build_domain_toolset, execute_tool_by_name
from app.config import config
from app.strategy.agent_driven.engine import AgentDrivenStrategyEngine
from app.strategy.agent_driven.models import (
    AgentStrategyConfig,
    MasterSummaryDecision,
    ScheduleActionType,
    StandardBusinessStatus,
)
from app.strategy.contracts.context import ClueContext
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)


class ClueSupervisorAgent:
    """
    全局无状态单例 Multi-Agent 编排中枢 (Stateless Supervisor):
    通过 PydanticAI RunContext[AgentRuntimeDeps] + 动态组合 Toolsets 实现按线索瞬时换装，
    支持：
    1. 线索流入自动分诊 (Inflow Triage for workflowId='auto')
    2. 话后 Single-Pass 复合决策 + Guarded ToolBus 实时调用 + Deferred Approval 人工审批挂起
    3. Always-On 认知唤醒自检 (on_wakeup_timer / approval_resumed)
    4. 上线前 Dry-Run 预检 (评估器/可见工具/护栏预览)
    5. 严格 5.0s 超时控制与确定性降级
    """

    def __init__(self, registry: Optional[DomainPackRegistry] = None, timeout_seconds: float = 5.0):
        self.registry = registry or domain_pack_registry
        self.timeout_seconds = timeout_seconds
        self._agent: Optional[Agent[AgentRuntimeDeps, Any]] = None

    def _get_model(self):
        try:
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider

            llm_cfg = config.llm_factory_config
            if not llm_cfg or not llm_cfg.base_url:
                return None
            # 若未配置真实密钥（除本地兼容端点），允许走调用时由外部决定
            if not llm_cfg.api_key and "openai.com" in llm_cfg.base_url:
                return None
            provider = OpenAIProvider(base_url=llm_cfg.base_url, api_key=llm_cfg.api_key or "sk-dummy")
            return OpenAIChatModel(llm_cfg.model_name or "default-model", provider=provider)
        except Exception as err:
            logger.debug(f"[ClueSupervisor] 初始化模型跳过: {err}")
            return None

    def get_stateless_agent(self) -> Optional[Agent[AgentRuntimeDeps, Any]]:
        """获取全局唯一的无状态 PydanticAI Agent 单例"""
        model = self._get_model()
        if not model:
            return None
        if self._agent is None:
            agent: Agent[AgentRuntimeDeps, Any] = Agent(
                model=model,
                deps_type=AgentRuntimeDeps,
                output_type=SupervisorActionPlan,
                defer_model_check=True,
            )

            @agent.instructions
            def _inject_domain_cartridge_instructions(ctx: RunContext[AgentRuntimeDeps]) -> str:
                pack = ctx.deps.domain_pack
                mem_idx = ctx.deps.structured_memory.to_compact_index()
                coalesced_str = ""
                if ctx.deps.coalesced_events:
                    coalesced_str = f"\n近期合并到达事件: {ctx.deps.coalesced_events}"
                return (
                    f"你当前正在以【{pack.name or pack.workflow_id}】(workflow_id={pack.workflow_id}) 身份处理线索 {ctx.deps.clue_id}。\n"
                    f"业务职责范围: {pack.domain_description}\n"
                    f"业务专属准则: {pack.domain_master_prompt or '遵循标准转化与合规防骚扰准则。'}\n"
                    f"合法外呼工时窗口: {pack.work_start_time} - {pack.work_end_time}\n"
                    f"结构化记忆索引: {mem_idx}\n"
                    f"全局 24h 已触达次数: {ctx.deps.recent_24h_touches}/{pack.max_touches_per_24h}；"
                    f"已跨业务切换次数: {ctx.deps.handoff_count}/{pack.max_handoffs}。"
                    f"{coalesced_str}"
                )

            self._agent = agent
        return self._agent

    async def build_runtime_deps(
        self,
        clue_id: str,
        workflow_id: Optional[str] = None,
        *,
        dry_run: bool = False,
        coalesced_events: Optional[List[Dict[str, Any]]] = None,
    ) -> AgentRuntimeDeps:
        """动态装配 PydanticAI RunContext[AgentRuntimeDeps]"""
        wf_id = workflow_id
        agent_data: Dict[str, Any] = {}
        phone_number: Optional[str] = None
        recent_touches = 0

        if not dry_run:
            try:
                from app.dbmodels import crud

                clue = await crud.get_clue_info(clue_id)
                if clue:
                    wf_id = wf_id or clue.workflow_id
                    agent_data = dict(clue.agent_data or {})
                    phone_number = clue.phone_number
                    if phone_number:
                        since = get_now() - timedelta(hours=24)
                        recent_touches = await crud.count_recent_outbound_touches_by_phone(phone_number, since)
            except Exception as err:
                logger.debug(f"[ClueSupervisor] 加载线索运行时上下文降级: {err}")

        pack = await self.registry.get_pack(wf_id)
        memory = MemoryDreamer.load_memory(agent_data)
        handoff_history = list(agent_data.get("handoff_history") or [])
        target_descs = await self.registry.get_target_descriptions(pack.allowed_handoff_targets)

        return AgentRuntimeDeps(
            clue_id=clue_id,
            domain_pack=pack,
            structured_memory=memory,
            handoff_count=len(handoff_history),
            target_descriptions=target_descs,
            phone_number=phone_number,
            recent_24h_touches=recent_touches,
            coalesced_events=list(coalesced_events or []),
            dry_run=dry_run,
        )

    async def inspect_visible_tools(self, deps: AgentRuntimeDeps) -> List[Dict[str, Any]]:
        """检查给定业务卡带与运行时上下文下，经 Filtered/Prepared 后的真实可见工具清单"""
        toolset = build_domain_toolset(deps.domain_pack)
        ctx = RunContext(deps=deps, model=None, usage=None)  # type: ignore[arg-type]
        raw_tools = await toolset.get_tools(ctx)
        return [
            {
                "name": name,
                "description": t.tool_def.description,
                "requires_approval": name in (deps.domain_pack.approval_required_tools or []),
            }
            for name, t in raw_tools.items()
        ]

    @staticmethod
    def _extract_tool_executions(result: Any) -> List[Dict[str, Any]]:
        """从 PydanticAI RunResult 消息历史中提取工具调用与返回审计记录"""
        executions: List[Dict[str, Any]] = []
        by_call_id: Dict[str, Dict[str, Any]] = {}
        try:
            for msg in result.new_messages():
                for part in getattr(msg, "parts", []) or []:
                    part_kind = getattr(part, "part_kind", "")
                    if part_kind == "tool-call":
                        call_id = getattr(part, "tool_call_id", "")
                        entry = {
                            "tool_name": getattr(part, "tool_name", ""),
                            "args": getattr(part, "args", None),
                            "tool_call_id": call_id,
                        }
                        executions.append(entry)
                        if call_id:
                            by_call_id[call_id] = entry
                    elif part_kind == "tool-return":
                        call_id = getattr(part, "tool_call_id", "")
                        if call_id and call_id in by_call_id:
                            content = getattr(part, "content", None)
                            by_call_id[call_id]["result"] = (
                                content.model_dump(mode="json") if hasattr(content, "model_dump") else content
                            )
        except Exception:
            pass
        return executions

    async def _persist_deferred_approvals(
        self,
        clue_id: str,
        workflow_id: str,
        output: DeferredToolRequests,
        *,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """将 DeferredToolRequests 中的高危工具调用请求持久化为 tool_approval 任务"""
        approvals = getattr(output, "approvals", []) or []
        first_req = approvals[0] if approvals else None
        tool_name = getattr(first_req, "tool_name", "unknown_tool") if first_req else "unknown_tool"
        tool_args = first_req.args_as_dict() if (first_req and hasattr(first_req, "args_as_dict")) else {}
        approval_detail: Dict[str, Any] = {
            "tool_name": tool_name,
            "tool_args": tool_args,
            "approval_count": len(approvals),
            "approval_task_ids": [],
        }
        if not dry_run:
            from app.agents.approvals import create_deferred_approval_task
            for req in approvals:
                req_name = getattr(req, "tool_name", "unknown_tool")
                req_args = req.args_as_dict() if hasattr(req, "args_as_dict") else {}
                approval_task = await create_deferred_approval_task(
                    clue_id=clue_id,
                    workflow_id=workflow_id,
                    tool_name=req_name,
                    tool_args=req_args,
                    reason=f"Agent 请求调用高危工具 '{req_name}'，等待人工审批",
                )
                approval_detail["approval_task_ids"].append(approval_task.id)
            if approval_detail["approval_task_ids"]:
                approval_detail["approval_task_id"] = approval_detail["approval_task_ids"][0]
        return approval_detail

    async def run_single_pass_postcall(
        self,
        clue_id: str,
        prompt: str,
        workflow_id: Optional[str] = None,
        *,
        dry_run: bool = False,
        timeout: Optional[float] = None,
        coalesced_events: Optional[List[Dict[str, Any]]] = None,
    ) -> MasterSummaryDecision:
        """
        Tier 1 Single-Pass 话后主决策入口:
        将 ClueSupervisorAgent + Guarded ToolBus + Deferred Approval 正式接入真实业务主链路，
        单次 LLM 调用同时完成业务维度分析、声明式 HTTP 工具调用与 4 态排期收敛。
        """
        agent = self.get_stateless_agent()
        if not agent:
            raise RuntimeError("No LLM model configured for ClueSupervisorAgent")

        deps = await self.build_runtime_deps(
            clue_id,
            workflow_id=workflow_id,
            dry_run=dry_run,
            coalesced_events=coalesced_events,
        )
        from app.strategy.agent_driven.evaluators import build_single_pass_decision_model

        decision_schema = build_single_pass_decision_model(
            deps.domain_pack.workflow_id,
            deps.domain_pack.custom_evaluators,
        )
        toolset = build_domain_toolset(deps.domain_pack)
        output_spec: Any = (
            [decision_schema, DeferredToolRequests]
            if deps.domain_pack.approval_required_tools
            else decision_schema
        )
        effective_timeout = timeout or self.timeout_seconds or 5.0

        result = await asyncio.wait_for(
            agent.run(
                prompt,
                deps=deps,
                toolsets=[toolset],
                output_type=output_spec,
            ),
            timeout=effective_timeout,
        )
        output = result.output

        if isinstance(output, DeferredToolRequests):
            approval_detail = await self._persist_deferred_approvals(
                clue_id, deps.domain_pack.workflow_id, output, dry_run=dry_run
            )
            tool_name = approval_detail["tool_name"]
            return MasterSummaryDecision(
                status=StandardBusinessStatus.PARTIAL,
                reason=f"高危工具 '{tool_name}' 已挂起等待人工审批 (Deferred Tool Call)",
                action=ScheduleActionType.SCHEDULE_CALL,
                deferred_approval=approval_detail,
                tool_executions=self._extract_tool_executions(result),
            )

        executions = self._extract_tool_executions(result)
        if decision_schema is not MasterSummaryDecision:
            raw_findings = getattr(output, "custom_evaluator_findings", None)
            findings_dict = (
                raw_findings.model_dump(exclude_none=True)
                if hasattr(raw_findings, "model_dump")
                else dict(raw_findings or {})
            )
            return MasterSummaryDecision.model_validate({
                **output.model_dump(exclude={"custom_evaluator_findings", "tool_executions"}),
                "custom_evaluator_findings": findings_dict,
                "tool_executions": executions,
            })

        decision: MasterSummaryDecision = output
        decision.tool_executions = executions
        return decision

    async def evaluate_wakeup(
        self,
        clue_id: str,
        *,
        check_goal: str = "",
        coalesced_events: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Always-On 认知自唤醒执行器 (`agent_wakeup` / `approval_resumed` 共用):
        1. 先处理合并到达的硬事实事件（如企微通过通知 / 人工审批放行或驳回）；
        2. 构造 WakeupEvaluationContext（当前状态、合并事件、四桶记忆、今日剩余配额）；
        3. 若仍处于非终态且配置了 LLM，则挂载 Guarded ToolBus（含 DeferredToolRequests 门禁）唤醒自主决策；
        4. 若无 LLM 或超时，执行确定性 Always-On 唤醒自检（补发短信并安排工作窗电话追呼）并沉淀四桶记忆。
        """
        from app.clue_handler.state import ClueState
        from app.dbmodels import crud
        from app.dbmodels.models import ExecutionTask
        from app.observability.events_bus import ClueEndEvent, event_bus
        from app.strategy.agent_driven.tools import snap_to_working_window

        terminal_states = (ClueState.ACCEPTED.value, ClueState.REJECTED.value, "completed", "cancelled", "failed")
        events = list(coalesced_events or [])
        clue = await crud.get_clue_info(clue_id)
        if not clue:
            return {"handled": True, "outcome": "clue_not_found"}

        if clue.status in terminal_states:
            return {"handled": True, "outcome": "already_terminal", "status": clue.status}

        # 1. 处理合并事件中的企微/渠道成功通知（兼容 success=True 与 data.resultCode=='3'）
        for ev in events:
            if is_channel_success_event(ev):
                await ExecutionTask.filter(
                    clue_id=clue_id,
                    task_type__in=["agent_wakeup", "call"],
                    status__in=["pending", "retry"],
                ).update(status="cancelled", last_error="cancelled_by_wakeup_channel_success")
                await crud.update_clue_state_or_raise(clue_id, ClueState.ACCEPTED.value)
                await crud.save_close_reason(clue_id, "渠道添加成功(合并事件唤醒收口)")
                await MemoryDreamer.curate_and_persist(
                    clue_id,
                    digest_entry=f"[唤醒自检] {check_goal} -> 渠道已通过，收口 ACCEPTED",
                    progress="accepted_via_channel",
                )
                await event_bus.emit(ClueEndEvent(
                    clue_id=clue_id,
                    workflow_id=clue.workflow_id,
                    event_data={"code": 200, "reason": "渠道添加成功"},
                ))
                return {"handled": True, "outcome": "accepted_via_channel"}

        # 2. 处理人工审批恢复事件 (approval_resumed)，并在执行工具后刷新线索状态
        approval_notes: List[str] = []
        for ev in events:
            if ev.get("type") == "approval_resumed":
                tool_name = ev.get("tool_name")
                tool_args = ev.get("tool_args") or {}
                if ev.get("approved") and tool_name:
                    exec_res = await execute_tool_by_name(
                        clue_id,
                        tool_name,
                        tool_args,
                        workflow_id=clue.workflow_id,
                        bypass_approval=True,
                    )
                    approval_notes.append(f"审批通过并已执行 {tool_name}: {exec_res}")
                else:
                    approval_notes.append(f"人工审批驳回 {tool_name}: {ev.get('comment', '')}")

        if approval_notes:
            refreshed = await crud.get_clue_info(clue_id)
            if refreshed:
                clue = refreshed
            if clue.status in terminal_states:
                await MemoryDreamer.curate_and_persist(
                    clue_id,
                    digest_entry=f"[审批恢复] {'; '.join(approval_notes)[:160]} -> 终态 {clue.status}",
                    progress=f"approval_terminal:{clue.status}",
                )
                return {
                    "handled": True,
                    "outcome": f"approval_executed_terminal:{clue.status}",
                    "status": clue.status,
                }

        # 3. 构造 WakeupEvaluationContext，挂载 ToolBus 唤醒 SupervisorAgent 自主思考与行动
        agent = self.get_stateless_agent()
        deps = await self.build_runtime_deps(
            clue_id,
            workflow_id=clue.workflow_id,
            coalesced_events=events,
        )
        remaining_touches = max(0, deps.domain_pack.max_touches_per_24h - deps.recent_24h_touches)
        wakeup_ctx = WakeupEvaluationContext(
            clue_id=clue_id,
            workflow_id=deps.domain_pack.workflow_id,
            current_status=clue.status,
            check_goal=check_goal or "检查线索当前进度并决定下一步跟进动作",
            coalesced_events=events,
            memory_index=deps.structured_memory.to_compact_index(),
            recent_24h_touches=deps.recent_24h_touches,
            max_touches_per_24h=deps.domain_pack.max_touches_per_24h,
            remaining_touches_today=remaining_touches,
            approval_notes=approval_notes,
        )
        if agent is not None:
            try:
                toolset = build_domain_toolset(deps.domain_pack)
                output_spec: Any = (
                    [SupervisorActionPlan, DeferredToolRequests]
                    if deps.domain_pack.approval_required_tools
                    else SupervisorActionPlan
                )
                wakeup_prompt = (
                    f"【认知唤醒定时自检 (Always-On Wakeup)】\n"
                    f"唤醒上下文 (WakeupEvaluationContext): {wakeup_ctx.model_dump_json(ensure_ascii=False)}\n"
                    f"请结合结构化记忆、近期事件与剩余今日配额 ({wakeup_ctx.remaining_touches_today})，"
                    f"直接调用合适工具推进业务（如发短信、安排电话追呼、重设唤醒定时器或收口结案），"
                    f"并输出 SupervisorActionPlan。"
                )
                res = await asyncio.wait_for(
                    agent.run(
                        wakeup_prompt,
                        deps=deps,
                        toolsets=[toolset],
                        output_type=output_spec,
                    ),
                    timeout=self.timeout_seconds,
                )
                if isinstance(res.output, DeferredToolRequests):
                    approval_detail = await self._persist_deferred_approvals(
                        clue_id, deps.domain_pack.workflow_id, res.output, dry_run=False
                    )
                    return {
                        "handled": True,
                        "outcome": "deferred_approval",
                        "deferred_approval": approval_detail,
                        "tools_called": len(self._extract_tool_executions(res)),
                    }
                plan: SupervisorActionPlan = res.output
                await MemoryDreamer.curate_and_persist(
                    clue_id,
                    digest_entry=f"[认知唤醒] {check_goal} -> {plan.reasoning[:120]}",
                    progress="wakeup_evaluated",
                )
                return {
                    "handled": True,
                    "outcome": "supervisor_executed",
                    "reasoning": plan.reasoning,
                    "tools_called": len(self._extract_tool_executions(res)),
                }
            except Exception as err:
                logger.warning(f"[ClueSupervisor] 唤醒推理降级为确定性检查 (clue_id={clue_id}): {err}")

        # 4. 确定性唤醒兜底：若处于 PENDING 仍未通过且今日触达配额充足，补发短信提醒并安排合法工时窗回访
        fallback_action = "memory_recorded"
        if clue.status == ClueState.PENDING.value and wakeup_ctx.remaining_touches_today > 0:
            sms_dispatched = False
            try:
                from app.apis.webhook_dispatcher import dispatch_webhook_event
                await dispatch_webhook_event(
                    event="action_sms",
                    clue_id=clue_id,
                    workflow_id=clue.workflow_id,
                    data={"channel": "sms", "message_hint": f"唤醒自检补发提醒: {check_goal}"},
                )
                sms_dispatched = True
                fallback_action = "sms_followup_dispatched"
            except Exception:
                pass

            if "tool_schedule_followup" in (deps.domain_pack.allowed_tools or []):
                try:
                    has_pending_call = await ExecutionTask.filter(
                        clue_id=clue_id,
                        task_type="call",
                        status__in=["pending", "retry"],
                    ).exists()
                    if not has_pending_call:
                        from app.call_schedule.service import schedule_call

                        target_dt = snap_to_working_window(
                            get_now() + timedelta(hours=3),
                            deps.domain_pack.work_start_time,
                            deps.domain_pack.work_end_time,
                        )
                        await crud.update_clue_state_or_raise(clue_id, ClueState.SCHEDULED.value)
                        await schedule_call(clue_id, target_dt, priority=5)
                        fallback_action = (
                            "sms_and_followup_scheduled" if sms_dispatched else "followup_scheduled"
                        )
                except Exception as sched_err:
                    logger.debug(f"[ClueSupervisor] 唤醒兜底安排回访跳过: {sched_err}")
        elif clue.status == ClueState.AWAITING_APPROVAL.value and approval_notes:
            # 审批驳回或执行非终态工具后若仍停留在 AWAITING_APPROVAL，安全恢复至 SCHEDULED 以便后续推进
            try:
                await crud.update_clue_state_or_raise(clue_id, ClueState.SCHEDULED.value)
                fallback_action = "restored_to_scheduled_after_approval"
            except Exception:
                pass

        await MemoryDreamer.curate_and_persist(
            clue_id,
            digest_entry=f"[唤醒自检] {check_goal} (合并事件 {len(events)} 条, action={fallback_action})",
            progress="wakeup_checked",
        )
        return {"handled": True, "outcome": fallback_action, "coalesced_count": len(events)}

    async def triage_inflow_workflow(
        self,
        agent_data: Optional[Dict[str, Any]] = None,
        fallback_workflow_id: str = "default",
    ) -> str:
        """
        当新线索流入 workflowId='auto' 时，基于所有注册业务卡带的 domain_description 自动分诊路由。
        优先尝试语义匹配，若无模型或超时 (<3s) 自动安全匹配关键词或兜底返回 fallback_workflow_id。
        """
        packs = await self.registry.list_all_packs()
        if not packs:
            return fallback_workflow_id

        notes_text = ""
        if isinstance(agent_data, dict):
            notes_text = " ".join(
                str(v) for v in agent_data.values() if isinstance(v, (str, int, float))
            )

        best_kw_wf = fallback_workflow_id
        for wf_id, pack in packs.items():
            if wf_id in ("default", "auto"):
                continue
            keywords = [wf_id, pack.name]
            if wf_id == "car_loan":
                keywords.extend(["车贷", "分期", "贷款", "首付", "月供"])
            elif wf_id == "new_car_sales":
                keywords.extend(["试驾", "新车", "全款", "看车", "报价"])
            elif wf_id == "used_car_tradein":
                keywords.extend(["二手车", "置换", "旧车", "估价"])
            if any(kw and kw in notes_text for kw in keywords):
                best_kw_wf = wf_id
                break

        model = self._get_model()
        if not model or not notes_text.strip():
            return best_kw_wf

        try:
            candidates = "\n".join(
                f"- {wf_id}: {p.name} - {p.domain_description}" for wf_id, p in packs.items()
            )
            triage_prompt = (
                f"请根据线索补充信息，从下列可用业务线中选择最匹配的一个 workflow_id (只返回 ID):\n"
                f"{candidates}\n\n线索信息: {notes_text}"
            )
            triage_agent = Agent(model=model, output_type=str, defer_model_check=True)
            res = await asyncio.wait_for(triage_agent.run(triage_prompt), timeout=3.0)
            chosen = res.output.strip()
            if chosen in packs:
                return chosen
        except Exception as err:
            logger.debug(f"[ClueSupervisor] Inflow 分诊降级使用启发式 ({best_kw_wf}): {err}")

        return best_kw_wf

    async def dry_run_workflow(
        self,
        workflow_id: str,
        conversation: List[Dict[str, Any]],
        *,
        total_call_count: int = 1,
        continuous_no_answer_count: int = 0,
        handoff_count: int = 0,
    ) -> Dict[str, Any]:
        """
        上线前 Dry-Run 预检：不写入真实数据库，预览该业务卡带的评估维度、
        PydanticAI 动态过滤后的可见工具集、4 桶记忆索引与护栏排期结果。
        """
        deps = await self.build_runtime_deps("dry_run_clue", workflow_id, dry_run=True)
        deps.handoff_count = handoff_count
        pack = deps.domain_pack

        visible_tools = await self.inspect_visible_tools(deps)
        strategy_cfg = AgentStrategyConfig(
            work_start_time=pack.work_start_time,
            work_end_time=pack.work_end_time,
            evaluation_mode=pack.evaluation_mode,
            workflow_id=pack.workflow_id,
            max_touches_per_24h=pack.max_touches_per_24h,
            domain_master_prompt=pack.domain_master_prompt,
            allowed_handoff_targets=pack.allowed_handoff_targets,
            custom_evaluators=pack.custom_evaluators,
            enabled_sub_prompts=pack.enabled_sub_evaluators,
        )
        engine = AgentDrivenStrategyEngine(config=strategy_cfg)

        now = get_now()
        mock_record = type(
            "_DryRunRecord",
            (),
            {
                "answered": 1 if conversation else 0,
                "outcome": "connected" if conversation else "no_answer",
                "duration_seconds": 60 if conversation else 0,
                "config_snapshot": {"aiCallContextList": conversation},
            },
        )()

        ctx = ClueContext(
            clue_id="dry_run_clue",
            clue_intake_time=now - timedelta(hours=1),
            call_records=[mock_record],
            call_end_time=now,
            total_call_count=total_call_count,
            continuous_no_answer_count=continuous_no_answer_count,
        )
        schedule_result = await engine.calculate(ctx)

        return {
            "workflow_id": pack.workflow_id,
            "version": pack.version,
            "name": pack.name,
            "evaluation_mode": pack.evaluation_mode,
            "working_window": f"{pack.work_start_time} - {pack.work_end_time}",
            "enabled_sub_evaluators": pack.enabled_sub_evaluators,
            "custom_evaluators": [e.name for e in pack.custom_evaluators],
            "visible_tools": visible_tools,
            "in_call_flow": pack.in_call_flow,
            "memory_index": deps.structured_memory.to_compact_index(),
            "schedule_preview": {
                "success": schedule_result.success,
                "business_status": schedule_result.business_status,
                "next_call_time": schedule_result.next_call_time.isoformat() if schedule_result.next_call_time else None,
                "priority": schedule_result.priority,
                "use_work_phone": schedule_result.use_work_phone,
                "reason": schedule_result.reason,
                "voice_agent_directive": schedule_result.voice_agent_directive,
            },
        }


clue_supervisor_agent = ClueSupervisorAgent()
