from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional

from pydantic_ai import Agent, RunContext

from app.agents.contracts import (
    AgentRuntimeDeps,
    BusinessDomainPack,
    ClueStructuredMemory,
    SupervisorActionPlan,
)
from app.agents.domain_registry import DomainPackRegistry, domain_pack_registry
from app.agents.memory_dreamer import MemoryDreamer
from app.agents.tool_bus import build_domain_toolset
from app.config import config
from app.strategy.agent_driven.engine import AgentDrivenStrategyEngine
from app.strategy.agent_driven.models import AgentStrategyConfig
from app.strategy.contracts.context import ClueContext
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)


class ClueSupervisorAgent:
    """
    全局无状态单例 Multi-Agent 编排中枢 (Stateless Supervisor):
    通过 PydanticAI RunContext[AgentRuntimeDeps] + 动态组合 Toolsets 实现按线索瞬时换装，
    支持：
    1. 线索流入自动分诊 (Inflow Triage for workflowId='auto')
    2. 话后/唤醒周期内业务多专家决策与跨业务 Handoff 协调
    3. 上线前 Dry-Run 预检 (评估器/可见工具/护栏预览)
    4. 严格 5.0s 超时控制与确定性降级
    """

    def __init__(self, registry: Optional[DomainPackRegistry] = None, timeout_seconds: float = 5.0):
        self.registry = registry or domain_pack_registry
        self.timeout_seconds = timeout_seconds
        self._agent: Optional[Agent[AgentRuntimeDeps, SupervisorActionPlan]] = None

    def _get_model(self):
        try:
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider

            llm_cfg = config.llm_factory_config
            if not llm_cfg or not llm_cfg.base_url:
                return None
            provider = OpenAIProvider(base_url=llm_cfg.base_url, api_key=llm_cfg.api_key or "sk-dummy")
            return OpenAIChatModel(llm_cfg.model_name or "default-model", provider=provider)
        except Exception as err:
            logger.debug(f"[ClueSupervisor] 初始化模型跳过: {err}")
            return None

    def get_stateless_agent(self) -> Optional[Agent[AgentRuntimeDeps, SupervisorActionPlan]]:
        """获取全局唯一的无状态 PydanticAI Agent 单例"""
        model = self._get_model()
        if not model:
            return None
        if self._agent is None:
            agent: Agent[AgentRuntimeDeps, SupervisorActionPlan] = Agent(
                model=model,
                deps_type=AgentRuntimeDeps,
                output_type=SupervisorActionPlan,
                defer_model_check=True,
            )

            @agent.instructions
            def _inject_domain_cartridge_instructions(ctx: RunContext[AgentRuntimeDeps]) -> str:
                pack = ctx.deps.domain_pack
                mem_idx = ctx.deps.structured_memory.to_compact_index()
                return (
                    f"你当前正在以【{pack.name or pack.workflow_id}】(workflow_id={pack.workflow_id}) 身份处理线索 {ctx.deps.clue_id}。\n"
                    f"业务职责范围: {pack.domain_description}\n"
                    f"业务专属准则: {pack.domain_master_prompt or '遵循标准转化与合规防骚扰准则。'}\n"
                    f"结构化记忆索引: {mem_idx}\n"
                    f"全局 24h 已触达次数: {ctx.deps.recent_24h_touches}/{pack.max_touches_per_24h}；"
                    f"已跨业务切换次数: {ctx.deps.handoff_count}/{pack.max_handoffs}。"
                )

            self._agent = agent
        return self._agent

    async def build_runtime_deps(
        self,
        clue_id: str,
        workflow_id: Optional[str] = None,
        *,
        dry_run: bool = False,
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

        # 快速关键词启发式兜底路由
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
            "enabled_sub_evaluators": pack.enabled_sub_evaluators,
            "custom_evaluators": [e.name for e in pack.custom_evaluators],
            "visible_tools": visible_tools,
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
