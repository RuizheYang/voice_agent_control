from __future__ import annotations

import logging
import asyncio
from typing import Dict, Optional, Any

from app.strategy.contracts import BaseStrategyEngine
from app.strategy.factory import StrategyEngineFactory
from app.apis.telephony import send_call, LineType
from app.apis.voice_agent import cancel_call
from app.clue_handler.stages.inflow import get_inflow_handler_cls
from app.clue_handler.stages.postcall import get_postcall_handler_cls
from app.clue_handler.stages.schedule import get_schedule_handler_cls
from app.clue_handler.stages.cancel import get_cancel_handler_cls

logger = logging.getLogger(__name__)


class ClueHandlerFactory:
    """
    按 workflow_id 作用域装配并维护 ClueHandler 的统一工厂：
    保证同一业务线全生命周期（流入、呼叫、挂机、追呼决策）针对该线索使用确定好并直接注入的具体策略与总结引擎。
    """

    _handler_cache: Dict[str, Any] = {}
    _lock: asyncio.Lock = asyncio.Lock()

    @classmethod
    async def get_handler_by_workflow(cls, workflow_id: Optional[str]) -> Any:
        """根据 workflow_id 获取装配好具体策略与总结类的 ClueHandler 实例"""
        target_wf = workflow_id.strip() if (workflow_id and workflow_id.strip()) else "default"

        if target_wf in cls._handler_cache:
            return cls._handler_cache[target_wf]

        async with cls._lock:
            if target_wf in cls._handler_cache:
                return cls._handler_cache[target_wf]

            # 1. 查询 Workflow 配置与绑定的 Strategy
            from app.dbmodels.crud import get_workflow_config
            from app.dbmodels.models import OutboundStrategy

            strategy_id = "default"
            try:
                wf_config = await get_workflow_config(target_wf)
                if wf_config and wf_config.strategy_id:
                    strategy_id = wf_config.strategy_id
            except Exception as e:
                logger.warning(f"[ClueHandlerFactory] 获取 workflow_config '{target_wf}' 失败: {e}")

            strategy = None
            try:
                strategy = await OutboundStrategy.filter(strategy_id=strategy_id).first()
            except Exception as e:
                logger.warning(f"[ClueHandlerFactory] 获取 OutboundStrategy '{strategy_id}' 失败: {e}")

            raw_schedule_id = strategy.schedule_id if strategy and strategy.schedule_id else "agent_driven"
            schedule_id = StrategyEngineFactory.normalize_schedule_id(raw_schedule_id)
            followup = strategy.followup if strategy and isinstance(strategy.followup, dict) else {}
            raw_prompt = followup.get("summary_prompt") or followup.get("summaryPrompt")
            summary_prompt = raw_prompt.strip() if isinstance(raw_prompt, str) and raw_prompt.strip() else None

            # 2. 构建并注入 AgentDrivenStrategyEngine（支持领域卡带热覆盖）
            from app.agents.domain_registry import domain_pack_registry
            from app.strategy.agent_driven.engine import AgentDrivenStrategyEngine
            from app.strategy.agent_driven.models import AgentStrategyConfig

            domain_pack = await domain_pack_registry.get_pack(target_wf)
            agent_cfg = AgentStrategyConfig(
                strategy_prompt=summary_prompt,
                domain_master_prompt=domain_pack.domain_master_prompt,
                allowed_handoff_targets=list(domain_pack.allowed_handoff_targets),
                custom_evaluators=list(domain_pack.custom_evaluators),
                enabled_sub_prompts=list(domain_pack.enabled_sub_evaluators),
            )
            concrete_strategy = AgentDrivenStrategyEngine(config=agent_cfg)

            # 3. 组装专属 Handler：基于生命周期阶段注册表解析对应阶段类 (默认选用 default)
            from app.clue_handler.clue import ClueHandler

            inflow_cls = get_inflow_handler_cls("default")
            postcall_cls = get_postcall_handler_cls("default")
            schedule_cls = get_schedule_handler_cls("default")
            cancel_cls = get_cancel_handler_cls("default")

            new_clue_handler = inflow_cls(
                send_call_fn=send_call,
                strategy_engine=concrete_strategy,
            )
            postcall_handler = postcall_cls(
                strategy_engine=concrete_strategy,
                summary_prompt=summary_prompt,
            )
            schedule_handler = schedule_cls(
                send_call_fn=send_call,
                default_line_type=LineType.LANDLINE,
            )
            cancel_handler = cancel_cls(cancel_call_fn=cancel_call)

            handler = ClueHandler(
                new_clue_handler=new_clue_handler,
                postcall_handler=postcall_handler,
                schedule_handler=schedule_handler,
                cancel_call_handler=cancel_handler,
                workflow_id=target_wf,
            )

            cls._handler_cache[target_wf] = handler
            logger.info(
                f"[ClueHandlerFactory] 为 workflow '{target_wf}' 装配 ClueHandler 成功 "
                f"(schedule_id={schedule_id})"
            )
            return handler

    @classmethod
    async def get_handler_by_clue(cls, clue_id: str) -> Any:
        """根据线索 ID 查出 workflow_id，并获取一致的 ClueHandler"""
        from app.dbmodels.models import ClueInfo
        workflow_id = "default"
        try:
            clue = await ClueInfo.filter(clue_id=clue_id).first()
            if clue and clue.workflow_id:
                workflow_id = clue.workflow_id
        except Exception as e:
            logger.warning(f"[ClueHandlerFactory] 通过 clue_id '{clue_id}' 查找 workflow 异常: {e}")

        return await cls.get_handler_by_workflow(workflow_id)

    @classmethod
    def invalidate_workflow(cls, workflow_id: Optional[str] = None):
        """配置修改时清除对应缓存 (含领域卡带注册表热刷新)"""
        try:
            from app.agents.domain_registry import domain_pack_registry
            domain_pack_registry.invalidate(workflow_id)
        except Exception as err:
            logger.debug(f"[ClueHandlerFactory] 清除领域卡带缓存忽略: {err}")

        if workflow_id:
            cls._handler_cache.pop(workflow_id, None)
            logger.info(f"[ClueHandlerFactory] 已清除 workflow '{workflow_id}' 的 ClueHandler 缓存")
        else:
            cls._handler_cache.clear()
            logger.info(f"[ClueHandlerFactory] 已清空全部 ClueHandler 缓存")

    @classmethod
    def invalidate_all(cls):
        """清除全部缓存"""
        cls.invalidate_workflow(None)
