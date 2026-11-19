from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from tortoise.transactions import in_transaction

from app.agents.contracts import ClueStructuredMemory
from app.config import config
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)


class SemanticDreamOutput(BaseModel):
    """LLM 语义级记忆整理输出契约 (去重、冲突消解与摘要压缩)"""
    refined_customer_notes: Dict[str, Any] = Field(default_factory=dict)
    condensed_digest: List[str] = Field(default_factory=list)
    refined_instructions: Optional[str] = None


class MemoryDreamer:
    """
    异步记忆整理引擎 (Always-On Agents "Dreaming / Curation" Pass):
    1. Hot-Path 确定性合并 (`apply_to_agent_data`): 在主链路锁内零延迟同步沉淀四桶记忆，彻底杜绝并发写覆盖。
    2. Cold-Path 事务锁持久化 (`curate_and_persist`): 基于 `select_for_update()` 悲观锁原子合并，保证多协程安全。
    3. Semantic Dreaming (`dream_semantic_curation`): 在多轮累计后后台运行 LLM 语义提炼、去重与事实冲突消解。
    """

    MAX_DIGEST_ITEMS = 8

    @staticmethod
    def load_memory(agent_data: Optional[Dict[str, Any]]) -> ClueStructuredMemory:
        if not isinstance(agent_data, dict):
            return ClueStructuredMemory()
        raw = agent_data.get("memory")
        if isinstance(raw, dict):
            try:
                return ClueStructuredMemory.model_validate(raw)
            except Exception:
                pass
        return ClueStructuredMemory(
            instructions=str(agent_data.get("agent_directive") or ""),
            customer_notes={"handoff_context": agent_data["handoff_context"]}
            if agent_data.get("handoff_context")
            else {},
        )

    @classmethod
    def merge_memory(
        cls,
        current: ClueStructuredMemory,
        *,
        directive: Optional[str] = None,
        customer_facts: Optional[Dict[str, Any]] = None,
        digest_entry: Optional[str] = None,
        progress: Optional[str] = None,
    ) -> ClueStructuredMemory:
        """确定性四桶记忆合并与滚动裁剪"""
        updated = current.model_copy(deep=True)
        if directive:
            updated.instructions = directive.strip()
        if customer_facts:
            for k, v in customer_facts.items():
                if v not in (None, "", []):
                    updated.customer_notes[k] = v
        if digest_entry and digest_entry.strip():
            entry = digest_entry.strip()[:240]
            if not updated.interaction_digest or updated.interaction_digest[-1] != entry:
                updated.interaction_digest.append(entry)
            if len(updated.interaction_digest) > cls.MAX_DIGEST_ITEMS:
                updated.interaction_digest = updated.interaction_digest[-cls.MAX_DIGEST_ITEMS :]
        if progress:
            updated.progress = progress
        updated.last_dreamed_at = get_now().isoformat()
        return updated

    @classmethod
    def apply_to_agent_data(
        cls,
        agent_data: Optional[Dict[str, Any]],
        *,
        directive: Optional[str] = None,
        customer_facts: Optional[Dict[str, Any]] = None,
        digest_entry: Optional[str] = None,
        progress: Optional[str] = None,
    ) -> Dict[str, Any]:
        """在内存中同步合并四桶记忆到 agent_data 字典，供持有锁的主链路一次性原子保存"""
        merged_data = dict(agent_data or {})
        current = cls.load_memory(merged_data)
        updated = cls.merge_memory(
            current,
            directive=directive,
            customer_facts=customer_facts,
            digest_entry=digest_entry,
            progress=progress,
        )
        merged_data["memory"] = updated.model_dump(mode="json")
        if directive:
            merged_data["agent_directive"] = directive.strip()
        return merged_data

    @classmethod
    async def curate_and_persist(
        cls,
        clue_id: str,
        *,
        directive: Optional[str] = None,
        customer_facts: Optional[Dict[str, Any]] = None,
        digest_entry: Optional[str] = None,
        progress: Optional[str] = None,
    ) -> ClueStructuredMemory:
        """在数据库行级悲观锁内原子读取、合并并保存 ClueInfo.agent_data['memory']，防止并发覆盖"""
        from app.dbmodels.models import ClueInfo

        async with in_transaction() as conn:
            clue = (
                await ClueInfo.filter(clue_id=clue_id)
                .using_db(conn)
                .select_for_update()
                .first()
            )
            if not clue:
                return ClueStructuredMemory()

            agent_data = cls.apply_to_agent_data(
                clue.agent_data,
                directive=directive,
                customer_facts=customer_facts,
                digest_entry=digest_entry,
                progress=progress,
            )
            clue.agent_data = agent_data
            await clue.save(using_db=conn, update_fields=["agent_data", "update_time"])
            updated = cls.load_memory(agent_data)
            logger.info(f"[MemoryDreamer] 线索 {clue_id} 四桶记忆原子整理完成: progress={updated.progress}")
            return updated

    @classmethod
    async def dream_semantic_curation(cls, clue_id: str) -> ClueStructuredMemory:
        """
        Cold-Path LLM 语义级 Dreaming Pass:
        在持有 clue process_lock 与 DB 行锁下，利用大模型对多轮碎片化事实做去重、冲突消解与浓缩。
        若未配置模型或对话轮数不足，自动安全跳过。
        """
        from app.dbmodels import crud

        clue = await crud.get_clue_info(clue_id)
        if not clue:
            return ClueStructuredMemory()

        current = cls.load_memory(clue.agent_data)
        if len(current.interaction_digest) < 3 and len(current.customer_notes) < 3:
            return current

        try:
            from pydantic_ai import Agent
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider

            llm_cfg = config.llm_factory_config
            if not llm_cfg or not llm_cfg.base_url or not llm_cfg.api_key:
                return current

            provider = OpenAIProvider(base_url=llm_cfg.base_url, api_key=llm_cfg.api_key)
            model = OpenAIChatModel(llm_cfg.model_name or "gpt-4o-mini", provider=provider)
            dream_agent = Agent(model=model, output_type=SemanticDreamOutput, defer_model_check=True)

            prompt = (
                "你是 Always-On 外呼智能体的后台记忆整理师 (Memory Dreamer)。\n"
                "请对以下四桶记忆做去重、消解冲突（后发生事实覆盖旧矛盾事实）与浓缩：\n"
                f"- 当前进度: {current.progress}\n"
                f"- 跨轮指引: {current.instructions}\n"
                f"- 客户事实画像: {current.customer_notes}\n"
                f"- 近期纪要: {current.interaction_digest}\n"
                "要求：保留最核心的客户决策人、预算、车型、预约偏好，condensed_digest 最多保留 5 条精炼条目。"
            )
            res = await asyncio.wait_for(dream_agent.run(prompt), timeout=4.0)
            dream_out: SemanticDreamOutput = res.output

            from app.dbmodels.models import ClueInfo
            async with in_transaction() as conn:
                locked_clue = (
                    await ClueInfo.filter(clue_id=clue_id)
                    .using_db(conn)
                    .select_for_update()
                    .first()
                )
                if not locked_clue:
                    return current
                latest_mem = cls.load_memory(locked_clue.agent_data)
                if dream_out.refined_customer_notes:
                    latest_mem.customer_notes.update(dream_out.refined_customer_notes)
                if dream_out.condensed_digest:
                    latest_mem.interaction_digest = [
                        item.strip()[:240] for item in dream_out.condensed_digest[-cls.MAX_DIGEST_ITEMS:] if item.strip()
                    ]
                if dream_out.refined_instructions:
                    latest_mem.instructions = dream_out.refined_instructions.strip()
                latest_mem.last_dreamed_at = get_now().isoformat()

                agent_data = dict(locked_clue.agent_data or {})
                agent_data["memory"] = latest_mem.model_dump(mode="json")
                if latest_mem.instructions:
                    agent_data["agent_directive"] = latest_mem.instructions
                locked_clue.agent_data = agent_data
                await locked_clue.save(using_db=conn, update_fields=["agent_data", "update_time"])
                logger.info(f"[MemoryDreamer] 线索 {clue_id} 语义级 Dreaming Pass 完成")
                return latest_mem
        except Exception as err:
            logger.debug(f"[MemoryDreamer] 语义级 Dreaming Pass 降级跳过 (clue_id={clue_id}): {err}")
            return current

    @classmethod
    def schedule_background_dream(
        cls,
        clue_id: str,
        *,
        directive: Optional[str] = None,
        customer_facts: Optional[Dict[str, Any]] = None,
        digest_entry: Optional[str] = None,
        progress: Optional[str] = None,
        run_semantic: bool = False,
    ) -> Optional[asyncio.Task]:
        """非阻塞触发后台 Dreaming 任务（内部使用事务锁保证与主链路不互相覆盖）"""
        async def _safe_run() -> None:
            try:
                if any([directive, customer_facts, digest_entry, progress]):
                    await cls.curate_and_persist(
                        clue_id,
                        directive=directive,
                        customer_facts=customer_facts,
                        digest_entry=digest_entry,
                        progress=progress,
                    )
                if run_semantic:
                    await cls.dream_semantic_curation(clue_id)
            except Exception as err:
                logger.warning(f"[MemoryDreamer] 后台记忆整理异常 (已忽略): clue_id={clue_id}, err={err}")

        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(_safe_run())
        except RuntimeError:
            return None
