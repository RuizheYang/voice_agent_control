from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from app.agents.contracts import ClueStructuredMemory
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)


class MemoryDreamer:
    """
    异步记忆整理引擎 (Always-On Agents "Dreaming / Curation" Pass):
    在主链路结束后后台异步运行，将冗长的通话转写与多维评估结论压缩沉淀至 4 桶结构化记忆：
    - instructions: 跨轮业务与话术指引
    - customer_notes: 长期客户事实画像 (跨业务 Handoff 复用，避免重复盘问)
    - interaction_digest: 浓缩往期沟通摘要 (最多保留近 8 轮防 Context Rot)
    - progress: 当前线索进度标签
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
            updated.interaction_digest.append(digest_entry.strip()[:240])
            if len(updated.interaction_digest) > cls.MAX_DIGEST_ITEMS:
                updated.interaction_digest = updated.interaction_digest[-cls.MAX_DIGEST_ITEMS :]
        if progress:
            updated.progress = progress
        updated.last_dreamed_at = get_now().isoformat()
        return updated

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
        """落库更新 ClueInfo.agent_data['memory']"""
        from app.dbmodels import crud

        clue = await crud.get_clue_info(clue_id)
        if not clue:
            return ClueStructuredMemory()

        agent_data = dict(clue.agent_data or {})
        current = cls.load_memory(agent_data)
        updated = cls.merge_memory(
            current,
            directive=directive,
            customer_facts=customer_facts,
            digest_entry=digest_entry,
            progress=progress,
        )
        agent_data["memory"] = updated.model_dump(mode="json")
        if directive:
            agent_data["agent_directive"] = directive
        clue.agent_data = agent_data
        await clue.save(update_fields=["agent_data", "update_time"])
        logger.info(f"[MemoryDreamer] 线索 {clue_id} 四桶记忆整理完成: progress={updated.progress}")
        return updated

    @classmethod
    def schedule_background_dream(
        cls,
        clue_id: str,
        *,
        directive: Optional[str] = None,
        customer_facts: Optional[Dict[str, Any]] = None,
        digest_entry: Optional[str] = None,
        progress: Optional[str] = None,
    ) -> Optional[asyncio.Task]:
        """非阻塞触发后台 Dreaming 任务，绝不占用关键路径超时预算"""
        async def _safe_run() -> None:
            try:
                await cls.curate_and_persist(
                    clue_id,
                    directive=directive,
                    customer_facts=customer_facts,
                    digest_entry=digest_entry,
                    progress=progress,
                )
            except Exception as err:
                logger.warning(f"[MemoryDreamer] 后台记忆整理异常 (已忽略): clue_id={clue_id}, err={err}")

        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(_safe_run())
        except RuntimeError:
            return None
