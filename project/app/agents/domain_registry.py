from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional
import yaml

from app.agents.contracts import BusinessDomainPack, DEFAULT_BASE_TOOLS

logger = logging.getLogger(__name__)

DOMAINS_DIR = Path(__file__).resolve().parent.parent / "domains"


class DomainPackRegistry:
    """
    多业务领域卡带注册中心 (Cartridge Registry):
    优先级顺序 (从高到低):
    1. 数据库热发布配置 (WorkflowDomainConfig)
    2. 运行时动态注册包 / 本地 GitOps YAML 卡带 (app/domains/*.yaml)
    3. 基于 WorkflowBinding 的默认兜底卡带
    """

    def __init__(self, domains_dir: Optional[Path] = None):
        self._domains_dir = domains_dir or DOMAINS_DIR
        self._yaml_packs: Dict[str, BusinessDomainPack] = {}
        self._db_cache: Dict[str, BusinessDomainPack] = {}
        self._loaded_yaml = False

    def load_yaml_cartridges(self, force: bool = False) -> Dict[str, BusinessDomainPack]:
        """扫描并校验本地 YAML 业务卡带目录"""
        if self._loaded_yaml and not force:
            return self._yaml_packs

        loaded: Dict[str, BusinessDomainPack] = {}
        if self._domains_dir.exists() and self._domains_dir.is_dir():
            for path in sorted(self._domains_dir.glob("*.y*ml")):
                try:
                    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
                    if isinstance(raw, dict):
                        pack = BusinessDomainPack.model_validate(raw)
                        loaded[pack.workflow_id] = pack
                        logger.info(
                            f"[DomainPackRegistry] 已加载 YAML 业务卡带: "
                            f"workflow_id={pack.workflow_id}, version={pack.version}"
                        )
                except Exception as err:
                    logger.error(f"[DomainPackRegistry] 解析 YAML 卡带失败 '{path}': {err}")

        self._yaml_packs = loaded
        self._loaded_yaml = True
        return self._yaml_packs

    def register_pack(self, pack: BusinessDomainPack) -> None:
        """代码级或测试动态注册/热替换业务卡带"""
        self._db_cache[pack.workflow_id] = pack
        logger.info(f"[DomainPackRegistry] 动态注册业务卡带: {pack.workflow_id} ({pack.version})")

    def invalidate(self, workflow_id: Optional[str] = None) -> None:
        """原子清除缓存，触发下次访问从 DB/YAML 重新热装配"""
        if workflow_id:
            self._db_cache.pop(workflow_id, None)
        else:
            self._db_cache.clear()
            self._loaded_yaml = False

    async def get_pack(self, workflow_id: Optional[str]) -> BusinessDomainPack:
        """按优先级获取业务领域卡带 (DB > YAML/内存注册 > 默认兜底)"""
        wf_id = workflow_id.strip() if (workflow_id and workflow_id.strip()) else "default"
        self.load_yaml_cartridges()

        # 1. 优先命中已缓存的热更新包
        if wf_id in self._db_cache:
            return self._db_cache[wf_id]

        # 2. 查询数据库 WorkflowDomainConfig (DB 优先覆盖 YAML)
        try:
            from app.dbmodels.crud import get_workflow_domain_config, get_workflow_config

            db_domain = await get_workflow_domain_config(wf_id)
            if db_domain and isinstance(db_domain.domain_config, dict) and db_domain.domain_config:
                cfg_dict = dict(db_domain.domain_config)
                cfg_dict.setdefault("workflow_id", wf_id)
                cfg_dict.setdefault("version", db_domain.version or "v1.0")
                pack = BusinessDomainPack.model_validate(cfg_dict)
                self._db_cache[wf_id] = pack
                return pack

            # 3. 若 DB 无扩展领域配置，检查 YAML 卡带
            if wf_id in self._yaml_packs:
                return self._yaml_packs[wf_id]

            # 4. 从存量 WorkflowBinding 合成标准默认卡带，保障 100% 向后兼容
            binding = await get_workflow_config(wf_id)
            if binding:
                return BusinessDomainPack(
                    workflow_id=wf_id,
                    name=binding.name or wf_id,
                    version=binding.version or "v1.0",
                    domain_description=binding.description or f"业务流程 {wf_id}",
                    voice_agent_id=binding.agent_id or "default",
                    strategy_id=binding.strategy_id or "default",
                    allowed_tools=list(DEFAULT_BASE_TOOLS),
                )
        except Exception as err:
            logger.debug(f"[DomainPackRegistry] 查询数据库领域配置降级 (workflow_id={wf_id}): {err}")
            if wf_id in self._yaml_packs:
                return self._yaml_packs[wf_id]

        # 5. 最终安全兜底卡带
        return BusinessDomainPack(
            workflow_id=wf_id,
            name=f"默认业务 ({wf_id})",
            domain_description="通用外呼跟进业务线",
            allowed_tools=list(DEFAULT_BASE_TOOLS),
        )

    async def list_all_packs(self) -> Dict[str, BusinessDomainPack]:
        """汇总当前可用业务卡带 (用于 Supervisor 路由和 Triage 分诊)"""
        self.load_yaml_cartridges()
        merged: Dict[str, BusinessDomainPack] = dict(self._yaml_packs)
        merged.update(self._db_cache)

        try:
            from app.dbmodels.models import WorkflowDomainConfig

            records = await WorkflowDomainConfig.filter(is_active=True).all()
            for rec in records:
                if isinstance(rec.domain_config, dict):
                    cfg = dict(rec.domain_config)
                    cfg.setdefault("workflow_id", rec.workflow_id)
                    merged[rec.workflow_id] = BusinessDomainPack.model_validate(cfg)
        except Exception as err:
            logger.debug(f"[DomainPackRegistry] 遍历 DB 领域卡带忽略: {err}")

        return merged

    async def get_target_descriptions(self, target_ids: List[str]) -> Dict[str, str]:
        """获取可切换下游目标的语义描述映射"""
        if not target_ids:
            return {}
        all_packs = await self.list_all_packs()
        descriptions: Dict[str, str] = {}
        for tid in target_ids:
            if tid in all_packs:
                descriptions[tid] = all_packs[tid].domain_description or all_packs[tid].name or tid
            else:
                pack = await self.get_pack(tid)
                descriptions[tid] = pack.domain_description or pack.name or tid
        return descriptions


# 全局单例注册中心
domain_pack_registry = DomainPackRegistry()
