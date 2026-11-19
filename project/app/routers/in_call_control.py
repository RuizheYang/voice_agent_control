from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Path
from pydantic import BaseModel, Field

from app.agents.domain_registry import domain_pack_registry
from app.agents.memory_dreamer import MemoryDreamer
from app.agents.supervisor import clue_supervisor_agent
from app.agents.tool_bus import execute_tool_by_name
from app.apis.telephony import render_dynamic_agent_context
from app.dbmodels import crud

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/clue/in-call",
    tags=["in_call_realtime"],
    responses={404: {"description": "Not found"}},
)


class InCallToolInvokeRequest(BaseModel):
    tool_name: str = Field(..., min_length=1, description="调用的工具名称 (须在当前卡带 allowed_tools 白名单内)")
    tool_args: Dict[str, Any] = Field(default_factory=dict, description="工具入参")
    reason: str = Field(default="", description="本次通话中调用工具的上下文原因")
    dry_run: bool = Field(default=False, description="是否模拟演练")


class InCallHandoffRequest(BaseModel):
    target_workflow_id: str = Field(..., min_length=1, description="目标业务线 workflow_id")
    handoff_summary: str = Field(..., min_length=1, description="跨业务交接背景摘要")


@router.get("/{clue_id}/bootstrap", summary="通话中实时加载线索记忆、业务卡带、Flow 节点与工具契约")
async def in_call_bootstrap(clue_id: str = Path(..., description="线索唯一 ID")):
    """
    LiveKit Agents / Pipecat Bot 接通瞬间调用：
    返回脱敏线索上下文、四桶结构化记忆索引、动态渲染指令、in_call_flow 节点及当前卡带可见工具清单。
    """
    clue = await crud.get_clue_info(clue_id)
    if not clue:
        raise HTTPException(status_code=404, detail={"code": 40004, "msg": f"线索 {clue_id} 不存在"})

    deps = await clue_supervisor_agent.build_runtime_deps(clue_id, workflow_id=clue.workflow_id)
    pack = deps.domain_pack
    visible_tools = await clue_supervisor_agent.inspect_visible_tools(deps)

    voice_agent = await crud.get_voice_agent_config_by_id(clue.agent_id)
    agent_config = (
        {
            "agent_id": voice_agent.agent_id,
            "agent_name": voice_agent.agent_name,
            "version": voice_agent.version,
            "instruction": voice_agent.instruction,
            "open_message": voice_agent.open_message,
            "global_settings": voice_agent.globalSettings,
        }
        if voice_agent
        else {"agent_id": clue.agent_id, "instruction": pack.domain_master_prompt or ""}
    )
    rendered = render_dynamic_agent_context(agent_config, clue.agent_data)

    return {
        "code": 0,
        "msg": "success",
        "data": {
            "clue_id": clue.clue_id,
            "status": clue.status,
            "workflow_id": pack.workflow_id,
            "workflow_name": pack.name,
            "agent_id": clue.agent_id,
            "strategy_id": clue.strategy_id,
            "domain_description": pack.domain_description,
            "domain_master_prompt": pack.domain_master_prompt,
            "in_call_flow": pack.in_call_flow,
            "structured_memory": deps.structured_memory.model_dump(mode="json"),
            "memory_index": rendered["memoryIndex"],
            "agent_directive": rendered["agentDirective"],
            "dynamic_instruction": rendered["dynamicInstruction"],
            "open_message": rendered["openMessage"],
            "allowed_handoff_targets": pack.allowed_handoff_targets,
            "handoff_count": deps.handoff_count,
            "max_handoffs": pack.max_handoffs,
            "visible_tools": visible_tools,
        },
    }


@router.post("/{clue_id}/tool-invoke", summary="通话中安全代理执行业务卡带工具 (CredentialProxy + 白名单)")
async def in_call_tool_invoke(
    clue_id: str = Path(..., description="线索唯一 ID"),
    request: InCallToolInvokeRequest = ...,
):
    """
    供 LiveKit / Pipecat 实时语音 Agent 在通话进行中安全调用外部 HTTP 工具或调度工具，
    密钥由 CredentialProxy 运行时注入，绝不暴露给媒体面 LLM。
    """
    clue = await crud.get_clue_info(clue_id)
    if not clue:
        raise HTTPException(status_code=404, detail={"code": 40004, "msg": f"线索 {clue_id} 不存在"})

    result = await execute_tool_by_name(
        clue_id=clue_id,
        tool_name=request.tool_name,
        tool_args=request.tool_args,
        workflow_id=clue.workflow_id,
        dry_run=request.dry_run,
        reason=request.reason,
    )
    return {"code": 0 if result.get("ok") else 40003, "msg": result.get("status", "ok"), "data": result}


@router.post("/{clue_id}/handoff", summary="通话中不断线实时跨业务热切换 (LiveKit / Pipecat Mid-Call Handoff)")
async def in_call_realtime_handoff(
    clue_id: str = Path(..., description="线索唯一 ID"),
    request: InCallHandoffRequest = ...,
):
    """
    通话中客户诉求跨业务切换时调用：
    1. 校验目标业务是否在当前卡带 allowed_handoff_targets 白名单与 max_handoffs 内；
    2. 原子切换 ClueInfo 业务归属、作废旧业务在途任务、继承四桶记忆并刷新 clue_config；
    3. 立即返回目标业务卡带的最新指令、Flow 节点与工具清单，供语音 Session 实时换装。
    """
    clue = await crud.get_clue_info(clue_id)
    if not clue:
        raise HTTPException(status_code=404, detail={"code": 40004, "msg": f"线索 {clue_id} 不存在"})

    cur_pack = await domain_pack_registry.get_pack(clue.workflow_id)
    if request.target_workflow_id not in (cur_pack.allowed_handoff_targets or []):
        return {
            "code": 40003,
            "msg": f"目标业务 '{request.target_workflow_id}' 不在允许切换白名单 {cur_pack.allowed_handoff_targets} 内",
            "data": {"success": False},
        }

    target_pack = await domain_pack_registry.get_pack(request.target_workflow_id)
    handoff_res = await crud.atomic_handoff_clue_workflow(
        clue_id=clue_id,
        target_workflow_id=request.target_workflow_id,
        target_agent_id=target_pack.voice_agent_id,
        target_strategy_id=target_pack.strategy_id,
        handoff_summary=request.handoff_summary,
        max_handoffs=cur_pack.max_handoffs,
    )
    if not handoff_res.get("success"):
        return {
            "code": 40003,
            "msg": handoff_res.get("reason", "handoff_rejected"),
            "data": handoff_res,
        }

    # 同步将交接摘要沉淀入四桶记忆
    await MemoryDreamer.curate_and_persist(
        clue_id,
        directive=f"[通话中热切换 {handoff_res.get('from_workflow')}->{request.target_workflow_id}] {request.handoff_summary}",
        digest_entry=f"[Mid-Call Handoff] {request.handoff_summary}",
        progress=f"in_call_handoff:{request.target_workflow_id}",
    )

    new_deps = await clue_supervisor_agent.build_runtime_deps(
        clue_id, workflow_id=request.target_workflow_id
    )
    new_tools = await clue_supervisor_agent.inspect_visible_tools(new_deps)

    return {
        "code": 0,
        "msg": "success",
        "data": {
            "success": True,
            "from_workflow": handoff_res.get("from_workflow"),
            "to_workflow": request.target_workflow_id,
            "handoff_count": handoff_res.get("handoff_count"),
            "target_pack": {
                "workflow_id": target_pack.workflow_id,
                "name": target_pack.name,
                "voice_agent_id": target_pack.voice_agent_id,
                "strategy_id": target_pack.strategy_id,
                "domain_description": target_pack.domain_description,
                "domain_master_prompt": target_pack.domain_master_prompt,
                "in_call_flow": target_pack.in_call_flow,
            },
            "memory_index": new_deps.structured_memory.to_compact_index(),
            "visible_tools": new_tools,
        },
    }
