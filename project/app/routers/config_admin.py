import logging
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, Field

from app.schemas.strategy_config import (
    StrategyCreateRequest,
    StrategyConfigResponse,
)
from app.schemas.voice_agent_config import (
    VoiceAgentCreateRequest,
    VoiceAgentConfigResponse,
)
from app.schemas.workflow_config import (
    WorkflowCreateRequest,
    WorkflowBundleCreateRequest,
    WorkflowConfigResponse,
    WorkflowDryRunRequest,
    ControlPauseRequest,
    ControlResumeRequest,
    ControlStatusResponse,
)
from app.dbmodels.crud import (
    get_strategy_config,
    save_strategy_config,
    get_voice_agent_config,
    get_voice_agent_config_by_id,
    save_voice_agent_config,
    get_workflow_config,
    get_workflow_domain_config,
    save_workflow_config,
    provision_workflow_bundle,
    get_app_auth,
    save_app_auth,
)
from app.clue_handler.factory import ClueHandlerFactory
from app.redis_client.redis_manager import redis_manager
from app.utils.workers import is_inflow_paused, is_schedule_paused
from app.call_schedule.control import set_service_pause
from app.utils.exceptions import WorkflowAgentPairingError

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["config_admin"],
    responses={404: {"description": "Not found"}},
)

class AppAuthRegisterRequest(BaseModel):
    app_id: str = Field(..., description="外部应用标识")
    app_secret: str = Field(..., description="签名密钥")
    app_name: Optional[str] = Field(None, description="应用名称")
    allowed_workflows: Optional[List[str]] = Field(default_factory=lambda: ["*"], description="允许的 workflow_id 列表")

# ==================== 1. Voice Agent 配置管理 ====================

@router.post("/voice-agent/create", response_model=VoiceAgentConfigResponse, summary="创建或更新 Voice Agent")
@router.post("/admin/voice-agent/create", response_model=VoiceAgentConfigResponse, include_in_schema=False)
async def create_voice_agent(request: VoiceAgentCreateRequest) -> VoiceAgentConfigResponse:
    """创建或更新 Voice Agent 配置 (传入 agent_id 则更新/创建，未传入则自动生成 agent_id)"""
    logger.info(f"创建/更新 Voice Agent: agent_id={request.agent_id}, agent_name={request.agent_name}")
    try:
        saved = await save_voice_agent_config(request.model_dump())
        return VoiceAgentConfigResponse(
            code=0,
            msg="success",
            data={
                "agent_id": saved.agent_id,
                "agent_name": saved.agent_name,
                "version": saved.version,
                "description": saved.description,
                "global_settings": saved.globalSettings,
                "agent_type": saved.agent_type,
                "open_message": saved.open_message,
                "call_type": saved.call_type,
                "create_time": saved.create_time.isoformat() if saved.create_time else None,
            }
        )
    except Exception as e:
        logger.error(f"创建/更新 Voice Agent 异常: {e}", exc_info=True)
        return VoiceAgentConfigResponse(code=3, msg=f"创建 Voice Agent 失败: {str(e)}", data=None)

@router.get("/voice-agent/{agent_identifier}", response_model=VoiceAgentConfigResponse, summary="查询 Voice Agent 配置")
@router.get("/admin/voice-agent/{agent_identifier}", response_model=VoiceAgentConfigResponse, include_in_schema=False)
async def get_voice_agent(
    agent_identifier: str = Path(..., description="Agent 唯一标识 (agent_id) 或显示名称 (agent_name)"),
    version: Optional[str] = Query(None, description="版本标识")
) -> VoiceAgentConfigResponse:
    """查询 Voice Agent 配置"""
    config_record = await get_voice_agent_config(agent_identifier, version)
    if not config_record:
        return VoiceAgentConfigResponse(code=3, msg=f"Voice Agent '{agent_identifier}' 未找到", data=None)
    return VoiceAgentConfigResponse(
        code=0,
        msg="success",
        data={
            "agent_id": config_record.agent_id,
            "agent_name": config_record.agent_name,
            "version": config_record.version,
            "description": config_record.description,
            "global_settings": config_record.globalSettings,
            "agent_type": config_record.agent_type,
            "open_message": config_record.open_message,
            "call_type": config_record.call_type,
            "create_time": config_record.create_time.isoformat() if config_record.create_time else None,
        }
    )

# ==================== 2. 外呼跟进策略配置管理 ====================

@router.post("/strategy/create", response_model=StrategyConfigResponse, summary="创建或更新外呼跟进策略")
@router.post("/admin/strategy/create", response_model=StrategyConfigResponse, include_in_schema=False)
async def create_strategy(request: StrategyCreateRequest) -> StrategyConfigResponse:
    """创建或更新外呼策略配置 (绑定内置 schedule_id 追呼排期方案)"""
    logger.info("创建/更新外呼策略: strategy_id=%r, schedule_id=%r, summary_type=%r, has_summary_prompt=%s",
                request.strategy_id, request.schedule_id, request.followup.summary_type, bool(request.followup.summary_prompt))
    try:
        saved = await save_strategy_config(request.model_dump())
        ClueHandlerFactory.invalidate_all()
        return StrategyConfigResponse(
            code=0,
            msg="success",
            data={
                "strategy_id": saved.strategy_id,
                "schedule_id": saved.schedule_id,
                "name": saved.name,
                "followup": saved.followup,
                "sip": saved.sip,
                "webhook": saved.webhook,
                "create_time": saved.create_time.isoformat() if saved.create_time else None,
            }
        )
    except Exception as e:
        logger.error(f"创建/更新外呼策略异常: {e}", exc_info=True)
        return StrategyConfigResponse(code=3, msg=f"创建外呼策略失败: {str(e)}", data=None)

@router.get("/strategy/{strategy_id}", response_model=StrategyConfigResponse, summary="查询外呼跟进策略")
@router.get("/admin/strategy/{strategy_id}", response_model=StrategyConfigResponse, include_in_schema=False)
async def get_strategy(strategy_id: str = Path(..., description="外呼策略ID")) -> StrategyConfigResponse:
    """查询外呼策略配置"""
    config_record = await get_strategy_config(strategy_id)
    if not config_record:
        return StrategyConfigResponse(code=3, msg=f"Strategy '{strategy_id}' 未找到", data=None)
    return StrategyConfigResponse(
        code=0,
        msg="success",
        data={
            "strategy_id": config_record.strategy_id,
            "schedule_id": config_record.schedule_id,
            "name": config_record.name,
            "followup": config_record.followup,
            "sip": config_record.sip,
            "webhook": config_record.webhook,
            "create_time": config_record.create_time.isoformat() if config_record.create_time else None,
        }
    )

# ==================== 3. 业务线 Workflow 绑定与一站式聚合管理 ====================

@router.post("/workflow/bundle/create", response_model=WorkflowConfigResponse, summary="一站式创建或更新业务线配置包")
@router.post("/admin/workflow/bundle/create", response_model=WorkflowConfigResponse, include_in_schema=False)
async def create_workflow_bundle(request: WorkflowBundleCreateRequest) -> WorkflowConfigResponse:
    """
    一站式业务线配置包聚合创建/更新 (Facade 模式)：
    支持在单个接口调用中同时传入 workflow_id 基础信息、Voice Agent 完整配置与外呼跟进策略配置。
    若传入嵌套 voice_agent/strategy 则自动创建对应实体并绑定；也可传入已有的 agent_id/strategy_id 复用。
    """
    logger.info(f"一站式创建/更新业务线配置包: workflow_id={request.workflow_id}")
    try:
        if request.domain_config is not None:
            from app.agents.contracts import BusinessDomainPack
            cfg = dict(request.domain_config)
            cfg.setdefault("workflow_id", request.workflow_id)
            BusinessDomainPack.model_validate(cfg)
        data = await provision_workflow_bundle(request.model_dump())
        ClueHandlerFactory.invalidate_workflow(request.workflow_id)
        return WorkflowConfigResponse(
            code=0,
            msg="success",
            data=data
        )
    except WorkflowAgentPairingError as e:
        logger.warning(f"一站式创建业务线配套缺失: {e}")
        return WorkflowConfigResponse(code=3, msg=str(e), data=None)
    except ValueError as e:
        logger.warning(f"一站式创建业务线配置校验失败: {e}")
        return WorkflowConfigResponse(code=3, msg=str(e), data=None)
    except Exception as e:
        logger.error(f"一站式创建业务线配置异常: {e}", exc_info=True)
        return WorkflowConfigResponse(code=3, msg=f"创建业务线配置失败: {str(e)}", data=None)


@router.post("/workflow/create", response_model=WorkflowConfigResponse, summary="创建或更新业务线 Workflow 绑定")
@router.post("/admin/workflow/create", response_model=WorkflowConfigResponse, include_in_schema=False)
async def create_workflow(request: WorkflowCreateRequest) -> WorkflowConfigResponse:
    """
    创建或更新业务流程与默认策略、Agent的绑定配置：
    - 若传入 voice_agent 或 strategy 嵌套对象，自动切换为一站式聚合创建
    - 若仅传入 agent_id 与 strategy_id，则直接执行关联绑定
    """
    logger.info(f"创建/更新 Workflow: {request.workflow_id} (agent_id={request.agent_id}, strategy_id={request.strategy_id})")
    try:
        if request.domain_config is not None:
            from app.agents.contracts import BusinessDomainPack
            cfg = dict(request.domain_config)
            cfg.setdefault("workflow_id", request.workflow_id)
            BusinessDomainPack.model_validate(cfg)
        data = await provision_workflow_bundle(request.model_dump())
        ClueHandlerFactory.invalidate_workflow(request.workflow_id)
        return WorkflowConfigResponse(
            code=0,
            msg="success",
            data=data
        )
    except WorkflowAgentPairingError as e:
        logger.warning(f"创建 Workflow 配套缺失: {e}")
        return WorkflowConfigResponse(code=3, msg=str(e), data=None)
    except ValueError as e:
        logger.warning(f"创建 Workflow 参数错误: {e}")
        return WorkflowConfigResponse(code=3, msg=str(e), data=None)
    except Exception as e:
        logger.error(f"创建 Workflow 异常: {e}", exc_info=True)
        return WorkflowConfigResponse(code=3, msg=f"创建业务线配置失败: {str(e)}", data=None)


@router.get("/workflow/{workflow_id}", response_model=WorkflowConfigResponse, summary="查询业务线 Workflow 配置")
@router.get("/admin/workflow/{workflow_id}", response_model=WorkflowConfigResponse, include_in_schema=False)
async def get_workflow(workflow_id: str = Path(..., description="业务线流程标识")) -> WorkflowConfigResponse:
    """查询业务线 Workflow 配置 (附带关联的 Agent 和策略详情快照)"""
    config_record = await get_workflow_config(workflow_id)
    if not config_record:
        return WorkflowConfigResponse(code=3, msg=f"Workflow '{workflow_id}' 未找到", data=None)

    saved_agent = await get_voice_agent_config_by_id(config_record.agent_id)
    if not saved_agent:
        saved_agent = await get_voice_agent_config(config_record.agent_id)
    saved_strategy = await get_strategy_config(config_record.strategy_id)

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

    saved_domain = None
    try:
        saved_domain = await get_workflow_domain_config(workflow_id)
    except Exception:
        pass

    return WorkflowConfigResponse(
        code=0,
        msg="success",
        data={
            "workflow_id": config_record.workflow_id,
            "name": config_record.name,
            "agent_id": config_record.agent_id,
            "strategy_id": config_record.strategy_id,
            "version": config_record.version,
            "description": config_record.description,
            "is_active": config_record.is_active,
            "create_time": config_record.create_time.isoformat() if config_record.create_time else None,
            "voice_agent": agent_dict,
            "strategy": strategy_dict,
            "domain_config": saved_domain.domain_config if saved_domain else None,
        }
    )


@router.post("/workflow/{workflow_id}/dry-run", response_model=WorkflowConfigResponse, summary="业务线卡带模拟预检 (Dry-Run)")
@router.post("/admin/workflow/{workflow_id}/dry-run", response_model=WorkflowConfigResponse, include_in_schema=False)
async def dry_run_workflow(
    workflow_id: str = Path(..., description="业务线流程标识"),
    request: WorkflowDryRunRequest = ...,
) -> WorkflowConfigResponse:
    """上线前模拟预检：验证卡带的声明式评估器、可见工具集与护栏排期结果，不写入真实业务库"""
    try:
        from app.agents.supervisor import clue_supervisor_agent
        preview = await clue_supervisor_agent.dry_run_workflow(
            workflow_id=workflow_id,
            conversation=request.conversation,
            total_call_count=request.total_call_count,
            continuous_no_answer_count=request.continuous_no_answer_count,
            handoff_count=request.handoff_count,
        )
        return WorkflowConfigResponse(code=0, msg="success", data=preview)
    except Exception as e:
        logger.error(f"Dry-Run 预检失败 (workflow_id={workflow_id}): {e}", exc_info=True)
        return WorkflowConfigResponse(code=3, msg=f"Dry-Run 预检失败: {str(e)}", data=None)

# ==================== 4. 运行控制 (暂停 / 恢复) ====================

@router.post("/control/pause", response_model=ControlStatusResponse, summary="系统运行暂停开关")
@router.post("/admin/control/pause", response_model=ControlStatusResponse, include_in_schema=False)
async def control_pause(request: ControlPauseRequest):
    """
    暂停服务调度：
    1. scope='inflow': 暂停服务内部新线索流入消费 worker
    2. scope='schedule': 暂停服务内部次呼到期排期 worker
    3. scope='all': 暂停上述全部
    """
    logger.info("[Control] 收到暂停指令: scope=%s, reason=%s", request.scope, request.reason)
    if request.scope in ["inflow", "all"]:
        await redis_manager.set_json("system:pause:inflow", {"paused": True, "reason": request.reason})
    if request.scope in ["schedule", "all"]:
        await redis_manager.set_json("system:pause:schedule", {"paused": True, "reason": request.reason})

    try:
        await set_service_pause(request.scope, True, request.reason)
    except Exception as e:
        logger.warning(f"[Control] 同步持久化暂停状态失败 (可能未初始化DB): {e}")

    return ControlStatusResponse(
        code=0,
        msg="paused",
        data={
            "inflow_paused": await is_inflow_paused(),
            "schedule_paused": await is_schedule_paused(),
        }
    )

@router.post("/control/resume", response_model=ControlStatusResponse, summary="系统运行恢复开关")
@router.post("/admin/control/resume", response_model=ControlStatusResponse, include_in_schema=False)
async def control_resume(request: ControlResumeRequest):
    """
    恢复服务调度：
    1. scope='inflow': 恢复新线索流入消费
    2. scope='schedule': 恢复次呼到期排期
    3. scope='all': 恢复全部
    """
    logger.info(f"[Control] 收到恢复指令: scope={request.scope}")
    if request.scope in ["inflow", "all"]:
        await redis_manager.delete("system:pause:inflow")
    if request.scope in ["schedule", "all"]:
        await redis_manager.delete("system:pause:schedule")

    try:
        await set_service_pause(request.scope, False)
    except Exception as e:
        logger.warning(f"[Control] 同步持久化恢复状态失败 (可能未初始化DB): {e}")

    return ControlStatusResponse(
        code=0,
        msg="resumed",
        data={
            "inflow_paused": await is_inflow_paused(),
            "schedule_paused": await is_schedule_paused(),
        }
    )

@router.get("/control/status", response_model=ControlStatusResponse, summary="查询系统当前暂停状态")
@router.get("/admin/control/status", response_model=ControlStatusResponse, include_in_schema=False)
async def control_status():
    """查询流入和次呼排期的暂停状态"""
    return ControlStatusResponse(
        code=0,
        msg="success",
        data={
            "inflow_paused": await is_inflow_paused(),
            "schedule_paused": await is_schedule_paused(),
        }
    )

# ==================== 5. App 鉴权注册 ====================

@router.post("/auth/register", summary="注册应用 AppId 与 Secret (运维管理)")
@router.post("/admin/auth/register", summary="注册应用 AppId 与 Secret (运维管理)", include_in_schema=False)
async def register_app_auth(request: AppAuthRegisterRequest):
    """注册或更新 App 签名鉴权信息"""
    logger.info(f"注册/更新 App 授权: {request.app_id}")
    saved = await save_app_auth(
        app_id=request.app_id,
        app_secret=request.app_secret,
        allowed_workflows=request.allowed_workflows,
        app_name=request.app_name,
    )
    return {
        "code": 0,
        "msg": "success",
        "data": {
            "app_id": saved.app_id,
            "app_name": saved.app_name,
            "allowed_workflows": saved.allowed_workflows,
        }
    }
