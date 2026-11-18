from __future__ import annotations

import hashlib
import inspect
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Dict, List, Literal, Optional

import httpx
from pydantic import Field
from pydantic_ai import RunContext
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets import AbstractToolset, CombinedToolset, FunctionToolset

from app.agents.contracts import (
    AgentRuntimeDeps,
    BusinessDomainPack,
    DeclarativeHttpToolSpec,
    HandoffResult,
)
from app.strategy.agent_driven.models import (
    AgentStrategyConfig,
    MasterSummaryDecision,
    ScheduleActionType,
    StandardBusinessStatus,
)
from app.strategy.agent_driven.harness import PolicyHarness
from app.utils.time_utils import ensure_tz_aware, get_now

logger = logging.getLogger(__name__)

_TYPE_MAP: Dict[str, Any] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
}


# ==============================================================================
# 1. Credential-Injecting Proxy (凭证注入代理，密钥绝不进入 LLM 上下文)
# ==============================================================================

class CredentialProxy:
    """凭证注入代理：仅在工具执行发往外部服务瞬间动态注入鉴权头"""

    def __init__(self) -> None:
        self._secrets: Dict[str, Dict[str, str]] = {}

    def register_secret(self, credential_ref: str, headers: Dict[str, str]) -> None:
        self._secrets[credential_ref] = dict(headers)

    def resolve_headers(self, credential_ref: Optional[str]) -> Dict[str, str]:
        if not credential_ref:
            return {}
        if credential_ref in self._secrets:
            return dict(self._secrets[credential_ref])
        env_key = f"AGENT_CREDENTIAL_{credential_ref.upper()}"
        token = os.getenv(env_key, "")
        if token:
            return {"Authorization": f"Bearer {token}"}
        return {}


credential_proxy = CredentialProxy()


# ==============================================================================
# 2. 零代码声明式 HTTP 工具工厂 (Declarative HTTP Tool Generator)
# ==============================================================================

class DeclarativeHttpToolFactory:
    """将业务卡带中的 declarative_http_tools 动态转换为 PydanticAI FunctionToolset"""

    @staticmethod
    def build_toolset(
        specs: List[DeclarativeHttpToolSpec],
        proxy: Optional[CredentialProxy] = None,
    ) -> FunctionToolset[AgentRuntimeDeps]:
        active_proxy = proxy or credential_proxy
        toolset: FunctionToolset[AgentRuntimeDeps] = FunctionToolset(id="declarative_http_tools")

        for spec in specs:
            tool_fn = DeclarativeHttpToolFactory._create_http_tool_fn(spec, active_proxy)
            toolset.add_function(tool_fn, name=spec.name, description=spec.description, takes_ctx=True)

        return toolset

    @staticmethod
    def _create_http_tool_fn(spec: DeclarativeHttpToolSpec, proxy: CredentialProxy):
        params: List[inspect.Parameter] = [
            inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=RunContext[AgentRuntimeDeps])
        ]
        annotations: Dict[str, Any] = {
            "ctx": RunContext[AgentRuntimeDeps],
            "return": Dict[str, Any],
        }

        for pname, pspec in spec.parameters.items():
            py_type = _TYPE_MAP.get(pspec.type, str)
            annotated_type = Annotated[py_type, Field(description=pspec.description or pname)]
            annotations[pname] = annotated_type
            default = inspect.Parameter.empty if pspec.required else pspec.default
            params.append(
                inspect.Parameter(
                    pname,
                    inspect.Parameter.KEYWORD_ONLY,
                    default=default,
                    annotation=annotated_type,
                )
            )

        async def _http_tool_runner(ctx: RunContext[AgentRuntimeDeps], **kwargs: Any) -> Dict[str, Any]:
            if ctx.deps.dry_run:
                return {
                    "ok": True,
                    "dry_run": True,
                    "tool": spec.name,
                    "method": spec.method,
                    "url": spec.url,
                    "credential_injected": bool(spec.credential_ref),
                    "args": kwargs,
                }

            headers = {"Content-Type": "application/json", **proxy.resolve_headers(spec.credential_ref)}
            try:
                async with httpx.AsyncClient(timeout=spec.timeout_seconds) as client:
                    if spec.method == "GET":
                        resp = await client.get(spec.url, params=kwargs, headers=headers)
                    else:
                        resp = await client.post(spec.url, json=kwargs, headers=headers)
                    resp.raise_for_status()
                    try:
                        return {"ok": True, "status_code": resp.status_code, "data": resp.json()}
                    except Exception:
                        return {"ok": True, "status_code": resp.status_code, "text": resp.text[:1000]}
            except Exception as err:
                logger.warning(f"[DeclarativeHttpTool] 工具 {spec.name} 调用失败: {err}")
                return {"ok": False, "error": type(err).__name__, "detail": str(err)}

        _http_tool_runner.__name__ = spec.name
        _http_tool_runner.__doc__ = spec.description
        _http_tool_runner.__signature__ = inspect.Signature(params, return_annotation=Dict[str, Any])
        _http_tool_runner.__annotations__ = annotations
        return _http_tool_runner


# ==============================================================================
# 3. 平台底座受控工具集 (Guarded Base Toolset)
# ==============================================================================

base_toolset: FunctionToolset[AgentRuntimeDeps] = FunctionToolset(id="base_clue_tools")


@base_toolset.tool
async def tool_schedule_followup(
    ctx: RunContext[AgentRuntimeDeps],
    target_time_iso: str,
    priority: int = 5,
    use_work_phone: bool = False,
    reason: str = "Agent智能跟进排期",
) -> Dict[str, Any]:
    """
    为当前线索安排下一次合规外呼排期 (内部强制经过 PolicyHarness 工时吸附与跨业务 24h Outbox 频控)。
    """
    pack = ctx.deps.domain_pack
    if ctx.deps.recent_24h_touches >= pack.max_touches_per_24h:
        return {
            "scheduled": False,
            "blocked_reason": f"global_outbox_24h_limit_reached:{ctx.deps.recent_24h_touches}/{pack.max_touches_per_24h}",
        }

    try:
        parsed_dt = ensure_tz_aware(datetime.fromisoformat(target_time_iso))
    except Exception:
        parsed_dt = ensure_tz_aware(get_now()) + timedelta(hours=2)

    harness = PolicyHarness(AgentStrategyConfig())
    dummy_decision = MasterSummaryDecision(
        status=StandardBusinessStatus.PARTIAL,
        reason=reason,
        action=ScheduleActionType.SCHEDULE_CALL,
        next_call_time=parsed_dt,
        priority=max(1, min(9, int(priority))),
        use_work_phone=bool(use_work_phone),
    )

    class _MinCtx:
        clue_id = ctx.deps.clue_id
        clue_intake_time = get_now()
        total_call_count = 0
        continuous_no_answer_count = 0

    safe_res = harness.evaluate_and_snap(dummy_decision, _MinCtx(), [])
    if not safe_res.success or not safe_res.next_call_time:
        return {"scheduled": False, "blocked_reason": safe_res.reason}

    if ctx.deps.dry_run:
        return {
            "scheduled": True,
            "dry_run": True,
            "next_call_at": safe_res.next_call_time.isoformat(),
            "priority": safe_res.priority,
            "use_work_phone": safe_res.use_work_phone,
        }

    from app.call_schedule.service import schedule_call
    from app.clue_handler.state import ClueState
    from app.dbmodels import crud

    await crud.update_clue_next_call_work_phone(ctx.deps.clue_id, safe_res.use_work_phone)
    try:
        await crud.update_clue_state_or_raise(ctx.deps.clue_id, ClueState.SCHEDULED.value)
    except Exception as st_err:
        logger.debug(f"[ToolBus] 更新 SCHEDULED 状态跳过: {st_err}")

    task = await schedule_call(
        ctx.deps.clue_id,
        safe_res.next_call_time,
        priority=safe_res.priority or 5,
    )
    return {
        "scheduled": True,
        "task_id": task.id,
        "next_call_at": task.run_at.isoformat(),
        "priority": safe_res.priority,
    }


@base_toolset.tool
async def tool_set_wakeup_timer(
    ctx: RunContext[AgentRuntimeDeps],
    wake_at_iso: str,
    check_goal: str,
) -> Dict[str, Any]:
    """
    设定认知型自唤醒定时器 (Always-On Follow-up Timer，不直接拨打电话)：
    用于延迟自检（例如“加微2小时后检查是否通过，未通过再决定发短信或补呼”）。
    """
    try:
        wake_dt = ensure_tz_aware(datetime.fromisoformat(wake_at_iso))
    except Exception:
        wake_dt = ensure_tz_aware(get_now()) + timedelta(hours=2)

    now = ensure_tz_aware(get_now())
    if wake_dt <= now:
        wake_dt = now + timedelta(minutes=15)

    if ctx.deps.dry_run:
        return {
            "timer_set": True,
            "dry_run": True,
            "wake_at": wake_dt.isoformat(),
            "check_goal": check_goal,
        }

    from app.dbmodels.models import ExecutionTask

    # 自动作废同线索尚未触发的旧唤醒 Timer
    await ExecutionTask.filter(
        clue_id=ctx.deps.clue_id,
        task_type="agent_wakeup",
        status__in=["pending", "retry"],
    ).update(status="cancelled", last_error="superseded_by_new_wakeup_timer")

    import uuid
    key = hashlib.sha256(
        f"wakeup:{ctx.deps.clue_id}:{wake_dt.isoformat()}:{check_goal}:{uuid.uuid4().hex}".encode()
    ).hexdigest()
    task = await ExecutionTask.create(
        task_key=key,
        task_type="agent_wakeup",
        workflow_id=ctx.deps.domain_pack.workflow_id,
        clue_id=ctx.deps.clue_id,
        strategy_id=ctx.deps.domain_pack.strategy_id,
        run_at=wake_dt.astimezone(timezone.utc),
        payload={"checkGoal": check_goal, "workflowId": ctx.deps.domain_pack.workflow_id},
        status="pending",
    )
    return {
        "timer_set": True,
        "task_id": task.id,
        "wake_at": wake_dt.isoformat(),
        "check_goal": check_goal,
    }


@base_toolset.tool
async def tool_terminate_clue(
    ctx: RunContext[AgentRuntimeDeps],
    terminal_status: Literal["accepted", "rejected"],
    reason: str,
) -> Dict[str, Any]:
    """
    将线索收口至终态 (accepted 或 rejected)，并同步发出 clue_ended 通知。
    """
    if ctx.deps.dry_run:
        return {
            "terminated": True,
            "dry_run": True,
            "status": terminal_status,
            "reason": reason,
        }

    from app.dbmodels import crud
    from app.observability.events_bus import ClueEndEvent, event_bus

    end_code = 200 if terminal_status == "accepted" else 300
    await crud.update_clue_state_or_raise(ctx.deps.clue_id, terminal_status)
    await crud.save_close_reason(ctx.deps.clue_id, reason)
    try:
        await event_bus.emit(
            ClueEndEvent(
                clue_id=ctx.deps.clue_id,
                workflow_id=ctx.deps.domain_pack.workflow_id,
                event_data={"code": end_code, "reason": reason},
            )
        )
    except Exception as ev_err:
        logger.warning(f"[ToolBus] 发射 ClueEndEvent 异常: {ev_err}")

    return {"terminated": True, "status": terminal_status, "code": end_code, "reason": reason}


@base_toolset.tool
async def tool_trigger_channel_action(
    ctx: RunContext[AgentRuntimeDeps],
    channel: Literal["wechat", "sms"],
    message_hint: str = "",
) -> Dict[str, Any]:
    """
    协同触发非语音触达通道动作 (如发送企业微信好友申请或补发跟进短信)，受全局 24h Outbox 限流保护。
    """
    pack = ctx.deps.domain_pack
    if ctx.deps.recent_24h_touches >= pack.max_touches_per_24h:
        return {
            "triggered": False,
            "blocked_reason": f"global_outbox_24h_limit_reached:{ctx.deps.recent_24h_touches}/{pack.max_touches_per_24h}",
        }

    if ctx.deps.dry_run:
        return {"triggered": True, "dry_run": True, "channel": channel, "message_hint": message_hint}

    from app.apis.webhook_dispatcher import dispatch_webhook_event
    from app.clue_handler.state import ClueState
    from app.dbmodels import crud

    event_name = "action_wechat" if channel == "wechat" else "action_sms"
    if channel == "wechat":
        try:
            await crud.update_clue_state_or_raise(ctx.deps.clue_id, ClueState.PENDING.value)
        except Exception as st_err:
            logger.debug(f"[ToolBus] 切至 PENDING 跳过: {st_err}")

    await dispatch_webhook_event(
        event=event_name,
        clue_id=ctx.deps.clue_id,
        data={"channel": channel, "message_hint": message_hint, "workflow_id": pack.workflow_id},
    )
    return {"triggered": True, "channel": channel, "event": event_name}


@base_toolset.tool
async def tool_handoff_workflow(
    ctx: RunContext[AgentRuntimeDeps],
    target_workflow_id: str,
    handoff_summary: str,
) -> HandoffResult:
    """
    跨业务动态热切换：当识别到客户真实诉求属于另一个业务线时，原子移交线索并继承跨业务上下文记忆。
    """
    pack = ctx.deps.domain_pack
    if target_workflow_id not in pack.allowed_handoff_targets:
        return HandoffResult(
            success=False,
            from_workflow=pack.workflow_id,
            to_workflow=target_workflow_id,
            handoff_count=ctx.deps.handoff_count,
            reason=f"目标业务 '{target_workflow_id}' 不在允许白名单 {pack.allowed_handoff_targets} 内",
        )

    if ctx.deps.handoff_count >= pack.max_handoffs:
        return HandoffResult(
            success=False,
            from_workflow=pack.workflow_id,
            to_workflow=target_workflow_id,
            handoff_count=ctx.deps.handoff_count,
            reason=f"已达到最大业务切换跳数限制 ({pack.max_handoffs})",
        )

    from app.agents.domain_registry import domain_pack_registry

    target_pack = await domain_pack_registry.get_pack(target_workflow_id)
    if ctx.deps.dry_run:
        return HandoffResult(
            success=True,
            from_workflow=pack.workflow_id,
            to_workflow=target_workflow_id,
            handoff_count=ctx.deps.handoff_count + 1,
            reason=f"[dry_run] 将热切换至 {target_pack.name}: {handoff_summary}",
        )

    from app.dbmodels.crud import atomic_handoff_clue_workflow

    res = await atomic_handoff_clue_workflow(
        clue_id=ctx.deps.clue_id,
        target_workflow_id=target_workflow_id,
        target_agent_id=target_pack.voice_agent_id,
        target_strategy_id=target_pack.strategy_id,
        handoff_summary=handoff_summary,
        max_handoffs=pack.max_handoffs,
    )
    return HandoffResult(
        success=bool(res.get("success")),
        from_workflow=res.get("from_workflow", pack.workflow_id),
        to_workflow=res.get("to_workflow", target_workflow_id),
        handoff_count=res.get("handoff_count", ctx.deps.handoff_count),
        reason=res.get("reason", "handoff_completed" if res.get("success") else "handoff_failed"),
    )


# ==============================================================================
# 4. PydanticAI 动态工具过滤、描述增强与延迟审批组装器
# ==============================================================================

async def domain_tool_filter(ctx: RunContext[AgentRuntimeDeps], tool_def: ToolDefinition) -> bool:
    """按业务卡带 allowed_tools 白名单与防乒乓跳数动态显隐工具"""
    pack = ctx.deps.domain_pack
    if tool_def.name not in pack.allowed_tools:
        return False
    if tool_def.name == "tool_handoff_workflow":
        return bool(pack.allowed_handoff_targets) and ctx.deps.handoff_count < pack.max_handoffs
    return True


async def domain_tool_prepare(
    ctx: RunContext[AgentRuntimeDeps],
    tool_defs: List[ToolDefinition],
) -> List[ToolDefinition]:
    """在推理步前动态将可切换目标业务说明注入 tool_handoff_workflow 描述"""
    pack = ctx.deps.domain_pack
    for td in tool_defs:
        if td.name == "tool_handoff_workflow" and pack.allowed_handoff_targets:
            targets_desc = ", ".join(
                f"{tid}({ctx.deps.target_descriptions.get(tid, tid)})"
                for tid in pack.allowed_handoff_targets
            )
            td.description = f"{td.description or ''} 当前允许切换的下游业务目标仅限: [{targets_desc}]。"
    return tool_defs


def domain_approval_required(
    ctx: RunContext[AgentRuntimeDeps],
    tool_def: ToolDefinition,
    tool_args: Dict[str, Any],
) -> bool:
    """Deferred Tool Calls 检查：匹配卡带 approval_required_tools 时挂起等待人工审批"""
    return tool_def.name in (ctx.deps.domain_pack.approval_required_tools or [])


def build_domain_toolset(
    domain_pack: BusinessDomainPack,
    proxy: Optional[CredentialProxy] = None,
) -> AbstractToolset[AgentRuntimeDeps]:
    """组合底座工具集与业务卡带声明式 HTTP 工具集，叠加过滤、预准备与延迟审批护栏"""
    sub_toolsets: List[AbstractToolset[AgentRuntimeDeps]] = [base_toolset]
    if domain_pack.declarative_http_tools:
        http_toolset = DeclarativeHttpToolFactory.build_toolset(
            domain_pack.declarative_http_tools, proxy=proxy
        )
        sub_toolsets.append(http_toolset)

    combined = CombinedToolset(sub_toolsets)
    return (
        combined.filtered(domain_tool_filter)
        .prepared(domain_tool_prepare)
        .approval_required(domain_approval_required)
    )
