from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, field_validator


# ==============================================================================
# 1. 声明式扩展规范 (Declarative Cartridge Specs)
# ==============================================================================

class DeclarativeEvaluatorSpec(BaseModel):
    """零代码声明式业务子评估器定义 (通过 pydantic.create_model 动态实例化)"""
    name: str = Field(..., min_length=1, description="评估器唯一标识，如 loan_qualification")
    prompt: str = Field(..., min_length=1, description="引导模型思考的业务分析提示词")
    output_fields: Dict[str, str] = Field(
        default_factory=dict,
        description="提取字段与类型映射，如 {'has_stable_income': 'bool', 'credit_note': 'str'}",
    )


class DeclarativeToolParamSpec(BaseModel):
    """声明式 HTTP 工具参数定义"""
    type: Literal["str", "int", "float", "bool"] = "str"
    description: str = ""
    required: bool = True
    default: Optional[Any] = None


class DeclarativeHttpToolSpec(BaseModel):
    """零代码声明式 HTTP 工具定义 (支持 Credential Proxy 与 2s 超时硬限)"""
    name: str = Field(..., min_length=1, description="工具名称，必须以 tool_ 开头")
    description: str = Field(..., min_length=1, description="工具功能描述，供 LLM 决定何时调用")
    method: Literal["GET", "POST"] = "POST"
    url: str = Field(..., description="目标服务接口 URL (仅允许 https:// 或内网白名单)")
    credential_ref: Optional[str] = Field(
        None, description="密钥引用标识，由 CredentialProxy 运行时注入请求头，禁止直接写明文密钥"
    )
    timeout_seconds: float = Field(default=2.0, gt=0.0, le=2.0, description="单工具超时上限锁定 <= 2.0s")
    parameters: Dict[str, DeclarativeToolParamSpec] = Field(default_factory=dict)

    @field_validator("url")
    @classmethod
    def validate_safe_url(cls, v: str) -> str:
        clean = v.strip()
        if not (clean.startswith("https://") or clean.startswith("http://127.0.0.1") or clean.startswith("http://localhost") or clean.startswith("http://internal-")):
            raise ValueError(f"出于 SSRF 安全策略，声明式工具 URL 仅允许 https:// 或受控内网地址: {v}")
        return clean


# ==============================================================================
# 2. Anthropic Always-On 四桶结构化记忆 (4-Bucket Memory)
# ==============================================================================

class ClueStructuredMemory(BaseModel):
    """
    融合 Anthropic Always-On Agents 四桶持久化记忆结构：
    1. instructions: 跨轮业务与话术指引
    2. customer_notes: 经过异步 Dreaming 压缩沉淀的长期客户事实画像
    3. interaction_digest: 浓缩往期沟通摘要列表 (避免原文堆砌)
    4. progress: 当前阶段所处状态与待办进度
    """
    instructions: str = ""
    customer_notes: Dict[str, Any] = Field(default_factory=dict)
    interaction_digest: List[str] = Field(default_factory=list)
    progress: str = "initial"
    last_dreamed_at: Optional[str] = None

    def to_compact_index(self) -> str:
        """Index-First 紧凑渲染，防止 Context Rot 与 Token 膨胀"""
        notes_str = ", ".join(f"{k}={v}" for k, v in self.customer_notes.items()) if self.customer_notes else "暂无"
        recent_digest = self.interaction_digest[-3:] if self.interaction_digest else []
        digest_str = " | ".join(recent_digest) if recent_digest else "暂无往期沟通"
        return (
            f"[进度]: {self.progress}; "
            f"[客户画像]: {notes_str}; "
            f"[近期纪要]: {digest_str}; "
            f"[跨轮指引]: {self.instructions or '无'}"
        )


# ==============================================================================
# 3. 业务领域卡带包 (Business Domain Pack / Cartridge)
# ==============================================================================

DEFAULT_BASE_TOOLS = [
    "tool_schedule_followup",
    "tool_set_wakeup_timer",
    "tool_terminate_clue",
    "tool_trigger_channel_action",
    "tool_handoff_workflow",
]


class BusinessDomainPack(BaseModel):
    """单个业务线的完整声明式领域卡带 (支持 YAML/JSON 与 DB 热插拔)"""
    workflow_id: str
    name: str = ""
    version: str = "v1.0"
    domain_description: str = Field("", description="供 Supervisor 与 Triage 识别业务范围的语义描述")
    voice_agent_id: str = "default"
    strategy_id: str = "default"

    # 评估器与决策模式配置
    evaluation_mode: Literal["single_pass", "map_reduce"] = Field(
        default="single_pass",
        description="决策模式: single_pass(默认单次复合推理+工具挂载) / map_reduce(多专家并发+主控汇总)",
    )
    enabled_sub_evaluators: List[str] = Field(
        default_factory=lambda: ["intent", "objection", "appointment", "sentiment"],
        description="启用的内置子评估器列表",
    )
    custom_evaluators: List[DeclarativeEvaluatorSpec] = Field(
        default_factory=list, description="业务卡带自定义的声明式子评估器"
    )
    domain_master_prompt: Optional[str] = Field(None, description="注入主控决策 Prompt 的业务专属准则")

    # 合规工时窗口 (替代硬编码 9-21)
    work_start_time: str = Field(default="09:00:00", description="外呼合规开始时间 HH:MM:SS")
    work_end_time: str = Field(default="21:00:00", description="外呼合规结束时间 HH:MM:SS")

    # 通话中实时多智能体流配置 (对齐 Pipecat Flows / LiveKit Agents)
    in_call_flow: Optional[Dict[str, Any]] = Field(
        default=None, description="通话中实时多智能体节点配置 (initial_node, nodes, realtime_handoff_rules)"
    )

    # 工具与权限配置
    declarative_http_tools: List[DeclarativeHttpToolSpec] = Field(default_factory=list)
    allowed_tools: List[str] = Field(default_factory=lambda: list(DEFAULT_BASE_TOOLS))
    approval_required_tools: List[str] = Field(
        default_factory=list, description="需挂起等待人工审批 (Deferred Approval) 的高危工具名称列表"
    )

    # 跨业务切换与全局防骚扰护栏
    allowed_handoff_targets: List[str] = Field(default_factory=list)
    max_handoffs: int = Field(default=2, ge=0, le=5)
    max_touches_per_24h: int = Field(default=5, ge=1, le=20, description="跨业务跨通道 24h 总触达上限")


# ==============================================================================
# 4. PydanticAI 运行时依赖与输出契约
# ==============================================================================

class HandoffResult(BaseModel):
    """跨业务热切换结果"""
    success: bool
    from_workflow: Optional[str] = None
    to_workflow: Optional[str] = None
    handoff_count: int = 0
    reason: str = ""


class ToolExecutionRecord(BaseModel):
    """工具调用审计记录"""
    tool_name: str
    status: Literal["executed", "deferred_approval", "blocked", "error"]
    detail: Dict[str, Any] = Field(default_factory=dict)


class SupervisorActionPlan(BaseModel):
    """ClueSupervisorAgent 统一结构化输出契约"""
    selected_workflow_id: str = Field(..., description="当前或推荐路由的目标业务线 ID")
    handoff_triggered: bool = False
    handoff_reason: Optional[str] = None
    handoff_summary: Optional[str] = None
    summary_decision: Optional[Any] = None
    tool_executions: List[ToolExecutionRecord] = Field(default_factory=list)
    deferred_approval: Optional[Dict[str, Any]] = None
    reasoning: str = ""
    fallback_used: bool = False


class AgentRuntimeDeps(BaseModel):
    """PydanticAI RunContext[AgentRuntimeDeps] 运行时依赖注入容器"""
    clue_id: str
    domain_pack: BusinessDomainPack
    structured_memory: ClueStructuredMemory = Field(default_factory=ClueStructuredMemory)
    handoff_count: int = 0
    target_descriptions: Dict[str, str] = Field(default_factory=dict)
    phone_number: Optional[str] = None
    recent_24h_touches: int = 0
    coalesced_events: List[Dict[str, Any]] = Field(default_factory=list)
    dry_run: bool = False


class WakeupEvaluationContext(BaseModel):
    """Always-On 认知唤醒 Run 的结构化上下文快照"""
    clue_id: str
    workflow_id: str = "default"
    current_status: str = ""
    check_goal: str = ""
    coalesced_events: List[Dict[str, Any]] = Field(default_factory=list)
    memory_index: str = ""
    recent_24h_touches: int = 0
    max_touches_per_24h: int = 5
    remaining_touches_today: int = 5
    approval_notes: List[str] = Field(default_factory=list)


def is_channel_success_event(event: Dict[str, Any]) -> bool:
    """统一判断合并事件中是否包含渠道添加成功信号（兼容 success=True 与 data.resultCode=='3'）"""
    if not isinstance(event, dict) or event.get("type") != "channel_notify":
        return False
    ev_data = event.get("data") if isinstance(event.get("data"), dict) else {}
    return event.get("success") is True or str(ev_data.get("resultCode", "")) == "3"
