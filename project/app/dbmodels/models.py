from tortoise.models import Model
from tortoise import fields
from tortoise.contrib.pydantic import pydantic_model_creator


# ==============================================================================
# 1. Voice Agent 实体表 (对齐文档 2.1，作为独立实体配置)
# ==============================================================================
class VoiceAgent(Model):
    """Voice Agent 配置实体表 (托管提示词、音色、LLM 等配置)"""
    id = fields.BigIntField(primary_key=True, description="数据库内部主键")
    agent_id = fields.CharField(max_length=128, unique=True, db_index=True, source_field="agentId", description="Agent 唯一标识")
    agent_name = fields.CharField(max_length=128, default="", source_field="agentName", description="Agent 显示名称")
    version = fields.CharField(max_length=32, default="v1.0", description="版本标签")
    description = fields.TextField(null=True, description="用途说明")
    instruction = fields.TextField(default="", description="核心提示词指令模板")
    open_message = fields.CharField(max_length=1024, null=True, source_field="openMessage", description="开场白模板")
    stt = fields.JSONField(null=True, description="ASR 配置")
    tts = fields.JSONField(null=True, description="TTS 配置")
    llm = fields.JSONField(null=True, description="LLM 配置")
    turn_taking = fields.JSONField(null=True, source_field="turnTaking", description="轮次控制配置")
    tools = fields.JSONField(null=True, description="工具列表")
    knowledge_base = fields.JSONField(null=True, source_field="knowledgeBase", description="绑定的知识库")
    global_settings = fields.JSONField(null=True, source_field="globalSettings", description="全局配置包含stt/tts/llm等")
    agent_type = fields.CharField(max_length=32, default="single_prompt", source_field="agentType", description="Agent 类型")
    call_type = fields.CharField(max_length=16, default="outbound", source_field="callType", description="呼叫方向: inbound/outbound")
    status = fields.CharField(max_length=16, default="active", description="状态: active/inactive")
    create_time = fields.DatetimeField(auto_now_add=True, source_field="createTime")
    update_time = fields.DatetimeField(auto_now=True, source_field="updateTime")

    class Meta:
        table = "voice_agent"

    @property
    def agentId(self):
        return self.agent_id

    @property
    def agentName(self):
        return self.agent_name

    @property
    def globalSettings(self):
        if self.global_settings:
            return self.global_settings
        return {
            "stt": self.stt,
            "tts": self.tts,
            "turn_taking": self.turn_taking,
            "knowledge_base": self.knowledge_base,
            "tools": self.tools or [],
            "llm": self.llm,
            "instruction": self.instruction,
        }


# ==============================================================================
# 2. 外呼策略实体表 (对齐文档 2.2，内置排期方案与追呼限制)
# ==============================================================================
class OutboundStrategy(Model):
    """外呼跟进策略配置表 (只选择固定排期模板，托管总结规则与限制)"""
    id = fields.BigIntField(primary_key=True, description="数据库内部主键")
    strategy_id = fields.CharField(max_length=128, unique=True, db_index=True, source_field="strategyId", description="策略实例唯一ID")
    name = fields.CharField(max_length=128, null=True, default="", description="策略显示名称")
    schedule_id = fields.CharField(max_length=128, default="agent_driven", source_field="scheduleId", description="排期引擎方案ID (agent_driven)")
    schedule_version = fields.CharField(max_length=32, default="1.0", source_field="scheduleVersion", description="内置排期模板版本")
    followup = fields.JSONField(default=dict, description="跟进配置JSON (summary_type 模块标识、summary_prompt 补充提取及四项追呼限制)")
    decision_rule_version = fields.CharField(max_length=32, default="1", source_field="decisionRuleVersion", description="内部决策规则版本")
    sip = fields.JSONField(null=True, description="话务线路参数 (accessor_id, line_id, mobile_line_id)")
    webhook = fields.JSONField(null=True, description="回推订阅配置 (url, events)")
    status = fields.CharField(max_length=16, default="active", description="策略状态: active/paused")
    config_version = fields.IntField(default=1, source_field="configVersion", description="配置修改版本")
    create_time = fields.DatetimeField(auto_now_add=True, source_field="createTime")
    update_time = fields.DatetimeField(auto_now=True, source_field="updateTime")

    class Meta:
        table = "outbound_strategy"

    @property
    def strategyId(self):
        return self.strategy_id

    @property
    def scheduleId(self):
        return self.schedule_id


# ==============================================================================
# 3. 业务线绑定表 (workflow_id 映射 agent_id 与 strategy_id)
# ==============================================================================
class WorkflowBinding(Model):
    """业务流程与 Agent / 策略绑定配置表"""
    id = fields.BigIntField(primary_key=True, description="绑定内部主键")
    workflow_id = fields.CharField(max_length=128, unique=True, db_index=True, source_field="workflowId", description="外部流程/业务线ID")
    agent_id = fields.CharField(max_length=128, source_field="agentId", description="关联的 Voice Agent ID")
    strategy_id = fields.CharField(max_length=128, db_index=True, source_field="strategyId", description="关联的外呼策略 ID")
    binding_version = fields.IntField(default=1, source_field="bindingVersion", description="绑定版本")
    name = fields.CharField(max_length=128, null=True, description="业务线名称")
    description = fields.TextField(null=True, description="业务线流程描述")
    version = fields.CharField(max_length=64, default="v1.0", description="业务线版本标签")
    is_active = fields.BooleanField(default=True, source_field="isActive", description="是否启用")
    create_time = fields.DatetimeField(auto_now_add=True, source_field="createTime")
    update_time = fields.DatetimeField(auto_now=True, source_field="updateTime")

    class Meta:
        table = "workflow_binding"

    @property
    def workflowId(self):
        return self.workflow_id

    @property
    def agentId(self):
        return self.agent_id

    @property
    def strategyId(self):
        return self.strategy_id

    @property
    def isActive(self):
        return self.is_active


# ==============================================================================
# 3.5 Multi-Agent 业务领域包配置表 (独立建表，零破坏兼容旧表)
# ==============================================================================
class WorkflowDomainConfig(Model):
    """业务线 Multi-Agent 卡带配置表 (托管 Prompt、声明式评估器、工具白名单、Handoff 流转规则)"""
    id = fields.BigIntField(primary_key=True, description="领域配置内部主键")
    workflow_id = fields.CharField(max_length=128, unique=True, db_index=True, source_field="workflowId", description="关联业务线 ID")
    version = fields.CharField(max_length=64, default="v1.0", description="卡带配置版本")
    domain_config = fields.JSONField(default=dict, source_field="domainConfig", description="完整的 BusinessDomainPack JSON 配置")
    is_active = fields.BooleanField(default=True, source_field="isActive", description="是否启用")
    create_time = fields.DatetimeField(auto_now_add=True, source_field="createTime")
    update_time = fields.DatetimeField(auto_now=True, source_field="updateTime")

    class Meta:
        table = "workflow_domain_config"

    @property
    def workflowId(self):
        return self.workflow_id


# ==============================================================================
# 4. 统一线索与执行状态表 (彻底替代原 Mongo Clue + MySQL ClueState)
# ==============================================================================
class ClueInfo(Model):
    """统一线索及执行状态表 (单表聚合线索数据、动态参数、状态机与并发版本)"""
    id = fields.BigIntField(primary_key=True, description="线索内部主键")
    clue_id = fields.CharField(max_length=128, unique=True, db_index=True, source_field="clueId", description="全局唯一线索ID")
    workflow_instance_id = fields.CharField(max_length=64, unique=True, source_field="workflowInstanceId", description="服务端生成的运行实例ID")
    source_app_id = fields.CharField(max_length=128, default="", source_field="sourceAppId", description="认证请求头 X-App-Id")
    external_task_id = fields.CharField(max_length=128, null=True, source_field="externalTaskId", description="外部业务任务ID")
    workflow_id = fields.CharField(max_length=128, db_index=True, source_field="workflowId", description="流程/业务线ID")
    agent_id = fields.CharField(max_length=128, source_field="agentId", description="最终生效的 Agent ID")
    strategy_id = fields.CharField(max_length=128, db_index=True, source_field="strategyId", description="最终生效的策略 ID")
    import_task_id = fields.BigIntField(default=0, source_field="importTaskId", description="关联导入任务ID")
    phone_number = fields.CharField(max_length=512, source_field="phoneNumber", description="客户电话 (加密存储)")
    customer_name = fields.CharField(max_length=512, null=True, source_field="customerName", description="客户姓名 (加密存储)")
    agent_data = fields.JSONField(default=dict, source_field="agentData", description="动态业务变量与上下文 (RunContext)")
    overrides = fields.JSONField(null=True, description="单条流入原始覆盖参数 (本期预留)")
    clue_config = fields.JSONField(default=dict, source_field="clueConfig", description="入流时固化的策略快照 (ClueConfig)")
    config_version = fields.IntField(default=1, source_field="configVersion", description="线索配置版本")
    status = fields.CharField(max_length=24, default="created", db_index=True, description="线索状态: created/scheduled/calling/summary/pending/accepted/rejected")
    close_reason = fields.CharField(max_length=128, null=True, source_field="closeReason", description="完成、取消或失败原因")
    end_code = fields.IntField(null=True, source_field="endCode", description="线索结束状态码 200/300/500")
    decision = fields.CharField(max_length=16, null=True, description="最近一次跟进决策 continue/fail/success")
    total_call_count = fields.IntField(default=0, source_field="totalCallCount", description="已占用的呼叫次数")
    no_answer_count = fields.IntField(default=0, source_field="noAnswerCount", description="无人接听次数")
    next_call_at = fields.DatetimeField(null=True, source_field="nextCallAt", description="下次计划外呼时间")
    expires_at = fields.DatetimeField(null=True, source_field="expiresAt", description="最长追呼过期时间")
    last_call_id = fields.CharField(max_length=64, null=True, source_field="lastCallId", description="最近一次通话ID")
    state_version = fields.IntField(default=0, source_field="stateVersion", description="并发校验乐观锁版本")
    received_at = fields.DatetimeField(auto_now_add=True, source_field="receivedAt", description="线索接收时间")
    closed_at = fields.DatetimeField(null=True, source_field="closedAt", description="线索结束时间")
    create_time = fields.DatetimeField(auto_now_add=True, source_field="createTime")
    update_time = fields.DatetimeField(auto_now=True, source_field="updateTime")

    class Meta:
        table = "clue_info"

    def __init__(self, **kwargs):
        if "clueId" in kwargs and "clue_id" not in kwargs:
            kwargs["clue_id"] = kwargs.pop("clueId")
        if "state" in kwargs and "status" not in kwargs:
            kwargs["status"] = kwargs.pop("state")
        if "workflowInstanceId" in kwargs and "workflow_instance_id" not in kwargs:
            kwargs["workflow_instance_id"] = kwargs.pop("workflowInstanceId")
        if "workflowId" in kwargs and "workflow_id" not in kwargs:
            kwargs["workflow_id"] = kwargs.pop("workflowId")
        if "agentId" in kwargs and "agent_id" not in kwargs:
            kwargs["agent_id"] = kwargs.pop("agentId")
        if "strategyId" in kwargs and "strategy_id" not in kwargs:
            kwargs["strategy_id"] = kwargs.pop("strategyId")
        if "phoneNumber" in kwargs and "phone_number" not in kwargs:
            kwargs["phone_number"] = kwargs.pop("phoneNumber")
        if "customerName" in kwargs and "customer_name" not in kwargs:
            kwargs["customer_name"] = kwargs.pop("customerName")
        if "workflow_instance_id" not in kwargs:
            kwargs["workflow_instance_id"] = f"wi_{kwargs.get('clue_id', 'default')}"
        if "workflow_id" not in kwargs:
            kwargs["workflow_id"] = "default"
        if "agent_id" not in kwargs:
            kwargs["agent_id"] = "default"
        if "strategy_id" not in kwargs:
            kwargs["strategy_id"] = "default"
        if "phone_number" not in kwargs:
            kwargs["phone_number"] = "13800000000"
        super().__init__(**kwargs)

    @classmethod
    def filter(cls, *args, **kwargs):
        if "clueId" in kwargs and "clue_id" not in kwargs:
            kwargs["clue_id"] = kwargs.pop("clueId")
        if "state" in kwargs and "status" not in kwargs:
            kwargs["status"] = kwargs.pop("state")
        if "workflowId" in kwargs and "workflow_id" not in kwargs:
            kwargs["workflow_id"] = kwargs.pop("workflowId")
        if "agentId" in kwargs and "agent_id" not in kwargs:
            kwargs["agent_id"] = kwargs.pop("agentId")
        if "strategyId" in kwargs and "strategy_id" not in kwargs:
            kwargs["strategy_id"] = kwargs.pop("strategyId")
        return super().filter(*args, **kwargs)

    @property
    def clueId(self):
        return self.clue_id

    @property
    def workflowInstanceId(self):
        return self.workflow_instance_id

    @property
    def customerPhoneNum(self):
        return self.phone_number

    @property
    def customerCity(self):
        return self.agent_data.get("customerCity") if isinstance(self.agent_data, dict) else None

    @property
    def clueSource(self):
        return self.agent_data.get("clueSource", "default") if isinstance(self.agent_data, dict) else "default"

    @property
    def orderTime(self):
        return self.agent_data.get("orderTime") if isinstance(self.agent_data, dict) else None

    @property
    def agentData(self):
        return self.agent_data

    @property
    def clueConfig(self):
        return self.clue_config

    @property
    def state(self):
        return self.status

    @state.setter
    def state(self, value):
        self.status = value


# ==============================================================================
# 5. 统一通话记录表 (彻底替代原 EndCallRecord + CallSummaryRecord + Mongo EndCallInfo)
# ==============================================================================
class CallRecord(Model):
    """每次外呼的权威状态与分析决策记录表"""
    id = fields.BigIntField(primary_key=True, description="通话内部主键")
    call_id = fields.CharField(max_length=64, unique=True, db_index=True, source_field="callId", description="外呼幂等标识与通话唯一ID")
    clue_id = fields.CharField(max_length=128, db_index=True, source_field="clueId", description="所属线索ID")
    agent_id = fields.CharField(max_length=128, source_field="agentId", description="本次调用的外部 Agent ID")
    strategy_id = fields.CharField(max_length=128, source_field="strategyId", description="本次调用的策略实例 ID")
    config_snapshot = fields.JSONField(default=dict, source_field="configSnapshot", description="外呼配置与运行时上下文快照")
    attempt_no = fields.IntField(default=1, source_field="attemptNo", description="呼叫轮次序号 (从 1 开始)")
    call_task_id = fields.BigIntField(default=0, source_field="callTaskId", description="对应外呼任务 ID")
    provider = fields.CharField(max_length=64, default="default", description="外呼平台/供应商标识")
    provider_call_id = fields.CharField(max_length=128, null=True, source_field="providerCallId", description="供应商通话ID")
    status = fields.CharField(max_length=24, default="created", db_index=True, description="created/submitting/submitted/unknown/ringing/connected/ended/failed/cancelled")
    outcome = fields.CharField(max_length=32, null=True, description="answered/no_answer/busy/rejected/voicemail/invalid_number/error")
    answered = fields.IntField(null=True, description="0未接通 / 1接通")
    scheduled_at = fields.DatetimeField(null=True, source_field="scheduledAt", description="计划执行时间")
    submitted_at = fields.DatetimeField(null=True, source_field="submittedAt", description="提交外呼时间")
    started_at = fields.DatetimeField(null=True, source_field="startedAt", description="开始呼叫时间")
    answered_at = fields.DatetimeField(null=True, source_field="answeredAt", description="接通时间")
    ended_at = fields.DatetimeField(null=True, source_field="endedAt", description="通话结束时间")
    duration_seconds = fields.IntField(null=True, source_field="durationSeconds", description="通话秒数")
    billable_seconds = fields.IntField(null=True, source_field="billableSeconds", description="计费秒数")
    recording_url = fields.CharField(max_length=2048, null=True, source_field="recordingUrl", description="录音文件地址")
    provider_result_code = fields.CharField(max_length=64, null=True, source_field="providerResultCode", description="供应商结果码")
    provider_result_message = fields.CharField(max_length=512, null=True, source_field="providerResultMessage", description="供应商结果信息")
    summary_status = fields.CharField(max_length=16, default="pending", source_field="summaryStatus", description="总结状态: pending/processing/succeeded/failed/skipped")
    common_call_events = fields.JSONField(null=True, source_field="commonCallEvents", description="大模型通用结构化提取分析结果")
    # 旧执行层决策保持原语义；Summary 三态保存在 decision_result.summary 中。
    decision = fields.CharField(max_length=16, null=True, description="执行层决策，沿用 continue/fail/success")
    decision_result = fields.JSONField(null=True, source_field="decisionResult", description="分区明细：summary 为总结结果，execution 为执行层结果；保留旧字段")
    state_version = fields.IntField(default=0, source_field="stateVersion", description="并发及重复回调校验版本")
    create_time = fields.DatetimeField(auto_now_add=True, source_field="createTime")
    update_time = fields.DatetimeField(auto_now=True, source_field="updateTime")

    class Meta:
        table = "call_record"

    @property
    def callId(self):
        return self.call_id

    @property
    def clueId(self):
        return self.clue_id

    @property
    def callAnswered(self):
        return bool(self.answered == 1)

    @property
    def recordFile(self):
        return self.recording_url


# ==============================================================================
# 6. 任务发件箱与异步队列表 (Transactional Outbox)
# ==============================================================================
class ExecutionTask(Model):
    """持久化任务与事件发件箱 (管理 Webhook 重试、延迟外呼、事件追溯)"""
    id = fields.BigIntField(primary_key=True, description="任务内部主键")
    task_key = fields.CharField(max_length=64, unique=True, db_index=True, source_field="taskKey", description="业务去重 SHA-256 键")
    task_type = fields.CharField(max_length=24, db_index=True, source_field="taskType", description="import/new_clue/call/summary/action/webhook/event")
    app_id = fields.CharField(max_length=128, null=True, source_field="appId")
    request_id = fields.CharField(max_length=128, null=True, source_field="requestId")
    workflow_id = fields.CharField(max_length=128, null=True, source_field="workflowId")
    clue_id = fields.CharField(max_length=128, null=True, db_index=True, source_field="clueId")
    call_id = fields.CharField(max_length=64, null=True, source_field="callId")
    strategy_id = fields.CharField(max_length=128, null=True, source_field="strategyId")
    event_id = fields.CharField(max_length=64, null=True, db_index=True, source_field="eventId")
    event_type = fields.CharField(max_length=64, null=True, source_field="eventType")
    event_version = fields.CharField(max_length=32, null=True, source_field="eventVersion")
    delivery_key = fields.CharField(max_length=64, null=True, source_field="deliveryKey")
    delivery_policy = fields.CharField(max_length=16, null=True, source_field="deliveryPolicy")
    payload = fields.JSONField(default=dict, description="任务载荷或请求体")
    result = fields.JSONField(null=True, description="小型执行结果")
    status = fields.CharField(max_length=16, default="pending", db_index=True, description="pending/processing/retry/unknown/succeeded/failed/cancelled")
    run_at = fields.DatetimeField(source_field="runAt", description="下次可执行时间")
    retry_count = fields.IntField(default=0, source_field="retryCount", description="已使用的重试次数")
    max_retries = fields.IntField(default=5, null=True, source_field="maxRetries", description="最多重试次数")
    lock_owner = fields.CharField(max_length=128, null=True, source_field="lockOwner")
    lock_token = fields.CharField(max_length=64, null=True, source_field="lockToken")
    locked_until = fields.DatetimeField(null=True, source_field="lockedUntil")
    last_error = fields.TextField(null=True, source_field="lastError")
    finished_at = fields.DatetimeField(null=True, source_field="finishedAt")
    create_time = fields.DatetimeField(auto_now_add=True, source_field="createTime")
    update_time = fields.DatetimeField(auto_now=True, source_field="updateTime")

    class Meta:
        table = "execution_task"


# ==============================================================================
# 7. 服务与策略暂停历史表
# ==============================================================================
class PauseRecord(Model):
    """服务与策略级别暂停控制记录表"""
    id = fields.BigIntField(primary_key=True, description="暂停记录内部主键")
    scope_type = fields.CharField(max_length=16, source_field="scopeType", description="service/strategy")
    target_id = fields.CharField(max_length=128, source_field="targetId", description="voice-agent-hub 或 strategyId")
    paused_at = fields.DatetimeField(source_field="pausedAt", description="实际暂停时间")
    paused_until = fields.DatetimeField(null=True, source_field="pausedUntil", description="计划恢复时间")
    resumed_at = fields.DatetimeField(null=True, source_field="resumedAt", description="实际恢复时间")
    reason = fields.CharField(max_length=512, null=True, description="暂停原因")
    paused_by = fields.CharField(max_length=64, null=True, source_field="pausedBy", description="暂停操作人")
    resumed_by = fields.CharField(max_length=64, null=True, source_field="resumedBy", description="恢复操作人")
    active_pause_key = fields.CharField(max_length=160, null=True, unique=True, source_field="activePauseKey", description="活动状态唯一标识")
    create_time = fields.DatetimeField(auto_now_add=True, source_field="createTime")

    class Meta:
        table = "pause_record"


# ==============================================================================
# 8. 应用调用方鉴权表
# ==============================================================================
class AppAuth(Model):
    """外部调用方鉴权与业务线权限表"""
    id = fields.BigIntField(primary_key=True, description="应用内部主键")
    app_id = fields.CharField(max_length=128, unique=True, db_index=True, source_field="appId", description="X-App-Id 应用标识")
    app_secret = fields.CharField(max_length=255, source_field="appSecret", description="HMAC-SHA256 密钥")
    app_name = fields.CharField(max_length=128, null=True, source_field="appName", description="应用名称")
    allowed_workflows = fields.JSONField(default=list, source_field="allowedWorkflows", description="授权的 workflow 列表")
    is_active = fields.BooleanField(default=True, source_field="isActive", description="是否启用")
    create_time = fields.DatetimeField(auto_now_add=True, source_field="createTime")
    update_time = fields.DatetimeField(auto_now=True, source_field="updateTime")

    class Meta:
        table = "appAuth"

    @property
    def appId(self):
        return self.app_id

    @property
    def appSecret(self):
        return self.app_secret


CallRecordPydantic = pydantic_model_creator(
    CallRecord,
    name="CallRecordPydantic",
)
