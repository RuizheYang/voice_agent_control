import uuid
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, ConfigDict, model_validator, field_validator

class SttConfig(BaseModel):
    provider_id: str = Field("tencent", description="语音识别服务标识")
    hotwords: List[str] = Field(default_factory=list, description="识别热词")
    language: str = Field("zh-CN", description="语音识别目标语言")
    vad_silence: int = Field(240, description="VAD 静音判定阈值 (ms)")
    model_config = ConfigDict(extra="allow")

class TtsConfig(BaseModel):
    provider_id: str = Field("yx", description="语音合成服务标识")
    voice: str = Field("female", description="合成音色选项或音色ID")
    language: str = Field("zh-CN", description="语音合成目标语言")
    speed: float = Field(1.0, description="语速倍率")
    model_config = ConfigDict(extra="allow")

class TurnTakingConfig(BaseModel):
    provider_id: str = Field("yx-turn", description="轮次判断服务标识")
    model_config = ConfigDict(extra="allow")

class KnowledgeBaseConfig(BaseModel):
    kb_id: Optional[str] = Field(None, description="绑定的知识库唯一标识")
    model_config = ConfigDict(extra="allow")

class LlmConfig(BaseModel):
    model_id: str = Field("gemma4-31B", description="大语言模型服务标识")
    model_config = ConfigDict(extra="allow")

class VoiceAgentGlobalSettings(BaseModel):
    stt: Optional[SttConfig] = Field(default_factory=SttConfig, description="语音识别配置")
    tts: Optional[TtsConfig] = Field(default_factory=TtsConfig, description="语音合成配置")
    turn_taking: Optional[TurnTakingConfig] = Field(default_factory=TurnTakingConfig, description="对话轮次控制配置")
    knowledge_base: Optional[KnowledgeBaseConfig] = None
    tools: List[str] = Field(default_factory=list, description="可调用的工具列表")
    llm: Optional[LlmConfig] = Field(default_factory=LlmConfig, description="大模型配置")
    instruction: Optional[str] = Field(None, description="核心指令模板")
    model_config = ConfigDict(extra="allow")

class VoiceAgentCreateRequest(BaseModel):
    agent_id: Optional[str] = Field(None, description="Agent 唯一标识。若传入则更新/覆盖，未传入则自动生成")
    agent_name: str = Field(..., description="Agent 显示名称")
    version: str = Field("v1.0", description="Agent 配置版本")
    description: Optional[str] = Field(None, description="Agent 用途说明")
    global_settings: Optional[VoiceAgentGlobalSettings] = None
    # 同时兼容平铺传入字段
    stt: Optional[SttConfig] = None
    tts: Optional[TtsConfig] = None
    turn_taking: Optional[TurnTakingConfig] = None
    knowledge_base: Optional[KnowledgeBaseConfig] = None
    tools: Optional[List[str]] = None
    llm: Optional[LlmConfig] = None
    instruction: Optional[str] = Field(None, description="核心指令模板")
    agent_type: str = Field("single_prompt", description="Agent 类型: single_prompt")
    open_message: Optional[str] = Field(None, description="开场白模板")
    call_type: str = Field("outbound", description="呼叫类型: inbound / outbound")
    model_config = ConfigDict(extra="allow")

    @field_validator("agent_name")
    def validate_agent_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("agent_name 不能为空")
        return v.strip()

    @field_validator("call_type")
    def validate_call_type(cls, v: str) -> str:
        if v not in ("inbound", "outbound"):
            raise ValueError("call_type 必须为 'inbound' 或 'outbound'")
        return v

    @model_validator(mode="after")
    def merge_and_validate_fields(self) -> "VoiceAgentCreateRequest":
        # 1. 如果未传 agent_id，自动生成唯一 ID
        if not self.agent_id:
            self.agent_id = f"agent_{uuid.uuid4().hex[:12]}"

        # 2. 从 global_settings 同步到平铺字段
        if self.global_settings:
            gs = self.global_settings
            if gs.instruction and not self.instruction:
                self.instruction = gs.instruction
            if gs.stt and not self.stt:
                self.stt = gs.stt
            if gs.tts and not self.tts:
                self.tts = gs.tts
            if gs.turn_taking and not self.turn_taking:
                self.turn_taking = gs.turn_taking
            if gs.knowledge_base and not self.knowledge_base:
                self.knowledge_base = gs.knowledge_base
            if gs.tools and not self.tools:
                self.tools = gs.tools
            if gs.llm and not self.llm:
                self.llm = gs.llm

        # 3. 校验 instruction 必填性（根据文档要求: instruction 不能为空）
        if not self.instruction or not self.instruction.strip():
            raise ValueError("instruction 核心指令模板不能为空")

        # 补全默认配置项
        if not self.stt:
            self.stt = SttConfig()
        if not self.tts:
            self.tts = TtsConfig()
        if not self.turn_taking:
            self.turn_taking = TurnTakingConfig()
        if not self.llm:
            self.llm = LlmConfig()
        if self.tools is None:
            self.tools = []

        # 4. 反向组装 global_settings，保证结构完整
        self.global_settings = VoiceAgentGlobalSettings(
            stt=self.stt,
            tts=self.tts,
            turn_taking=self.turn_taking,
            knowledge_base=self.knowledge_base,
            tools=self.tools,
            llm=self.llm,
            instruction=self.instruction,
        )

        return self

class VoiceAgentConfigResponse(BaseModel):
    code: int = 0
    msg: str = "success"
    data: Optional[Dict[str, Any]] = None
