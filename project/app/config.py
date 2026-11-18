import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class RedisConfig:
    
    """Redis配置"""
    cluster_mode: bool
    redis_url: str
    cluster_nodes: List
    password: str
    decode_responses: bool
    socket_connect_timeout: int
    socket_timeout: int
    retry_on_timeout: bool
    max_connections: int
    
    @classmethod
    def from_config(cls, config_dict):
        """从字典加载配置"""
        
        data = config_dict['redis']
        
        return cls(
            cluster_mode=data.get("cluster_mode", False),
            redis_url=data['redis_url'],
            cluster_nodes=data.get("cluster_nodes", []),
            password=data.get("password", ""),
            decode_responses=data.get("decode_responses", True),
            socket_connect_timeout=data.get("socket_connect_timeout", 5),
            socket_timeout=data.get("socket_timeout", 5),
            retry_on_timeout=data.get("retry_on_timeout", True),
            max_connections=data.get("max_connections", 100)
        )

@dataclass
class SecurityConfig:
    """加解密技术规范配置"""
    version_label: str
    xor_key: str
    encrypted_str: str
    search_salt: str

    @classmethod
    def from_config(cls, config_dict):
        # 获取 JSON 中的嵌套数据
        data = config_dict["security_config"]
        return cls(
            version_label=data["it_key_info"]["version_label"],
            xor_key=data["it_key_info"]["xor_key"],
            encrypted_str=data["it_key_info"]["encrypted_str"],
            search_salt=data["business_config"]["search_salt"]
        )
          
LOGO = r"""
 █████╗  ██████╗ ███████╗███╗   ██╗████████╗██╗ ██████╗
██╔══██╗██╔════╝ ██╔════╝████╗  ██║╚══██╔══╝██║██╔════╝
███████║██║  ███╗█████╗  ██╔██╗ ██║   ██║   ██║██║     
██╔══██║██║   ██║██╔══╝  ██║╚██╗██║   ██║   ██║██║     
██║  ██║╚██████╔╝███████╗██║ ╚████║   ██║   ██║╚██████╗
╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚═╝ ╚═════╝
            W E C H A T   A G E N T
"""

@dataclass
class LLMFactoryConfig:
    """大模型服务配置"""
    base_url: str
    api_key: str
    headers: dict
    extra_body: dict
    model_type: str
    model_name: str

    def resolve_credential(self) -> str:
        return self.api_key

    @classmethod
    def from_config(cls, config_dict):
        data = config_dict.get("llm_factory", {})
        if not data:
            # 返回空配置，如果没配就会在启动校验时被拦截
            return cls("", "", {}, {}, "", "")
        
        # 提取第一个配置的模型名
        model_name = list(data.keys())[0]
        model_data = data[model_name]
        return cls(
            model_name=model_name,
            base_url=model_data.get("base_url", ""),
            api_key=model_data.get("api_key", ""),
            headers=model_data.get("headers", {}),
            extra_body=model_data.get("extra_body", {}),
            model_type=model_data.get("model_type", "sglang_openai")
        )

@dataclass
class WechatAddedNoticeConfig:
    """加微成功通知接口配置"""
    url: str
    token: str
    app_id: str

    @classmethod
    def from_config(cls, config_dict):
        data = config_dict.get("wechat_added_notice", {})
        return cls(
            url=data.get("url", ""),
            token=data.get("token", ""),
            app_id=data.get("app_id", ""),
        )

@dataclass
class AsrConfig:
    """NS0011 一句话识别 ASR 配置。"""
    base_url: str
    path: str
    inter_id: str
    user_id: str
    token_key: str
    tenant_id: str
    language_type: str
    voice_format: str
    timeout_seconds: int
    retry_times: int

    @classmethod
    def from_config(cls, config_dict):
        data = config_dict.get("asr", {})
        return cls(
            base_url=data.get("base_url", ""),
            path=data.get("path", "/nebula_dmp/audio/query"),
            inter_id=data.get("inter_id", "NS0011"),
            user_id=data.get("user_id", ""),
            token_key=data.get("token_key", ""),
            tenant_id=data.get("tenant_id", "YIXIN"),
            language_type=data.get("language_type", "8k_zh"),
            voice_format=data.get("voice_format", "wav"),
            timeout_seconds=data.get("timeout_seconds", 10),
            retry_times=data.get("retry_times", 1),
        )


@dataclass
class CallRequestConfig:
    call_request_url: str = ""
    send_phone_status_url: str = ""

    @classmethod
    def from_config(cls, data, env):
        # URL按用户提供的状态地址及原项目同环境送号配置核对；不复制线路、话术或凭证。
        base = ("http://yuncall.yxit.cc" if env in ("prod", "production") else
                "http://10.10.201.194:9012" if env in ("test", "uat") else "")
        values = data.get("call_request_config", {})
        return cls(
            call_request_url=values.get("call_request_url", f"{base}/voip-ai/api/accessor/ai/outbound/sendPhone/v3" if base else ""),
            send_phone_status_url=values.get("send_phone_status_url", f"{base}/voip-ai/api/accessor/ai/callTask/page" if base else ""),
        )


@dataclass(frozen=True)
class CallScheduleSettings:
    workers: int = 2
    lease_seconds: int = 180
    http_timeout_seconds: int = 15
    dispatch_interval_ms: int = 1000
    reconcile_seconds: int = 15
    pause_delay_seconds: int = 5

    @classmethod
    def from_env(cls):
        values = {}
        for name in cls.__dataclass_fields__:
            value = int(os.getenv(f"CALL_SCHEDULE_{name.upper()}", str(getattr(cls, name))))
            if value < 1:
                raise ValueError(f"CALL_SCHEDULE_{name.upper()} must be positive")
            values[name] = value
        if values["lease_seconds"] <= 3*values["http_timeout_seconds"] + 30:
            raise ValueError("call lease must exceed three HTTP timeouts plus 30 seconds")
        return cls(**values)


@dataclass
class Config:
    voice_agent_report_url: str
    redis_config: RedisConfig
    mysql_url: str
    redis_queue_name: str 
    event_queue_name: str 
    security_config: SecurityConfig
    llm_factory_config: LLMFactoryConfig
    asr_config: AsrConfig
    access_system_url: str = ""
    message_reminder_url: str = ""
    wechat_added_notice_config: WechatAddedNoticeConfig = field(default_factory=lambda: WechatAddedNoticeConfig("", "", ""))
    bypass_call: bool = False
    call_request_config: CallRequestConfig = field(default_factory=CallRequestConfig)
    
    @classmethod
    def from_config(cls):
        env = os.getenv("ENV", 'test')
        config_path = Path(__file__).parent/ "configs" / f"{env}.json"
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件 {config_path} 不存在")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict = json.load(f)

        res = cls(
            call_request_config = CallRequestConfig.from_config(config_dict, env.lower()),
            voice_agent_report_url = config_dict.get('voice_agent_report_url', ''),
            access_system_url = config_dict.get('access_system_url', ''),
            redis_config = RedisConfig.from_config(config_dict),
            mysql_url = config_dict.get('mysql_url', ''),
            message_reminder_url = config_dict.get('message_reminder_url', ''),
            redis_queue_name = config_dict.get('redis_queue_name', 'voice_agent_hub'),
            event_queue_name = config_dict.get('event_queue_name', 'voice_agent_hub_events'),
            security_config = SecurityConfig.from_config(config_dict),
            llm_factory_config = LLMFactoryConfig.from_config(config_dict),
            wechat_added_notice_config = WechatAddedNoticeConfig.from_config(config_dict),
            asr_config = AsrConfig.from_config(config_dict),
            bypass_call = config_dict.get('bypass_call', False)
        )
        
        print("\033[96m" + LOGO + "\033[0m")
        print("\033[93m" + "🚀 Agentic WeChat Platform Configuration Loaded" + "\033[0m\n")
        
        print(f"🌍 \033[1mEnvironment:\033[0m            \033[92m{env.upper()}\033[0m")
        print(f"📞 \033[1mTelephony Sender Configured:\033[0m {bool(res.call_request_config.call_request_url)}")
        print(f"🗃️  \033[1mRedis Queue Name:\033[0m       \033[93m{res.redis_queue_name}\033[0m")
        print(f"🐬 \033[1mMySQL URL:\033[0m              \033[95m{res.mysql_url}\033[0m")
        print(f"🔴 \033[1mRedis URL:\033[0m              \033[91m{config_dict['redis']['redis_url']}\033[0m")
        print(f"📢 \033[1mVoice Agent Report URL:\033[0m  \033[94m{config_dict['voice_agent_report_url']}\033[0m")
        print(f"🧠 \033[1mLLM Engine:\033[0m             \033[95m{res.llm_factory_config.model_name}\033[0m")
        print(f"🎤 \033[1mASR Engine:\033[0m             \033[95m{res.asr_config.base_url}{res.asr_config.path}\033[0m")
        print(f"🔀 \033[1mBypass Call (挡板):\033[0m     \033[92m{config_dict.get('bypass_call', False)}\033[0m")
        print("\033[90m" + "-" * 70 + "\033[0m\n")
        
        return res
        
    
call_schedule_settings = CallScheduleSettings.from_env()
config = Config.from_config()
