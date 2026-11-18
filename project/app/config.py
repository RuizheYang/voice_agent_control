import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


def _mask_dsn(url: str) -> str:
    """脱敏数据库/Redis连接字符串中的密码，防止启动日志泄露凭证"""
    if not url:
        return ""
    return re.sub(r"://([^:/?#]+):([^@]+)@", r"://\1:***@", url)


@dataclass
class RedisConfig:
    """Redis 连接配置"""
    cluster_mode: bool
    redis_url: str
    cluster_nodes: List[str]
    password: str
    decode_responses: bool
    socket_connect_timeout: int
    socket_timeout: int
    retry_on_timeout: bool
    max_connections: int

    @classmethod
    def from_config(cls, config_dict: dict):
        data = config_dict.get("redis", {})
        cluster_nodes_env = os.getenv("REDIS_CLUSTER_NODES", "")
        env_nodes = [n.strip() for n in cluster_nodes_env.split(",") if n.strip()] if cluster_nodes_env else None
        cluster_mode_env = os.getenv("REDIS_CLUSTER_MODE")
        cluster_mode = (
            cluster_mode_env.lower() == "true"
            if cluster_mode_env is not None
            else data.get("cluster_mode", False)
        )
        return cls(
            cluster_mode=cluster_mode,
            redis_url=os.getenv("REDIS_URL", data.get("redis_url", "redis://localhost:6379/0")),
            cluster_nodes=env_nodes if env_nodes is not None else data.get("cluster_nodes", []),
            password=os.getenv("REDIS_PASSWORD", data.get("password", "")),
            decode_responses=data.get("decode_responses", True),
            socket_connect_timeout=data.get("socket_connect_timeout", 5),
            socket_timeout=data.get("socket_timeout", 10),
            retry_on_timeout=data.get("retry_on_timeout", True),
            max_connections=data.get("max_connections", 100),
        )


@dataclass
class SecurityConfig:
    """字段级 AES-GCM 加解密配置（支持 16/24/32 字节密钥或 Base64 编码）"""
    version_label: str
    encryption_key: str
    search_salt: str

    @classmethod
    def from_config(cls, config_dict: dict):
        data = config_dict.get("security_config", {})
        return cls(
            version_label=os.getenv("APP_ENCRYPTION_VERSION", data.get("version_label", "V1")),
            encryption_key=os.getenv("APP_ENCRYPTION_KEY", data.get("encryption_key", "")),
            search_salt=os.getenv("APP_SEARCH_SALT", data.get("search_salt", "voice-agent-hub-salt")),
        )


LOGO = r"""
 █████╗  ██████╗ ███████╗███╗   ██╗████████╗██╗ ██████╗
██╔══██╗██╔════╝ ██╔════╝████╗  ██║╚══██╔══╝██║██╔════╝
███████║██║  ███╗█████╗  ██╔██╗ ██║   ██║   ██║██║     
██╔══██║██║   ██║██╔══╝  ██║╚██╗██║   ██║   ██║██║     
██║  ██║╚██████╔╝███████╗██║ ╚████║   ██║   ██║╚██████╗
╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚═╝ ╚═════╝
       V O I C E   A G E N T   C O N T R O L
"""


@dataclass
class LLMFactoryConfig:
    """OpenAI 兼容大模型服务配置"""
    base_url: str
    api_key: str
    headers: dict
    extra_body: dict
    model_type: str
    model_name: str

    def resolve_credential(self) -> str:
        return self.api_key

    @classmethod
    def from_config(cls, config_dict: dict):
        env_model = os.getenv("LLM_MODEL_NAME", "")
        env_base_url = os.getenv("LLM_BASE_URL", "")
        env_api_key = os.getenv("LLM_API_KEY", "")

        data = config_dict.get("llm_factory", {})
        if data and not env_model:
            model_name = list(data.keys())[0]
            model_data = data[model_name]
            return cls(
                model_name=model_name,
                base_url=env_base_url or model_data.get("base_url", ""),
                api_key=env_api_key or model_data.get("api_key", ""),
                headers=model_data.get("headers", {}),
                extra_body=model_data.get("extra_body", {}),
                model_type=model_data.get("model_type", "openai"),
            )

        model_data = data.get(env_model, {}) if isinstance(data, dict) else {}
        return cls(
            model_name=env_model or model_data.get("model_name", "gpt-4o-mini"),
            base_url=env_base_url or model_data.get("base_url", "https://api.openai.com/v1"),
            api_key=env_api_key or model_data.get("api_key", ""),
            headers=model_data.get("headers", {}),
            extra_body=model_data.get("extra_body", {}),
            model_type=model_data.get("model_type", "openai"),
        )


@dataclass
class WechatAddedNoticeConfig:
    """可选下游渠道动作通知接口配置"""
    url: str
    token: str
    app_id: str

    @classmethod
    def from_config(cls, config_dict: dict):
        data = config_dict.get("channel_notice") or config_dict.get("wechat_added_notice") or {}
        return cls(
            url=os.getenv("WECHAT_NOTICE_URL", data.get("url", "")),
            token=os.getenv("WECHAT_NOTICE_TOKEN", data.get("token", "")),
            app_id=os.getenv("WECHAT_NOTICE_APP_ID", data.get("app_id", "")),
        )


@dataclass
class CallRequestConfig:
    """外呼网关适配配置（支持 mock 沙箱与通用 HTTP Webhook 网关）"""
    gateway_mode: str = "mock"  # mock | webhook
    call_request_url: str = ""
    send_phone_status_url: str = ""
    cancel_url: str = ""

    @classmethod
    def from_config(cls, data: dict, env: str):
        telephony_section = data.get("telephony") or data.get("call_request_config") or {}
        send_url = os.getenv("TELEPHONY_SEND_URL", telephony_section.get("call_request_url", ""))
        status_url = os.getenv("TELEPHONY_STATUS_URL", telephony_section.get("send_phone_status_url", ""))
        cancel_url = os.getenv("TELEPHONY_CANCEL_URL", telephony_section.get("cancel_url", ""))
        default_mode = "webhook" if send_url else "mock"
        gateway_mode = os.getenv(
            "TELEPHONY_GATEWAY_MODE",
            telephony_section.get("gateway_mode", default_mode),
        ).strip().lower()
        return cls(
            gateway_mode=gateway_mode,
            call_request_url=send_url,
            send_phone_status_url=status_url,
            cancel_url=cancel_url,
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
        if values["lease_seconds"] <= 3 * values["http_timeout_seconds"] + 30:
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
    access_system_url: str = ""
    message_reminder_url: str = ""
    wechat_added_notice_config: WechatAddedNoticeConfig = field(
        default_factory=lambda: WechatAddedNoticeConfig("", "", "")
    )
    bypass_call: bool = True
    call_request_config: CallRequestConfig = field(default_factory=CallRequestConfig)
    voice_agent_cancel_url: str = ""

    @classmethod
    def _load_json_dict(cls, env: str) -> dict:
        explicit = os.getenv("CONFIG_FILE", "").strip()
        candidates = []
        if explicit:
            candidates.append(Path(explicit))
        cfg_dir = Path(__file__).parent / "configs"
        candidates.extend([cfg_dir / f"{env}.json", cfg_dir / "config.json"])
        for path in candidates:
            if path.is_file():
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
        return {}

    @classmethod
    def from_config(cls):
        env = os.getenv("ENV", "dev").strip() or "dev"
        config_dict = cls._load_json_dict(env.lower())
        telephony_section = config_dict.get("telephony", {})
        call_req_cfg = CallRequestConfig.from_config(config_dict, env.lower())

        default_db = "sqlite://dbfiles/voice_agent_hub.db"
        db_url = os.getenv(
            "DATABASE_URL",
            os.getenv("MYSQL_URL", config_dict.get("database_url") or config_dict.get("mysql_url") or default_db),
        )
        bypass_env = os.getenv("BYPASS_CALL")
        if bypass_env is not None:
            bypass_call = bypass_env.lower() == "true"
        else:
            bypass_call = bool(
                telephony_section.get("bypass_call", config_dict.get("bypass_call", call_req_cfg.gateway_mode == "mock"))
            )

        res = cls(
            call_request_config=call_req_cfg,
            voice_agent_report_url=os.getenv("VOICE_AGENT_REPORT_URL", config_dict.get("voice_agent_report_url", "")),
            access_system_url=os.getenv("ACCESS_SYSTEM_URL", config_dict.get("access_system_url", "")),
            redis_config=RedisConfig.from_config(config_dict),
            mysql_url=db_url,
            message_reminder_url=os.getenv("MESSAGE_REMINDER_URL", config_dict.get("message_reminder_url", "")),
            redis_queue_name=os.getenv("REDIS_QUEUE_NAME", config_dict.get("redis_queue_name", "voice_agent_hub")),
            event_queue_name=os.getenv("EVENT_QUEUE_NAME", config_dict.get("event_queue_name", "voice_agent_hub_events")),
            security_config=SecurityConfig.from_config(config_dict),
            llm_factory_config=LLMFactoryConfig.from_config(config_dict),
            wechat_added_notice_config=WechatAddedNoticeConfig.from_config(config_dict),
            bypass_call=bypass_call,
            voice_agent_cancel_url=call_req_cfg.cancel_url,
        )

        print("\033[96m" + LOGO + "\033[0m")
        print("\033[93m🚀 Agentic Voice Control Platform Configuration Loaded\033[0m\n")
        print(f"🌍 Environment:             {env.upper()}")
        print(f"📞 Telephony Gateway Mode:  {res.call_request_config.gateway_mode} (bypass={res.bypass_call})")
        print(f"🗃️  Database DSN:            {_mask_dsn(res.mysql_url)}")
        print(f"🔴 Redis URL:               {_mask_dsn(res.redis_config.redis_url)}")
        print(f"🧠 LLM Model:               {res.llm_factory_config.model_name}")
        print("-" * 70 + "\n")
        return res


call_schedule_settings = CallScheduleSettings.from_env()
config = Config.from_config()
