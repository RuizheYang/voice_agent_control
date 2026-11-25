"""Clean V3 RSI platform configuration loader."""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


def _mask_dsn(url: str) -> str:
    """Mask database/Redis credentials in startup logs."""
    if not url:
        return ""
    return re.sub(r"://([^:/?#]+):([^@]+)@", r"://\1:***@", url)


@dataclass
class RedisConfig:
    """Optional Redis connection pool settings."""

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
    def from_config(cls, config_dict: dict) -> "RedisConfig":
        data = config_dict.get("redis", {})
        cluster_nodes_env = os.getenv("REDIS_CLUSTER_NODES", "")
        env_nodes = (
            [n.strip() for n in cluster_nodes_env.split(",") if n.strip()]
            if cluster_nodes_env
            else None
        )
        cluster_mode_env = os.getenv("REDIS_CLUSTER_MODE")
        cluster_mode = (
            cluster_mode_env.lower() == "true"
            if cluster_mode_env is not None
            else data.get("cluster_mode", False)
        )
        return cls(
            cluster_mode=cluster_mode,
            redis_url=os.getenv(
                "REDIS_URL", data.get("redis_url", "redis://localhost:6379/0")
            ),
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
    """Field-level AES-GCM encryption settings."""

    version_label: str
    encryption_key: str
    search_salt: str

    @classmethod
    def from_config(cls, config_dict: dict) -> "SecurityConfig":
        data = config_dict.get("security_config", {})
        return cls(
            version_label=os.getenv(
                "APP_ENCRYPTION_VERSION", data.get("version_label", "V1")
            ),
            encryption_key=os.getenv(
                "APP_ENCRYPTION_KEY", data.get("encryption_key", "")
            ),
            search_salt=os.getenv(
                "APP_SEARCH_SALT", data.get("search_salt", "voice-agent-hub-salt")
            ),
        )


@dataclass
class LLMFactoryConfig:
    """OpenAI-compatible LLM & VLM provider configuration."""

    base_url: str
    api_key: str
    headers: dict
    extra_body: dict
    model_type: str
    model_name: str

    def resolve_credential(self) -> str:
        return self.api_key

    @classmethod
    def from_config(cls, config_dict: dict) -> "LLMFactoryConfig":
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
class CallRequestConfig:
    """Edge voice telephony gateway settings (mock / http_webhook / livekit / pipecat)."""

    gateway_mode: str = "mock"
    call_request_url: str = ""
    send_phone_status_url: str = ""
    cancel_url: str = ""

    @classmethod
    def from_config(cls, data: dict) -> "CallRequestConfig":
        telephony_section = data.get("telephony") or data.get("call_request_config") or {}
        send_url = os.getenv(
            "TELEPHONY_SEND_URL", telephony_section.get("call_request_url", "")
        )
        status_url = os.getenv(
            "TELEPHONY_STATUS_URL", telephony_section.get("send_phone_status_url", "")
        )
        cancel_url = os.getenv(
            "TELEPHONY_CANCEL_URL", telephony_section.get("cancel_url", "")
        )
        default_mode = "webhook" if send_url else "mock"
        gateway_mode = (
            os.getenv(
                "TELEPHONY_GATEWAY_MODE",
                telephony_section.get("gateway_mode", default_mode),
            )
            .strip()
            .lower()
        )
        return cls(
            gateway_mode=gateway_mode,
            call_request_url=send_url,
            send_phone_status_url=status_url,
            cancel_url=cancel_url,
        )


@dataclass
class Config:
    """Minimal, domain-agnostic RSI V3 runtime configuration."""

    database_url: str
    redis_config: RedisConfig
    security_config: SecurityConfig
    llm_factory_config: LLMFactoryConfig
    call_request_config: CallRequestConfig = field(default_factory=CallRequestConfig)
    bypass_call: bool = True

    @property
    def mysql_url(self) -> str:
        """Backward-compatible property alias for database_url."""
        return self.database_url

    @classmethod
    def _load_json_dict(cls, env: str) -> dict:
        explicit = os.getenv("CONFIG_FILE", "").strip()
        candidates: List[Path] = []
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
    def from_config(cls) -> "Config":
        env = os.getenv("ENV", "dev").strip() or "dev"
        config_dict = cls._load_json_dict(env.lower())
        telephony_section = config_dict.get("telephony", {})
        call_req_cfg = CallRequestConfig.from_config(config_dict)

        default_db = "sqlite://dbfiles/voice_agent_hub.db"
        db_url = os.getenv(
            "DATABASE_URL",
            os.getenv(
                "MYSQL_URL",
                config_dict.get("database_url")
                or config_dict.get("mysql_url")
                or default_db,
            ),
        )
        bypass_env = os.getenv("BYPASS_CALL")
        if bypass_env is not None:
            bypass_call = bypass_env.lower() == "true"
        else:
            bypass_call = bool(
                telephony_section.get(
                    "bypass_call",
                    config_dict.get("bypass_call", call_req_cfg.gateway_mode == "mock"),
                )
            )

        res = cls(
            database_url=db_url,
            redis_config=RedisConfig.from_config(config_dict),
            security_config=SecurityConfig.from_config(config_dict),
            llm_factory_config=LLMFactoryConfig.from_config(config_dict),
            call_request_config=call_req_cfg,
            bypass_call=bypass_call,
        )

        print(f"[RSI V3] env={env.upper()} db={_mask_dsn(res.database_url)} "
              f"voice_mode={res.call_request_config.gateway_mode} model={res.llm_factory_config.model_name}")
        return res


config = Config.from_config()
