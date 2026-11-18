"""新线索接口契约；旧 clue_create 模型继续供历史接口使用。"""
import json
import re
from typing import Any, Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

Identifier = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]


class InflowOverrides(BaseModel):
    tts_id: Identifier | None = None
    voice_agent_id: Identifier | None = None
    followup_strategy_id: Identifier | None = None
    callback_url: str | None = Field(None, max_length=2048)
    model_config = ConfigDict(extra="forbid")

    @field_validator("callback_url")
    @classmethod
    def valid_callback(cls, value):
        if value is not None:
            from urllib.parse import urlsplit
            parts = urlsplit(value)
            if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password:
                raise ValueError("callback_url 必须是无认证信息的 HTTP(S) URL")
        return value


class InflowRecord(BaseModel):
    clue_id: Identifier
    external_task_id: Identifier | None = None
    phone_number: str = Field(min_length=7, max_length=32)
    agent_data: dict[str, Any]
    model_config = ConfigDict(extra="forbid")

    @field_validator("phone_number")
    @classmethod
    def normalize_phone(cls, value):
        value = value.strip().replace(" ", "").replace("-", "")
        if not re.fullmatch(r"\+?\d{7,15}", value, flags=re.ASCII):
            raise ValueError("phone_number 应为7至15位号码，可含国际区号前缀+")
        return value

    @field_validator("agent_data")
    @classmethod
    def validate_agent_data(cls, value):
        for name in ("variables", "extras"):
            if name in value and not isinstance(value[name], dict):
                raise ValueError(f"agent_data.{name} 必须是对象")
        json.dumps(value, allow_nan=False)
        return value


class SingleInflowRequest(InflowRecord):
    request_id: Identifier
    workflow_id: Identifier
    overrides: InflowOverrides | None = None


class BatchInflowRequest(BaseModel):
    request_id: Identifier
    workflow_id: Identifier
    records: list[Any] = Field(min_length=1, max_length=1000)
    model_config = ConfigDict(extra="forbid")


class FileInflowRequest(BaseModel):
    request_id: Identifier
    workflow_id: Identifier
    file_type: str
    file_url: str | None = Field(None, max_length=4096)
    file_sha256: str | None = Field(None, pattern=r"^[a-fA-F0-9]{64}$")
    model_config = ConfigDict(extra="forbid")

    @field_validator("file_type")
    @classmethod
    def valid_type(cls, value):
        value = value.lower().strip()
        if value not in ("jsonl", "excel"):
            raise ValueError("file_type 仅支持 jsonl 或 excel")
        return value
