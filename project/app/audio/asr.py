from __future__ import annotations

import base64
from typing import Any

import aiohttp
from app.config import config

class NS0011Client:
    """一句话识别 ASR 客户端。"""

    def __init__(self, cfg=None):
        self.cfg = cfg or config.asr_config

    @property
    def url(self) -> str:
        return f"{self.cfg.base_url.rstrip('/')}{self.cfg.path}"

    async def recognize(
        self,
        order_id: str,
        trans_id: str,
        *,
        audio_file: str | None = None,
        audio_content: bytes | None = None,
        voice_format: str | None = None,
    ) -> str:
        data: dict[str, Any] = {
            "languageType": self.cfg.language_type,
            "orderId": order_id,
            "transId": trans_id,
            "voiceFormat": voice_format or self.cfg.voice_format,
        }
        if audio_file:
            data["audioFile"] = audio_file
        elif audio_content:
            data["audioContent"] = base64.b64encode(audio_content).decode("ascii")
        else:
            raise ValueError("audioFile or audioContent is required")

        payload = {
            "header": {
                "tenantId": self.cfg.tenant_id,
                "interId": self.cfg.inter_id,
                "userId": self.cfg.user_id,
                "tokenKey": self.cfg.token_key,
            },
            "data": data,
        }

        timeout = aiohttp.ClientTimeout(total=self.cfg.timeout_seconds)
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.post(
                self.url,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=timeout,
            ) as response:
                text = await response.text()
                try:
                    result = await response.json(content_type=None)
                except Exception:
                    result = None
                status = response.status
        if status != 200:
            raise RuntimeError(f"NS0011 http error: status={status}, body={text[:300]}")
        if not isinstance(result, dict):
            raise RuntimeError(f"NS0011 invalid response: {text[:300]}")
        if str(result.get("code")) != "0":
            raise RuntimeError(f"NS0011 business error: code={result.get('code')}, message={result.get('message')}")
        result_data = result.get("data") or {}
        if not isinstance(result_data, dict):
            result_data = {}
        text_value = result_data.get("text") or ""
        return str(text_value).strip()
