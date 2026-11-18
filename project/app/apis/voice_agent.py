import asyncio
import logging
from typing import Any, Optional, Dict
import aiohttp
from pydantic import BaseModel

from app.apis.api_calls import do_post
from app.config import config
from app.observability.events_bus import (
    event_bus, 
    VoiceAgentCancelSuccessEvent,
)

logger = logging.getLogger(__name__)


class VoiceAgentCancelResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None

    @classmethod
    def fail(cls, message: str = "Request failed") -> "VoiceAgentCancelResponse":
        return cls(success=False, message=message)

    def is_success(self) -> bool:
        return self.success


async def cancel_call(clueId: str) -> VoiceAgentCancelResponse:
    """取消外呼任务"""
    # 精简配置不再提供旧取消接口，缺省时跳过远端请求。
    url = getattr(config, "voice_agent_cancel_url", "")
    if not url:
        return VoiceAgentCancelResponse.fail("Cancel endpoint is not configured")

    try:
        status, text, result = await do_post(
            url=url, 
            headers={}, 
            payload={"clueId": clueId}, 
            timeout_seconds=3,
            clue_id=clueId,
        )
        if status >= 400:
            logger.error(f"[VoiceAgent] clueId={clueId} 取消外呼接口调用失败. status={status} body={text}")
            return VoiceAgentCancelResponse.fail("Cancel request failed")
        
        if result and result.get("code", 1) != 0:
            logger.info(f"[VoiceAgent] clueId={clueId} 外呼已发起，取消失败: {result}")
            return VoiceAgentCancelResponse.fail(result.get("message", "Cancel failed"))

        logger.info(f"[VoiceAgent] clueId={clueId} 取消外呼成功. response: {result}")
        await event_bus.emit(VoiceAgentCancelSuccessEvent(
            clue_id=clueId,
            event_type="voice_agent_cancel_success",
            event_data={
                "response_code": result.get("code") if result else None,
                "response_message": result.get("message") if result else None,
            }
        ))
        return VoiceAgentCancelResponse(
            success=True,
            message="Cancel success",
            data=result,
        )
    except aiohttp.ClientError as e:
        logger.error(f"[VoiceAgent] clueId={clueId} 取消外呼接口异常: {e}")
        return VoiceAgentCancelResponse.fail("Client error")
    except asyncio.TimeoutError:
        logger.error(f"[VoiceAgent] clueId={clueId} 取消外呼接口超时.")
        return VoiceAgentCancelResponse.fail("Timeout error")
    except Exception as e:
        logger.error(f"[VoiceAgent] clueId={clueId} 取消外呼发生未知错误: {e}")
        return VoiceAgentCancelResponse.fail("Unknown error")
