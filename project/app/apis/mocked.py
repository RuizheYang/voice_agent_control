from typing import Optional, Any
from pydantic import BaseModel
from .voice_agent import VoiceAgentCancelResponse
from app.observability.events_bus import (
    event_bus,
    VoiceAgentCancelSuccessEvent,
)
import uuid

async def cancel_call(clueId: str) -> VoiceAgentCancelResponse:
    await event_bus.emit(VoiceAgentCancelSuccessEvent(
        clue_id=clueId,
        event_type="cancel_voice_agent_success",
        event_data={
            "response_code": 0,
            "response_message": "",
        }
    ))
    return VoiceAgentCancelResponse(
        success=True,
        message="Cancel success",
        data={"code": 0, "message": "Mocked cancel success response for testing"},
    )
