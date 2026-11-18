"""Business-neutral facts that apply to every completed telephone call."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from app.summary.analyzers.base import AnalyzerDefinition


PROMPT = """
## Background
You are analyzing a completed telephone call. You will be given the transcript
of a conversation between a caller and the person or system that answered.
Classify the call against the criteria below.

## Rules
- Judge ONLY from what the transcript supports. Do not infer facts that were not stated.
- If a topic was never raised or is genuinely ambiguous, use "unknown".
- Use the exact value vocabulary specified for each field.

G1 - Voicemail / Voice assistant / Mailbox
  yes: the call was answered by a voicemail / voice assistant / mailbox - no live human interaction.
  no: answered by a live person.
  unknown: cannot tell from the transcript.

G2 - Customer hostility / opt-out
  yes: the person is clearly angry/hostile, explicitly says they do NOT want to be
       called again (opt-out / do-not-call), and/or uses profanity or abusive language.
  no: none of the above; the person is neutral or cooperative.
  unknown: cannot tell from the transcript.
  Note: mere reluctance or being busy is NOT "yes"; require explicit anger, an explicit
  opt-out, or abusive/profane language.

G3 - Explicitly require human follow-up
  yes: the person explicitly requests a follow-up call from a human rather than an AI agent.
  no: the person does not request a follow-up call from a human.
  unknown: cannot tell from the transcript.

G4 - Next call appointment / scheduling
  yes: the person explicitly agrees or asks for a specific time/date for a follow-up call.
  no: the person does not agree to a specific time/date for a follow-up call.
  unknown: cannot tell from the transcript.
  If the person only says "call me later", interpret that as half an hour later.
  If the person only says "call me tomorrow/someday", interpret that as 10:00am on that day.
  If the person provides minutes / hours / days, calculate the time from the reference time.
  If the person provides a specific time/date, use that time/date.
  When you provide the next call appointment, always use "YYYY-MM-DD HH:MM".
  If it cannot be determined, provide "no" for the next call appointment.

G5 - Call summarization (call_summary)
  ALWAYS provide a concise CHINESE summary of the call in 1-2 sentences (no more than 30 Chinese characters).
  This field is mandatory for every answered call and must NEVER be null, empty, or omitted.

## Runtime context
Reference time: {reference_time}
Transcript:
{transcript}

## Output
Return the classification using the provided structured schema.
"""


class CommonEvidence(BaseModel):
    is_voicemail: Literal["yes", "no", "unknown"] = Field(
        ..., description="G1 - Voicemail / Voice assistant / Mailbox (descriptive flag only)"
    )
    customer_hostility_opt_out: Literal["yes", "no", "unknown"] = Field(
        ..., description="G2 - Customer hostility / opt-out"
    )
    require_human_follow_up: Literal["yes", "no", "unknown"] = Field(
        ..., description="G3 - Explicitly require human follow-up"
    )
    has_next_call_appointment: Literal["yes", "no", "unknown"] = Field(
        ..., description="G4 - Next call appointment / scheduling"
    )
    next_call_time: str | None = Field(
        None,
        description="If G4 = yes, provide the next call appointment in 'YYYY-MM-DD HH:MM' format; otherwise None.",
    )
    call_summary: str = Field(
        ...,
        description="G5 - Call Summarization: ALWAYS provide a concise CHINESE summary of the call in 1-2 sentences (no more than 30 Chinese characters). Mandatory field.",
    )

    @classmethod
    def default(cls) -> "CommonEvidence":
        return cls(
            is_voicemail="unknown",
            customer_hostility_opt_out="unknown",
            require_human_follow_up="unknown",
            has_next_call_appointment="unknown",
            next_call_time=None,
            call_summary="",
        )


ANALYZER = AnalyzerDefinition(
    name="common_call_events",
    result_type=CommonEvidence,
    prompt=PROMPT,
    version="1",
    skip_default=CommonEvidence(
        is_voicemail="unknown",
        customer_hostility_opt_out="unknown",
        require_human_follow_up="unknown",
        has_next_call_appointment="unknown",
        next_call_time=None,
        call_summary="",
    ),
)
