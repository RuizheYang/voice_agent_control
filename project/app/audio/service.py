from __future__ import annotations

import io
import logging
import uuid
import wave

import aiohttp

from app.audio.asr import NS0011Client

logger = logging.getLogger(__name__)

TARGET_SAMPLE_RATE = 8000


async def _download(url: str) -> bytes:
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.get(url, timeout=timeout) as response:
            response.raise_for_status()
            return await response.read()


def _decode_mp3(mp3_bytes: bytes, sample_rate: int = TARGET_SAMPLE_RATE) -> np.ndarray:
    import numpy as np
    from pydub import AudioSegment

    audio = AudioSegment.from_file(io.BytesIO(mp3_bytes), format="mp3")
    audio = audio.set_channels(1).set_frame_rate(sample_rate).set_sample_width(2)
    return np.asarray(audio.get_array_of_samples(), dtype=np.int16)


def _pcm_to_wav_bytes(pcm: np.ndarray, sample_rate: int) -> bytes:
    import numpy as np

    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        wav.writeframes(np.asarray(pcm, dtype=np.int16).tobytes())
    return buffer.getvalue()


class Mp3ToTextService:
    """mp3 -> VAD 裁剪 -> NS0011 一句话识别 -> text。"""

    def __init__(self, client: NS0011Client | None = None):
        self.client = client or NS0011Client()

    async def recognize(
        self,
        audio_url: str,
        *,
        order_id: str,
        trans_id: str | None = None,
    ) -> str:
        from app.audio.vad import SileroVAD

        trans_id = trans_id or uuid.uuid4().hex
        try:
            mp3_bytes = await _download(audio_url)
            pcm = _decode_mp3(mp3_bytes)
            vad = await SileroVAD.create_async(TARGET_SAMPLE_RATE)
            trimmed = vad.trim_speech(pcm)
            wav_bytes = _pcm_to_wav_bytes(trimmed, TARGET_SAMPLE_RATE)
        except Exception as exc:
            logger.warning("audio preprocessing failed, fallback to direct mp3 url: %s", exc)
            return await self.client.recognize(
                order_id=order_id,
                trans_id=trans_id,
                audio_file=audio_url,
                voice_format="mp3",
            )
        return await self.client.recognize(
            order_id=order_id,
            trans_id=trans_id,
            audio_content=wav_bytes,
            voice_format="wav",
        )
