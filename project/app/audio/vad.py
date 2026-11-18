from pathlib import Path
import asyncio
import numpy as np

MODEL_PATH = Path(__file__).parent / "model_asset" / "silero_vad.onnx"

class SileroVAD:
    """Minimal one-shot Silero VAD wrapper for 8k/16k PCM int16 audio."""

    def __init__(self, sample_rate: int = 16000):
        if sample_rate not in (8000, 16000):
            raise ValueError("Silero VAD supports only 8000 or 16000 Hz")
        import onnxruntime
        self.sample_rate = sample_rate
        self.frame_size = 512 if sample_rate == 16000 else 256
        self.context_size = 64 if sample_rate == 16000 else 32
        opts = onnxruntime.SessionOptions()
        opts.inter_op_num_threads = 1
        opts.intra_op_num_threads = 1
        self.session = onnxruntime.InferenceSession(
            str(MODEL_PATH), providers=["CPUExecutionProvider"], sess_options=opts
        )
        self.reset()

    @classmethod
    async def create_async(cls, sample_rate: int = 16000) -> "SileroVAD":
        if sample_rate not in (8000, 16000):
            raise ValueError("Silero VAD supports only 8000 or 16000 Hz")
        import onnxruntime
        opts = onnxruntime.SessionOptions()
        opts.inter_op_num_threads = 1
        opts.intra_op_num_threads = 1
        session = await asyncio.to_thread(
            onnxruntime.InferenceSession,
            str(MODEL_PATH), providers=["CPUExecutionProvider"], sess_options=opts
        )
        instance = cls.__new__(cls)
        instance.sample_rate = sample_rate
        instance.frame_size = 512 if sample_rate == 16000 else 256
        instance.context_size = 64 if sample_rate == 16000 else 32
        instance.session = session
        instance.reset()
        return instance

    def reset(self):
        self._state = np.zeros((2, 1, 128), dtype=np.float32)
        self._context = np.zeros((1, 0), dtype=np.float32)

    def _prob(self, frame: np.ndarray) -> float:
        if np.shape(self._context)[1] == 0:
            self._context = np.zeros((1, self.context_size), dtype=np.float32)
        x = np.concatenate((self._context, frame), axis=1).astype(np.float32)
        out, state = self.session.run(
            None,
            {
                "input": x,
                "state": self._state,
                "sr": np.array(self.sample_rate, dtype=np.int64),
            },
        )
        self._state = state
        self._context = x[..., -self.context_size:]
        return float(np.asarray(out).reshape(-1)[0])

    def probabilities(self, pcm: np.ndarray) -> np.ndarray:
        pcm = np.asarray(pcm, dtype=np.int16).reshape(-1)
        probs = []
        for start in range(0, len(pcm), self.frame_size):
            frame = pcm[start : start + self.frame_size]
            if frame.size < self.frame_size:
                frame = np.pad(frame, (0, self.frame_size - frame.size))
            audio = (frame.astype(np.float32) / 32768.0).reshape(1, -1)
            probs.append(self._prob(audio))
        return np.array(probs, dtype=np.float32)

    def trim_speech(self, pcm: np.ndarray, threshold: float = 0.5, padding_frames: int = 5) -> np.ndarray:
        pcm = np.asarray(pcm, dtype=np.int16).reshape(-1)
        probs = self.probabilities(pcm)
        speech = probs >= threshold
        if not speech.any():
            return pcm
        padded = np.zeros_like(speech)
        for idx, active in enumerate(speech):
            if not active:
                continue
            start = max(0, idx - padding_frames)
            end = min(len(speech), idx + padding_frames + 1)
            padded[start:end] = True
        sample_indices = []
        for idx, active in enumerate(padded):
            if active:
                sample_indices.append(slice(idx * self.frame_size, min((idx + 1) * self.frame_size, len(pcm))))
        if not sample_indices:
            return pcm
        return np.concatenate([pcm[s] for s in sample_indices])
