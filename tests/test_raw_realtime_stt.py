"""Tests for the provider-only Sarvam realtime protocol client."""

import argparse
import base64
import json
import wave
from pathlib import Path
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest

from examples.raw_realtime_stt import build_websocket_url, inspect_audio, send_audio


def write_pcm_wav(path: Path, *, sample_rate: int = 16000) -> None:
    """Write a short mono linear16 WAV fixture."""
    with wave.open(str(path), "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(b"\x00\x00" * (sample_rate // 10))


def test_inspect_audio_accepts_supported_wav(tmp_path: Path) -> None:
    """A 16 kHz mono linear16 WAV should pass validation."""
    audio_path = tmp_path / "hindi.wav"
    write_pcm_wav(audio_path)

    source = inspect_audio(audio_path, raw_sample_rate=16000)

    assert source.sample_rate == 16000
    assert source.duration_seconds == pytest.approx(0.1)
    assert source.is_wav


def test_inspect_audio_rejects_unsupported_sample_rate(tmp_path: Path) -> None:
    """The realtime endpoint accepts only 8 kHz or 16 kHz input."""
    audio_path = tmp_path / "hindi.wav"
    write_pcm_wav(audio_path, sample_rate=24000)

    with pytest.raises(ValueError, match="8000 or 16000"):
        inspect_audio(audio_path, raw_sample_rate=16000)


def test_build_websocket_url_uses_realtime_protocol_parameters() -> None:
    """Connection settings should target the dedicated realtime model."""
    args = argparse.Namespace(
        base_url="wss://api.sarvam.ai/speech-to-text-realtime/ws",
        language_code="hi-IN",
        stream_type="fast",
        mode="transcribe",
        return_timestamps=True,
    )

    url = build_websocket_url(args, sample_rate=16000)
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)

    assert parsed.path == "/speech-to-text-realtime/ws"
    assert query == {
        "encoding": ["linear16"],
        "endpointing": ["vad"],
        "language_code": ["hi-IN"],
        "mode": ["transcribe"],
        "model": ["saaras:v3-realtime"],
        "return_timestamps": ["true"],
        "sample_rate": ["16000"],
        "stream_type": ["fast"],
    }


@pytest.mark.asyncio
async def test_send_audio_chunks_trailing_silence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Trailing silence must stay below Sarvam's per-frame size cap."""
    audio_path = tmp_path / "hindi.wav"
    write_pcm_wav(audio_path)
    source = inspect_audio(audio_path, raw_sample_rate=16000)
    sent_messages: list[str] = []

    class RecordingWebSocket:
        async def send(self, message: str) -> None:
            sent_messages.append(message)

    monkeypatch.setattr("examples.raw_realtime_stt.asyncio.sleep", AsyncMock())

    await send_audio(
        RecordingWebSocket(),  # type: ignore[arg-type]
        source,
        chunk_ms=100,
        trailing_silence_ms=1000,
        max_audio_seconds=1.0,
    )

    decoded_chunks = [base64.b64decode(json.loads(message)["audio"]) for message in sent_messages]
    assert len(decoded_chunks) == 11
    assert all(len(chunk) <= 3200 for chunk in decoded_chunks)
