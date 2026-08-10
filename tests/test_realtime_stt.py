"""Tests for the Sarvam realtime STT Pipecat adapter."""

import base64
import json
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest
from pipecat.frames.frames import (
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    MetricsFrame,
    TranscriptionFrame,
)
from pipecat.metrics.metrics import ProcessingMetricsData, TTFBMetricsData
from pipecat.processors.frame_processor import FrameDirection
from pipecat.turns.user_turn_strategies import ExternalUserTurnStrategies
from websockets.protocol import State

import pipecat_sarvam
from pipecat_sarvam import SarvamRealtimeSTTService


class RecordingWebSocket:
    """Minimal open WebSocket used by send-path tests."""

    state = State.OPEN

    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, message: str) -> None:
        self.messages.append(message)


def test_package_exports_realtime_service() -> None:
    """The public package should expose its version and service class."""
    assert pipecat_sarvam.__version__ == "0.1.0.dev0"
    assert pipecat_sarvam.SarvamRealtimeSTTService is SarvamRealtimeSTTService


def test_default_url_targets_realtime_model() -> None:
    """Connection URL should contain the validated realtime defaults."""
    service = SarvamRealtimeSTTService(api_key="test-key")

    parsed = urlsplit(service._build_websocket_url())
    query = parse_qs(parsed.query)

    assert parsed.path == "/speech-to-text-realtime/ws"
    assert query["model"] == ["saaras:v3-realtime"]
    assert query["language_code"] == ["hi-IN"]
    assert query["stream_type"] == ["fast"]
    assert query["encoding"] == ["linear16"]
    assert query["sample_rate"] == ["16000"]


def test_rejects_non_realtime_model() -> None:
    """The dedicated endpoint must not accept legacy Sarvam models."""
    with pytest.raises(ValueError, match="saaras:v3-realtime"):
        SarvamRealtimeSTTService(
            api_key="test-key",
            settings=SarvamRealtimeSTTService.Settings(model="saaras:v3"),
        )


def test_server_vad_recommends_external_turn_strategies() -> None:
    """Server-owned speech boundaries should advertise external strategies."""
    service = SarvamRealtimeSTTService(api_key="test-key")

    metadata = service.service_metadata_frame()

    assert not service.supports_ttfs
    assert metadata.ttfs_p99_latency == 0.0
    assert isinstance(metadata.user_turn_strategies, ExternalUserTurnStrategies)


def test_rejects_encoded_audio_until_transcoding_is_supported() -> None:
    """Initial service input is deliberately limited to Pipecat's linear16 PCM."""
    with pytest.raises(ValueError, match="Only linear16"):
        SarvamRealtimeSTTService(
            api_key="test-key",
            settings=SarvamRealtimeSTTService.Settings(encoding="mulaw"),
        )


@pytest.mark.asyncio
async def test_run_stt_sends_base64_audio_in_capped_chunks() -> None:
    """Fast-mode audio messages must stay within the observed 16,000-byte cap."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    websocket = RecordingWebSocket()
    service._websocket = websocket  # type: ignore[assignment]
    service._session_ready.set()

    results = [result async for result in service.run_stt(b"\x01\x02" * 10000)]

    assert results == [None]
    assert len(websocket.messages) == 2
    payloads = [json.loads(message) for message in websocket.messages]
    chunks = [base64.b64decode(payload["audio"]) for payload in payloads]
    assert [len(chunk) for chunk in chunks] == [16000, 4000]
    assert all(payload["event"] == "audio_input" for payload in payloads)


@pytest.mark.asyncio
async def test_input_audio_frame_is_forwarded_without_buffering() -> None:
    """Pipecat input PCM should reach run_stt unchanged and immediately."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    received_audio: list[bytes] = []

    async def capture_audio(audio: bytes):
        received_audio.append(audio)
        yield None

    service.run_stt = capture_audio  # type: ignore[method-assign]
    frame = InputAudioRawFrame(
        audio=b"\x01\x02" * 160,
        sample_rate=16000,
        num_channels=1,
    )

    await service.process_audio_frame(frame, FrameDirection.DOWNSTREAM)

    assert received_audio == [frame.audio]


@pytest.mark.asyncio
async def test_maps_partial_and_final_transcripts() -> None:
    """Provider partial/final events should become the corresponding Pipecat frames."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service._request_id = "request-123"
    service.push_frame = AsyncMock()  # type: ignore[method-assign]
    service.emit_stt_usage_metrics = AsyncMock()  # type: ignore[method-assign]
    service.start_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.stop_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.start_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    service.stop_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    service._trace_transcription = AsyncMock()  # type: ignore[method-assign]
    await service._begin_utterance_metrics()

    await service._handle_message(
        {
            "event": "transcript.partial",
            "utterance_idx": 0,
            "text": "केशव",
        }
    )
    partial = service.push_frame.await_args_list[0].args[0]

    await service._handle_message(
        {
            "event": "transcript.final",
            "utterance_idx": 0,
            "text": "केशव के घर में चार खिड़कियाँ हैं।",
            "start_s": 0.5,
            "end_s": 4.2,
        }
    )
    final = service.push_frame.await_args_list[1].args[0]

    assert isinstance(partial, InterimTranscriptionFrame)
    assert partial.text == "केशव"
    assert partial.result["request_id"] == "request-123"
    assert isinstance(final, TranscriptionFrame)
    assert final.finalized
    assert final.result["start_s"] == 0.5
    assert final.result["request_id"] == "request-123"
    service.start_ttfb_metrics.assert_awaited_once()
    service.stop_ttfb_metrics.assert_awaited_once()
    service.start_processing_metrics.assert_awaited_once()
    service.stop_processing_metrics.assert_awaited_once()


@pytest.mark.asyncio
async def test_maps_server_vad_and_interruption() -> None:
    """Speech-start/end events should broadcast speaking frames and barge-in."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service.start_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.start_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    service.broadcast_frame = AsyncMock()  # type: ignore[method-assign]
    service.broadcast_interruption = AsyncMock()  # type: ignore[method-assign]

    await service._handle_message(
        {"event": "vad.speech_start", "utterance_idx": 0, "confidence": 0.98}
    )
    await service._handle_message(
        {"event": "vad.speech_end", "utterance_idx": 0, "confidence": 0.01}
    )

    assert service.broadcast_frame.await_args_list[0].args[0].__name__ == "UserStartedSpeakingFrame"
    assert service.broadcast_frame.await_args_list[1].args[0].__name__ == "UserStoppedSpeakingFrame"
    service.broadcast_interruption.assert_awaited_once()
    service.start_ttfb_metrics.assert_awaited_once()
    service.start_processing_metrics.assert_awaited_once()


@pytest.mark.asyncio
async def test_emits_first_and_final_transcript_latency_metrics() -> None:
    """Pipecat metrics should capture speech-to-first and speech-to-final latency."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service._enable_metrics = True
    service.push_frame = AsyncMock()  # type: ignore[method-assign]

    await service._begin_utterance_metrics()
    await service._mark_first_transcript_received()
    await service._finish_utterance_metrics()

    metrics_frames = [
        call.args[0]
        for call in service.push_frame.await_args_list
        if isinstance(call.args[0], MetricsFrame)
    ]
    assert len(metrics_frames) == 2
    assert isinstance(metrics_frames[0].data[0], TTFBMetricsData)
    assert isinstance(metrics_frames[1].data[0], ProcessingMetricsData)
    assert metrics_frames[0].data[0].value >= 0
    assert metrics_frames[1].data[0].value >= 0


@pytest.mark.asyncio
async def test_live_setting_update_uses_config_update() -> None:
    """Runtime-supported settings should update without reconnecting."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service._websocket = RecordingWebSocket()  # type: ignore[assignment]

    changed = await service.update_config(mode="codemix")

    payload = json.loads(service._websocket.messages[0])  # type: ignore[union-attr]
    assert changed.keys() >= {"mode"}
    assert payload == {
        "event": "config.update",
        "mode": "codemix",
    }
