"""Barge-in tests for Sarvam server-side VAD."""

from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pipecat.frames.frames import (
    InterruptionFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)

from pipecat_sarvam import SarvamRealtimeSTTService


@dataclass
class BotAudioSink:
    """Small output model that stops when Pipecat interrupts."""

    speaking: bool = True
    stopped_by_interruption: bool = False

    def handle(self, frame_class: type) -> None:
        if frame_class is InterruptionFrame:
            self.speaking = False
            self.stopped_by_interruption = True


@pytest.mark.parametrize("phrase", ["नहीं", "हाँ", "रुकिए", "एक मिनट"])
@pytest.mark.asyncio
async def test_short_farmer_interruption_stops_bot_and_is_transcribed(phrase: str) -> None:
    """A short VAD turn should stop bot audio and still produce a final transcript."""
    service = SarvamRealtimeSTTService(
        api_key="test-key",
        should_interrupt=True,
        enable_direct_mode=True,
    )
    bot_output = BotAudioSink()
    observed: list[str] = []
    final_frames: list[TranscriptionFrame] = []

    async def capture_broadcast(frame_class: type) -> None:
        observed.append(frame_class.__name__)
        bot_output.handle(frame_class)

    async def capture_push(frame: Any, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        if isinstance(frame, TranscriptionFrame):
            observed.append(type(frame).__name__)
            final_frames.append(frame)

    service.broadcast_frame = capture_broadcast  # type: ignore[method-assign]
    service.push_frame = capture_push  # type: ignore[method-assign]
    service.start_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.stop_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.start_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    service.stop_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    service.stop_all_metrics = AsyncMock()  # type: ignore[method-assign]
    service.emit_stt_usage_metrics = AsyncMock()  # type: ignore[method-assign]
    service._trace_transcription = AsyncMock()  # type: ignore[method-assign]

    assert bot_output.speaking
    await service._handle_message(
        {"event": "vad.speech_start", "utterance_idx": 0, "confidence": 0.99}
    )
    await service._handle_message(
        {"event": "vad.speech_end", "utterance_idx": 0, "confidence": 0.01}
    )
    await service._handle_message(
        {
            "event": "transcript.final",
            "utterance_idx": 0,
            "text": phrase,
        }
    )

    assert observed == [
        UserStartedSpeakingFrame.__name__,
        InterruptionFrame.__name__,
        UserStoppedSpeakingFrame.__name__,
        TranscriptionFrame.__name__,
    ]
    assert bot_output.stopped_by_interruption
    assert not bot_output.speaking
    assert final_frames[0].text == phrase
    assert final_frames[0].finalized


@pytest.mark.asyncio
async def test_interruption_can_be_disabled() -> None:
    """VAD still reports speech when automatic barge-in is disabled."""
    service = SarvamRealtimeSTTService(api_key="test-key", should_interrupt=False)
    service.start_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.start_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    service.broadcast_frame = AsyncMock()  # type: ignore[method-assign]
    service.broadcast_interruption = AsyncMock()  # type: ignore[method-assign]

    await service._handle_message(
        {"event": "vad.speech_start", "utterance_idx": 0, "confidence": 0.99}
    )

    service.broadcast_interruption.assert_not_awaited()
