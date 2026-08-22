"""Manual endpointing tests for locally supplied VAD boundaries."""

from unittest.mock import AsyncMock, patch

import pytest
from pipecat.frames.frames import VADUserStartedSpeakingFrame, VADUserStoppedSpeakingFrame
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.stt_service import WebsocketSTTService

from pipecat_sarvam import SarvamRealtimeSTTService


def build_service(endpointing: str) -> SarvamRealtimeSTTService:
    """Build a service with the socket and the metrics collector stubbed out."""
    service = SarvamRealtimeSTTService(
        api_key="test-key",
        settings=SarvamRealtimeSTTService.Settings(endpointing=endpointing),
    )
    service._send_json = AsyncMock()  # type: ignore[method-assign]
    service.start_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.start_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    service._reset_stt_ttfb_state = AsyncMock()  # type: ignore[method-assign]
    return service


async def drive_one_turn(service: SarvamRealtimeSTTService) -> None:
    """Feed one local speech start and stop through the adapter's own branch."""
    with patch.object(WebsocketSTTService, "process_frame", new=AsyncMock()):
        await service.process_frame(
            VADUserStartedSpeakingFrame(start_secs=0.2), FrameDirection.DOWNSTREAM
        )
        await service.process_frame(
            VADUserStoppedSpeakingFrame(stop_secs=0.8), FrameDirection.DOWNSTREAM
        )


@pytest.mark.asyncio
async def test_manual_endpointing_forwards_local_vad_boundaries() -> None:
    """Local VAD should become Sarvam speech_start and speech_end events."""
    service = build_service("manual")

    await drive_one_turn(service)

    events = [call.args[0] for call in service._send_json.await_args_list]
    assert events == [{"event": "speech_start"}, {"event": "speech_end"}]


@pytest.mark.asyncio
async def test_manual_endpointing_restores_the_sarvam_ttfb_anchor() -> None:
    """Both endpointing modes must measure speech start to first transcript.

    ``STTService._handle_vad_user_stopped_speaking`` re-anchors TTFB to the VAD
    speech end, which would otherwise make manual mode report a far smaller
    number than the benchmark in the README defines.
    """
    service = build_service("manual")

    with patch.object(WebsocketSTTService, "process_frame", new=AsyncMock()):
        await service.process_frame(
            VADUserStartedSpeakingFrame(start_secs=0.2), FrameDirection.DOWNSTREAM
        )
        anchor = service._utterance_start_time
        assert anchor > 0
        service.start_ttfb_metrics.reset_mock()
        await service.process_frame(
            VADUserStoppedSpeakingFrame(stop_secs=0.8), FrameDirection.DOWNSTREAM
        )

    service.start_ttfb_metrics.assert_awaited_once_with(start_time=anchor)
    assert service._user_speaking is False


@pytest.mark.asyncio
async def test_server_endpointing_ignores_local_vad_frames() -> None:
    """With server VAD, Sarvam owns the boundaries; do not signal them twice."""
    service = build_service("vad")

    await drive_one_turn(service)

    service._send_json.assert_not_awaited()
    service.start_ttfb_metrics.assert_not_awaited()
