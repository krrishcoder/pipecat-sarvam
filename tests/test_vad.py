"""Server-side VAD mapping tests."""

from unittest.mock import AsyncMock

import pytest
from pipecat.frames.frames import UserStartedSpeakingFrame, UserStoppedSpeakingFrame

from pipecat_sarvam import SarvamRealtimeSTTService


@pytest.mark.asyncio
async def test_server_vad_maps_to_user_speaking_frames() -> None:
    """Sarvam speech boundaries should become Pipecat speaking boundaries."""
    service = SarvamRealtimeSTTService(api_key="test-key", should_interrupt=False)
    service.start_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.start_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    service.broadcast_frame = AsyncMock()  # type: ignore[method-assign]

    await service._handle_message(
        {"event": "vad.speech_start", "utterance_idx": 0, "confidence": 0.99}
    )
    await service._handle_message(
        {"event": "vad.speech_end", "utterance_idx": 0, "confidence": 0.01}
    )

    assert service.broadcast_frame.await_args_list[0].args[0] is UserStartedSpeakingFrame
    assert service.broadcast_frame.await_args_list[1].args[0] is UserStoppedSpeakingFrame
