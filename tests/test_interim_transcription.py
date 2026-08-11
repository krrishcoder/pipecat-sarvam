"""Interim transcription mapping tests."""

from unittest.mock import AsyncMock

import pytest
from pipecat.frames.frames import InterimTranscriptionFrame

from pipecat_sarvam import SarvamRealtimeSTTService


@pytest.mark.asyncio
async def test_partial_transcript_maps_to_interim_frame() -> None:
    """Non-empty Sarvam partials should remain interim downstream."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service.push_frame = AsyncMock()  # type: ignore[method-assign]

    await service._handle_partial_transcript(
        {
            "event": "transcript.partial",
            "utterance_idx": 0,
            "text": "फसल तैयार",
        }
    )

    frame = service.push_frame.await_args.args[0]
    assert isinstance(frame, InterimTranscriptionFrame)
    assert frame.text == "फसल तैयार"


@pytest.mark.asyncio
async def test_empty_partial_is_not_emitted() -> None:
    """Sarvam can send an initial empty partial; it should not create a frame."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service.push_frame = AsyncMock()  # type: ignore[method-assign]

    await service._handle_partial_transcript(
        {
            "event": "transcript.partial",
            "utterance_idx": 0,
            "text": "",
        }
    )

    service.push_frame.assert_not_awaited()
