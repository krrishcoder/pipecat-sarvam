"""Final transcription mapping tests."""

from unittest.mock import AsyncMock

import pytest
from pipecat.frames.frames import TranscriptionFrame

from pipecat_sarvam import SarvamRealtimeSTTService


@pytest.mark.asyncio
async def test_final_transcript_is_marked_finalized() -> None:
    """Sarvam final events should produce finalized Pipecat frames."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service._request_id = "request-123"
    service.push_frame = AsyncMock()  # type: ignore[method-assign]
    service.emit_stt_usage_metrics = AsyncMock()  # type: ignore[method-assign]
    service.stop_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    service._trace_transcription = AsyncMock()  # type: ignore[method-assign]

    await service._handle_final_transcript(
        {
            "event": "transcript.final",
            "utterance_idx": 2,
            "text": "कृपया सिंचाई बंद कर दीजिए।",
        }
    )

    frame = service.push_frame.await_args.args[0]
    assert isinstance(frame, TranscriptionFrame)
    assert frame.finalized
    assert frame.text == "कृपया सिंचाई बंद कर दीजिए।"
    assert frame.result["request_id"] == "request-123"
