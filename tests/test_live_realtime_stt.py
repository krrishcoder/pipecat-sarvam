"""Opt-in live test for the Pipecat Sarvam realtime adapter."""

import asyncio
import os
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

import pytest
from pipecat.frames.frames import TranscriptionFrame

from examples.raw_realtime_stt import audio_chunks, get_api_key, inspect_audio
from pipecat_sarvam import SarvamRealtimeSTTService


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_adapter_transcribes_hindi_wav() -> None:
    """Connect through the adapter and receive one finalized Pipecat frame."""
    if os.getenv("RUN_SARVAM_INTEGRATION") != "1":
        pytest.skip("set RUN_SARVAM_INTEGRATION=1 to call the live Sarvam API")

    audio_path = Path("test_audio/hindi.wav")
    if not audio_path.is_file():
        pytest.skip("test_audio/hindi.wav is required for the live integration test")

    service = SarvamRealtimeSTTService(
        api_key=get_api_key(Path(".env")),
        keepalive_timeout=None,
    )
    source = inspect_audio(audio_path, raw_sample_rate=16000)
    final_frames: list[TranscriptionFrame] = []
    final_received = asyncio.Event()
    errors: list[str] = []

    def create_task(
        coroutine: Coroutine[Any, Any, Any],
        name: str | None = None,
        context: Any = None,
    ) -> asyncio.Task:
        del context
        return asyncio.create_task(coroutine, name=name)

    async def cancel_task(task: asyncio.Task, timeout: float | None = None) -> None:
        del timeout
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def capture_frame(frame: Any, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        if isinstance(frame, TranscriptionFrame):
            final_frames.append(frame)
            final_received.set()

    async def no_op(*args: Any, **kwargs: Any) -> None:
        del args, kwargs

    async def capture_error(error_msg: str, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        errors.append(error_msg)

    service.create_task = create_task  # type: ignore[method-assign]
    service.cancel_task = cancel_task  # type: ignore[method-assign]
    service.push_frame = capture_frame  # type: ignore[method-assign]
    service.broadcast_frame = no_op  # type: ignore[method-assign]
    service.broadcast_interruption = no_op  # type: ignore[method-assign]
    service.start_ttfb_metrics = no_op  # type: ignore[method-assign]
    service.stop_ttfb_metrics = no_op  # type: ignore[method-assign]
    service.start_processing_metrics = no_op  # type: ignore[method-assign]
    service.stop_processing_metrics = no_op  # type: ignore[method-assign]
    service.emit_stt_usage_metrics = no_op  # type: ignore[method-assign]
    service._trace_transcription = no_op  # type: ignore[method-assign]
    service.push_error = capture_error  # type: ignore[method-assign]

    await service._connect()
    try:
        streamed_seconds = 0.0
        for chunk, duration in audio_chunks(source, chunk_ms=100):
            if streamed_seconds >= 5.0:
                break
            async for _ in service.run_stt(chunk):
                pass
            await asyncio.sleep(duration)
            streamed_seconds += duration

        silence_chunk = b"\x00" * (source.sample_rate * source.frame_width // 10)
        for _ in range(15):
            async for _ in service.run_stt(silence_chunk):
                pass
            await asyncio.sleep(0.1)

        await asyncio.wait_for(final_received.wait(), timeout=5.0)
    finally:
        await service._disconnect()

    assert not errors
    assert final_frames
    assert final_frames[0].finalized
    assert final_frames[0].text
