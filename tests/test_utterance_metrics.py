"""Latency-measurement tests for the interruption/metrics ordering.

``FrameProcessor.broadcast_interruption`` calls ``stop_all_metrics``, and
``FrameProcessor.process_frame`` does the same for every inbound
``InterruptionFrame``. Both would flush an utterance measurement that had only
just started, so the service reported a few milliseconds of internal overhead
instead of the latency to the first transcript. These tests pin the ordering
and the re-arm that keep the README's benchmark honest.
"""

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest
from pipecat.frames.frames import InterruptionFrame, MetricsFrame
from pipecat.metrics.metrics import ProcessingMetricsData, TTFBMetricsData
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.stt_service import WebsocketSTTService

from pipecat_sarvam import SarvamRealtimeSTTService

FIRST_TRANSCRIPT_DELAY = 0.05
INTERRUPTION_WORK = 0.01

SPEECH_START = {"event": "vad.speech_start", "utterance_idx": 0}
PARTIAL = {"event": "transcript.partial", "utterance_idx": 0, "text": "हेलो"}
FINAL = {"event": "transcript.final", "utterance_idx": 0, "text": "हेलो।"}


def build_service(**kwargs) -> SarvamRealtimeSTTService:
    """Build a service whose broadcasts are recorded rather than pushed."""
    service = SarvamRealtimeSTTService(api_key="test-key", **kwargs)
    service.broadcast_frame = AsyncMock()  # type: ignore[method-assign]
    service._reset_stt_ttfb_state = AsyncMock()  # type: ignore[method-assign]
    return service


@pytest.mark.asyncio
async def test_speech_start_starts_metrics_after_the_interruption() -> None:
    """The interruption must be dispatched before the timers start.

    ``broadcast_interruption`` flushes every active metric, so starting the
    timers first reported the flush latency instead of the transcript latency.
    """
    service = build_service(should_interrupt=True)
    order: list[str] = []

    async def broadcast_interruption() -> None:
        order.append("interruption")

    async def start_ttfb_metrics(*, start_time=None) -> None:
        order.append("start_ttfb")

    service.broadcast_interruption = broadcast_interruption  # type: ignore[method-assign]
    service.start_ttfb_metrics = start_ttfb_metrics  # type: ignore[method-assign]
    service.start_processing_metrics = AsyncMock()  # type: ignore[method-assign]

    await service._handle_message(SPEECH_START)

    assert order == ["interruption", "start_ttfb"]


@pytest.mark.asyncio
async def test_speech_start_anchors_metrics_before_the_interruption() -> None:
    """Both timers share an anchor captured before any broadcast work."""
    service = build_service(should_interrupt=True)
    service.start_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.start_processing_metrics = AsyncMock()  # type: ignore[method-assign]

    async def slow_interruption() -> None:
        await asyncio.sleep(INTERRUPTION_WORK)

    service.broadcast_interruption = slow_interruption  # type: ignore[method-assign]

    before = time.time()
    await service._handle_message(SPEECH_START)
    after = time.time()

    anchor = service._utterance_start_time
    assert before <= anchor <= after
    # The anchor predates the interruption, so it is already in the past when the
    # timers finally start.
    assert after - anchor >= INTERRUPTION_WORK
    service.start_ttfb_metrics.assert_awaited_once_with(start_time=anchor)
    service.start_processing_metrics.assert_awaited_once_with(start_time=anchor)


@pytest.mark.asyncio
async def test_reported_latency_survives_the_interruption_flush() -> None:
    """End to end: the emitted metric values must reflect real elapsed time.

    This is the regression the browser harness surfaced. Every utterance
    reported single-digit milliseconds because the interruption broadcast
    flushed the timers roughly one millisecond after they started. The stand-in
    ``broadcast_interruption`` keeps the real ``stop_all_metrics`` call, which is
    the part the ordering has to survive.
    """
    service = build_service(should_interrupt=True)
    service._enable_metrics = True
    service.push_frame = AsyncMock()  # type: ignore[method-assign]
    service.emit_stt_usage_metrics = AsyncMock()  # type: ignore[method-assign]
    service._trace_transcription = AsyncMock()  # type: ignore[method-assign]

    async def broadcast_interruption() -> None:
        await service.stop_all_metrics()

    service.broadcast_interruption = broadcast_interruption  # type: ignore[method-assign]

    await service._handle_message(SPEECH_START)
    await asyncio.sleep(FIRST_TRANSCRIPT_DELAY)
    await service._handle_message(PARTIAL)
    await service._handle_message(FINAL)

    metrics = [
        data
        for call in service.push_frame.await_args_list
        if isinstance(call.args[0], MetricsFrame)
        for data in call.args[0].data
    ]
    ttfb = [data.value for data in metrics if isinstance(data, TTFBMetricsData)]
    processing = [data.value for data in metrics if isinstance(data, ProcessingMetricsData)]

    assert len(ttfb) == 1
    assert len(processing) == 1
    assert ttfb[0] >= FIRST_TRANSCRIPT_DELAY
    assert processing[0] >= FIRST_TRANSCRIPT_DELAY


@pytest.mark.asyncio
async def test_interruption_frame_rearms_a_pending_measurement() -> None:
    """An externally broadcast interruption must not lose the anchor.

    With ``endpointing="manual"`` the interruption originates upstream, so the
    service sees it as an inbound frame after its own timers have started.
    """
    service = build_service(
        settings=SarvamRealtimeSTTService.Settings(endpointing="manual"),
    )
    service._send_json = AsyncMock()  # type: ignore[method-assign]
    service.start_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.start_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    await service._begin_utterance_metrics()
    anchor = service._utterance_start_time
    service.start_ttfb_metrics.reset_mock()
    service.start_processing_metrics.reset_mock()

    with patch.object(WebsocketSTTService, "process_frame", new=AsyncMock()):
        await service.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)

    service.start_ttfb_metrics.assert_awaited_once_with(start_time=anchor)
    service.start_processing_metrics.assert_awaited_once_with(start_time=anchor)


@pytest.mark.asyncio
async def test_interruption_between_utterances_does_not_rearm() -> None:
    """A finished measurement must stay finished."""
    service = build_service()
    service.start_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.start_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    service.stop_ttfb_metrics = AsyncMock()  # type: ignore[method-assign]
    service.stop_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    await service._begin_utterance_metrics()
    await service._finish_utterance_metrics()
    service.start_ttfb_metrics.reset_mock()
    service.start_processing_metrics.reset_mock()

    with patch.object(WebsocketSTTService, "process_frame", new=AsyncMock()):
        await service.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)

    service.start_ttfb_metrics.assert_not_awaited()
    service.start_processing_metrics.assert_not_awaited()
