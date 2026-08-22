"""Runnable Sarvam realtime STT pipeline.

A complete, single-file Pipecat bot: microphone in, Sarvam `saaras:v3-realtime`
transcription out. It prints each partial hypothesis as it arrives and then the
finalized transcript, with the elapsed time since Sarvam detected speech, so the
head start that partials give you is visible in the terminal.

Install::

    pip install "pipecat-sarvam"
    pip install "pipecat-ai[runner,webrtc]"

Run::

    export SARVAM_API_KEY="your-api-key"
    python examples/realtime_stt_bot.py -t webrtc

Then open the URL the runner prints and allow microphone access. Speak Hindi (or
change ``language_code`` below), and watch partials stream in ahead of the final.

This pipeline is deliberately transcription-only, so it needs exactly one API
key. To turn it into a voice agent, insert a context aggregator, an LLM, and a
TTS service between ``stt`` and ``transport.output()``; barge-in then works
without further configuration, because the service broadcasts a Pipecat
interruption as soon as Sarvam reports speech.

Note that no local VAD analyzer is configured. With ``endpointing="vad"`` Sarvam
performs turn detection server-side and the service advertises
``ExternalUserTurnStrategies``, so there is no Silero model to download and no
``pipecat-ai[silero]`` dependency.
"""

import os
import time

from loguru import logger
from pipecat.frames.frames import (
    ErrorFrame,
    Frame,
    InterimTranscriptionFrame,
    InterruptionFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import create_transport
from pipecat.transports.base_transport import TransportParams
from pipecat.workers.runner import WorkerRunner

from pipecat_sarvam import SarvamRealtimeSTTService

# Sarvam accepts 8000 or 16000. The service's own value is authoritative — Pipecat's
# audio_in_sample_rate does not override it — so both are set to the same number to
# keep the pipeline honest.
SAMPLE_RATE = 16000

LANGUAGE_CODE = os.getenv("SARVAM_LANGUAGE_CODE", "hi-IN")


class TranscriptLogger(FrameProcessor):
    """Print transcription frames with the elapsed time since speech started.

    Interim frames are overwritten in place on the same terminal line; the final
    transcript is committed on its own line. The gap between the first partial and
    the final is the latency a barge-in implementation gets to work with.
    """

    def __init__(self):
        """Initialize the logger with no utterance in progress."""
        super().__init__()
        self._speech_started_at: float | None = None
        self._first_partial_at: float | None = None

    def _elapsed(self) -> str:
        if self._speech_started_at is None:
            return "  --  "
        return f"{time.monotonic() - self._speech_started_at:6.3f}"

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """Log transcription and speech-boundary frames, then pass them along."""
        await super().process_frame(frame, direction)

        if isinstance(frame, UserStartedSpeakingFrame):
            self._speech_started_at = time.monotonic()
            self._first_partial_at = None
            print("\n[speech start]")
        elif isinstance(frame, InterimTranscriptionFrame):
            if self._first_partial_at is None:
                self._first_partial_at = time.monotonic()
            # \r keeps the growing hypothesis on one line.
            print(f"\r  {self._elapsed()}s  partial  {frame.text}", end="", flush=True)
        elif isinstance(frame, TranscriptionFrame):
            print(f"\r  {self._elapsed()}s  FINAL    {frame.text}")
            if self._speech_started_at is not None and self._first_partial_at is not None:
                head_start = time.monotonic() - self._first_partial_at
                print(f"           partials led the final by {head_start:.3f}s")
            self._speech_started_at = None
        elif isinstance(frame, UserStoppedSpeakingFrame):
            print("[speech end]")
        elif isinstance(frame, InterruptionFrame):
            # In a full voice agent this is what stops the bot mid-sentence.
            logger.debug("Interruption broadcast — a bot would stop speaking here")
        elif isinstance(frame, ErrorFrame):
            logger.error(f"{frame.error} (fatal={getattr(frame, 'fatal', False)})")

        await self.push_frame(frame, direction)


async def bot(runner_args: RunnerArguments):
    """Entry point invoked by Pipecat's development runner."""
    api_key = os.getenv("SARVAM_API_KEY")
    if not api_key:
        raise RuntimeError("Export SARVAM_API_KEY before starting the bot")

    transport = await create_transport(
        runner_args,
        {
            "webrtc": lambda: TransportParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_in_sample_rate=SAMPLE_RATE,
            ),
            "daily": lambda: TransportParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_in_sample_rate=SAMPLE_RATE,
            ),
        },
    )

    stt = SarvamRealtimeSTTService(
        api_key=api_key,
        should_interrupt=True,
        settings=SarvamRealtimeSTTService.Settings(
            language_code=LANGUAGE_CODE,
            mode="transcribe",
            stream_type="fast",
            endpointing="vad",
            encoding="linear16",
            sample_rate=SAMPLE_RATE,
        ),
    )

    pipeline = Pipeline(
        [
            transport.input(),
            stt,
            TranscriptLogger(),
            transport.output(),
        ]
    )

    worker = PipelineWorker(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=SAMPLE_RATE,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
        # Sarvam's speech frames are not the frame types that reset Pipecat's idle
        # timer, so a demo session would be cancelled after five minutes of an open
        # page. Disabled here so a recording is never cut short.
        idle_timeout_secs=None,
    )

    # The development runner owns the process and its SIGINT handler; letting the
    # WorkerRunner install its own would hijack Ctrl-C from the HTTP server.
    runner = WorkerRunner(handle_sigint=False)

    logger.info(f"Sarvam realtime STT ready: {LANGUAGE_CODE} at {SAMPLE_RATE} Hz")
    await runner.run(worker)


if __name__ == "__main__":
    from pipecat.runner.run import main

    main()
