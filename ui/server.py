"""Browser harness that runs Sarvam realtime STT inside a real Pipecat pipeline.

This is a debugging tool, not part of the shipped package. It serves a
single-page UI and a WebSocket endpoint from one port using the ``websockets``
package, so it adds no dependency beyond what ``pipecat-sarvam`` already needs.

The browser captures microphone audio, resamples it to mono linear16 at the
configured sample rate, and streams it here as binary messages. Every
connection builds its own ``Pipeline`` -> ``PipelineWorker`` -> ``WorkerRunner``,
injects the audio as ``InputAudioRawFrame``, and streams every resulting Pipecat
frame back to the browser as JSON. What you see in the UI is therefore the real
frame output of ``SarvamRealtimeSTTService`` inside a genuine pipeline, not a
reimplementation of the protocol.

Run it with::

    python ui/server.py

then open http://127.0.0.1:8080 and allow microphone access.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import mimetypes
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from loguru import logger
from pipecat.frames.frames import (
    ErrorFrame,
    Frame,
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    MetricsFrame,
    StartFrame,
    STTMetadataFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.observers.base_observer import BaseObserver, FramePushed
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.workers.runner import WorkerRunner
from websockets.asyncio.server import ServerConnection, serve
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from examples.raw_realtime_stt import get_api_key  # noqa: E402
from pipecat_sarvam import SarvamRealtimeSTTService  # noqa: E402

STATIC_DIR = Path(__file__).resolve().parent / "static"
WEBSOCKET_PATHS = {"/ws", "/ws/"}

# Mirrors the validation in SarvamRealtimeSTTService so the UI cannot open a
# connection the service would reject anyway.
ALLOWED_SETTINGS = {
    "language_code": {
        "auto", "as-IN", "bn-IN", "brx-IN", "doi-IN", "en-IN", "gu-IN", "hi-IN",
        "kn-IN", "kok-IN", "ks-IN", "mai-IN", "ml-IN", "mni-IN", "mr-IN",
        "ne-IN", "or-IN", "pa-IN", "sa-IN", "sat-IN", "sd-IN", "ta-IN",
        "te-IN", "ur-IN",
    },
    "mode": {"transcribe", "translate", "verbatim", "translit", "codemix"},
    "stream_type": {"fast", "balanced", "simulated"},
    "endpointing": {"vad", "manual"},
}
ALLOWED_SAMPLE_RATES = {8000, 16000}

# Upper bound on remembered frame ids in BrowserObserver.
SEEN_LIMIT = 8192


def _json_safe(value: Any) -> Any:
    """Return ``value`` if it survives ``json.dumps``, otherwise its repr."""
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return repr(value)
    return value


def _language(value: Any) -> str | None:
    """Render a Pipecat ``Language`` (a ``StrEnum``) or ``None``."""
    return None if value is None else str(value)


def serialize_frame(frame: Frame) -> dict[str, Any] | None:
    """Convert a Pipecat frame into a JSON-safe payload for the browser.

    Fields are whitelisted deliberately. Several frames carry live Python
    objects (``ErrorFrame.exception``, ``ErrorFrame.processor``,
    ``StartFrame.tracing_context``) that ``json.dumps`` cannot encode.

    Args:
        frame: The frame observed on the pipeline.

    Returns:
        A JSON-safe dict, or None for frames the UI ignores.
    """
    base: dict[str, Any] = {"kind": type(frame).__name__, "frame": frame.name}

    if isinstance(frame, InterimTranscriptionFrame):
        return {
            **base,
            "type": "interim",
            "text": frame.text,
            "language": _language(frame.language),
            "timestamp": frame.timestamp,
            "result": _json_safe(frame.result),
        }
    if isinstance(frame, TranscriptionFrame):
        return {
            **base,
            "type": "final",
            "text": frame.text,
            "language": _language(frame.language),
            "timestamp": frame.timestamp,
            "finalized": frame.finalized,
            "result": _json_safe(frame.result),
        }
    if isinstance(frame, UserStartedSpeakingFrame):
        return {**base, "type": "speech_start"}
    if isinstance(frame, UserStoppedSpeakingFrame):
        return {**base, "type": "speech_end"}
    if isinstance(frame, ErrorFrame):
        return {
            **base,
            "type": "error",
            "error": frame.error,
            "fatal": frame.fatal,
            "processor": getattr(frame.processor, "name", None),
            "exception": None if frame.exception is None else repr(frame.exception),
        }
    if isinstance(frame, STTMetadataFrame):
        strategies = frame.user_turn_strategies
        return {
            **base,
            "type": "stt_metadata",
            "service_name": frame.service_name,
            "ttfs_p99_latency": frame.ttfs_p99_latency,
            "user_turn_strategies": None if strategies is None else type(strategies).__name__,
        }
    if isinstance(frame, MetricsFrame):
        data = [{"type": type(d).__name__, **d.model_dump()} for d in frame.data]
        # The pipeline emits one zeroed MetricsFrame per metrics-capable
        # processor at startup (PipelineParams.send_initial_empty_metrics);
        # drop those so the UI only shows real numbers. The test must be
        # narrow: TTFAMetricsData and TurnMetricsData have no `value` field at
        # all, and STTUsageMetricsData.value is an object, so only a frame
        # whose every value is the number zero counts as an empty priming frame.
        values = [item.get("value") for item in data]
        if data and all(
            isinstance(v, (int, float)) and not isinstance(v, bool) and v == 0 for v in values
        ):
            return None
        return {**base, "type": "metrics", "data": _json_safe(data)}
    if isinstance(frame, StartFrame):
        return {
            **base,
            "type": "start",
            "audio_in_sample_rate": frame.audio_in_sample_rate,
            "enable_metrics": frame.enable_metrics,
        }
    if isinstance(frame, InputAudioRawFrame):
        return None
    return {**base, "type": "other"}


class BrowserObserver(BaseObserver):
    """Forward every distinct pipeline frame to the browser.

    Two levels of deduplication are needed. ``on_push_frame`` fires once per
    inter-processor hop, so one frame is seen several times. And
    ``broadcast_frame`` (which the service uses for the speaking frames) creates
    two sibling instances with distinct ids, one per direction, for a single
    logical event; those are collapsed via ``broadcast_sibling_id``.
    """

    def __init__(self, send: Callable[[dict[str, Any]], Awaitable[None]], **kwargs):
        """Initialize the observer.

        Args:
            send: Coroutine that delivers one JSON payload to the browser.
            **kwargs: Forwarded to :class:`BaseObserver`.
        """
        super().__init__(**kwargs)
        self._send = send
        self._seen: set[int] = set()

    def _record(self, frame_id: int):
        """Remember a frame id, keeping the set bounded."""
        self._seen.add(frame_id)
        if len(self._seen) > SEEN_LIMIT:
            # Frame ids increase monotonically, so the oldest half can never be
            # pushed through the pipeline again and is safe to forget.
            self._seen = set(sorted(self._seen)[SEEN_LIMIT // 2 :])

    async def on_push_frame(self, data: FramePushed):
        """Serialize and forward one newly seen frame."""
        frame = data.frame
        # Audio is ~30 frames a second and is never shown, so it is dropped
        # before it can dominate the dedupe set.
        if isinstance(frame, InputAudioRawFrame):
            return

        sibling = getattr(frame, "broadcast_sibling_id", None)
        if frame.id in self._seen or (sibling is not None and sibling in self._seen):
            return
        self._record(frame.id)

        payload = serialize_frame(frame)
        if payload is None:
            return
        payload["direction"] = data.direction.name
        payload["source"] = data.source.name
        await self._send(payload)


def validate_start_message(message: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """Validate the browser's start message into Sarvam settings.

    Args:
        message: The decoded start message.

    Returns:
        A ``(settings, sample_rate)`` pair.

    Raises:
        ValueError: If any supplied value is not accepted by the service.
    """
    settings: dict[str, Any] = {}
    for field, allowed in ALLOWED_SETTINGS.items():
        value = message.get(field)
        if value is None:
            continue
        if value not in allowed:
            raise ValueError(f"Unsupported {field}: {value!r}")
        settings[field] = value

    sample_rate = int(message.get("sample_rate", 16000))
    if sample_rate not in ALLOWED_SAMPLE_RATES:
        raise ValueError(f"Unsupported sample_rate: {sample_rate!r}")

    settings.setdefault("language_code", "hi-IN")
    settings.setdefault("mode", "transcribe")
    settings.setdefault("stream_type", "fast")
    settings.setdefault("endpointing", "vad")
    return settings, sample_rate


class HarnessSession:
    """One browser connection driving one Pipecat pipeline."""

    def __init__(self, connection: ServerConnection, api_key: str):
        """Initialize the session.

        Args:
            connection: The accepted browser WebSocket connection.
            api_key: Sarvam API subscription key.
        """
        self._connection = connection
        self._api_key = api_key
        self._stt: SarvamRealtimeSTTService | None = None
        self._worker: PipelineWorker | None = None
        self._runner: WorkerRunner | None = None
        self._run_task: asyncio.Task | None = None
        self._sample_rate = 16000

    async def send(self, payload: dict[str, Any]):
        """Send one JSON payload to the browser, ignoring a closed socket."""
        try:
            await self._connection.send(json.dumps(payload, ensure_ascii=False))
        except (ConnectionClosed, RuntimeError):
            pass

    async def run(self):
        """Handle the connection until the browser disconnects."""
        try:
            await self._await_start()
        except ValueError as error:
            await self.send({"type": "fatal", "error": str(error)})
            return
        except (ConnectionClosed, asyncio.CancelledError):
            return

        try:
            await self._consume_messages()
        except ConnectionClosed:
            logger.info("Browser disconnected")
        finally:
            await self._shutdown()

    async def _await_start(self):
        """Wait for the browser's start message and build the pipeline."""
        while True:
            raw = await self._connection.recv()
            if isinstance(raw, bytes):
                # Audio before configuration; ignore it.
                continue
            message = json.loads(raw)
            if message.get("type") != "start":
                continue

            settings, sample_rate = validate_start_message(message)
            await self._build_pipeline(settings, sample_rate)
            return

    async def _build_pipeline(self, settings: dict[str, Any], sample_rate: int):
        """Construct and start the pipeline for this session."""
        self._sample_rate = sample_rate
        self._stt = SarvamRealtimeSTTService(
            api_key=self._api_key,
            sample_rate=sample_rate,
            settings=SarvamRealtimeSTTService.Settings(
                encoding="linear16",
                sample_rate=sample_rate,
                **settings,
            ),
        )

        pipeline = Pipeline([self._stt])
        params = PipelineParams(
            audio_in_sample_rate=sample_rate,
            enable_metrics=True,
            enable_usage_metrics=True,
        )
        self._worker = PipelineWorker(
            pipeline,
            params=params,
            observers=[BrowserObserver(self.send)],
            # A bare harness has no transport, so RTVI would only add noise and
            # audio frames never reset the idle timer.
            enable_rtvi=False,
            cancel_on_idle_timeout=False,
            idle_timeout_secs=None,
        )
        # handle_sigint would replace this process's own SIGINT handler.
        self._runner = WorkerRunner(handle_sigint=False)
        await self._runner.add_workers(self._worker)
        self._run_task = asyncio.create_task(self._runner.run(), name="sarvam-ui-runner")

        await self.send(
            {
                "type": "ready",
                "settings": {**settings, "encoding": "linear16", "sample_rate": sample_rate},
                "service": self._stt.name,
            }
        )
        logger.info(f"Session ready: {settings} at {sample_rate} Hz")

    async def _consume_messages(self):
        """Route browser messages into the pipeline."""
        async for raw in self._connection:
            if isinstance(raw, (bytes, bytearray)):
                await self._queue_audio(bytes(raw))
            else:
                await self._handle_control(json.loads(raw))

    async def _queue_audio(self, audio: bytes):
        """Inject captured PCM at the head of the pipeline."""
        if self._worker is None or not audio:
            return
        await self._worker.queue_frame(
            InputAudioRawFrame(audio=audio, sample_rate=self._sample_rate, num_channels=1)
        )

    async def _handle_control(self, message: dict[str, Any]):
        """Handle a non-audio control message from the browser."""
        kind = message.get("type")

        if kind == "vad" and self._worker is not None:
            # Only meaningful with endpointing="manual", where the adapter
            # forwards these as Sarvam speech_start / speech_end events.
            if message.get("speaking"):
                await self._worker.queue_frame(VADUserStartedSpeakingFrame(start_secs=0.2))
            else:
                await self._worker.queue_frame(VADUserStoppedSpeakingFrame(stop_secs=0.8))
            return

        if kind == "update_config" and self._stt is not None:
            fields = {k: v for k, v in message.get("fields", {}).items() if v is not None}
            try:
                changed = await self._stt.update_config(**fields)
            except Exception as error:
                await self.send({"type": "config_error", "error": str(error)})
                return
            await self.send(
                {"type": "config_applied", "requested": fields, "previous": _json_safe(changed)}
            )
            return

        if kind == "stop":
            await self._connection.close()

    async def _shutdown(self):
        """Tear the pipeline down without losing trailing frames."""
        if self._worker is not None:
            try:
                await self._worker.flush_pipeline(timeout=2.0)
            except Exception as error:
                logger.debug(f"flush_pipeline failed: {error}")
        if self._runner is not None:
            try:
                await self._runner.cancel(reason="browser disconnected")
            except Exception as error:
                logger.debug(f"runner cancel failed: {error}")
        if self._run_task is not None:
            self._run_task.cancel()
            await asyncio.gather(self._run_task, return_exceptions=True)
        logger.info("Session torn down")


def build_static_handler() -> Callable[[ServerConnection, Request], Response | None]:
    """Build a ``process_request`` hook that serves the UI over plain HTTP."""
    root = STATIC_DIR.resolve()

    def process_request(connection: ServerConnection, request: Request) -> Response | None:
        del connection
        path = request.path.split("?", 1)[0]
        if path in WEBSOCKET_PATHS:
            return None  # Continue with the WebSocket handshake.

        relative = path.lstrip("/") or "index.html"
        target = (root / relative).resolve()
        # `is_relative_to` rejects `..` traversal after resolution.
        if not target.is_relative_to(root) or not target.is_file():
            return Response(
                404,
                "Not Found",
                Headers({"Content-Type": "text/plain; charset=utf-8"}),
                b"Not found",
            )

        body = target.read_bytes()
        content_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        if content_type.startswith("text/") or content_type == "application/javascript":
            content_type = f"{content_type}; charset=utf-8"
        headers = Headers(
            {
                "Content-Type": content_type,
                "Content-Length": str(len(body)),
                "Cache-Control": "no-store",
            }
        )
        return Response(200, "OK", headers, body)

    return process_request


async def main_async(args: argparse.Namespace):
    """Serve the harness until interrupted."""
    api_key = get_api_key(args.env_file)

    async def handler(connection: ServerConnection):
        await HarnessSession(connection, api_key).run()

    async with serve(
        handler,
        args.host,
        args.port,
        process_request=build_static_handler(),
        max_size=None,
        ping_interval=20,
    ):
        logger.info(f"Sarvam realtime harness on http://{args.host}:{args.port}")
        await asyncio.Future()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    return parser.parse_args()


def main() -> None:
    """Run the harness server."""
    args = parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logger.info("Shutting down")
    except (OSError, ValueError) as error:
        raise SystemExit(f"error: {error}") from error


if __name__ == "__main__":
    main()
