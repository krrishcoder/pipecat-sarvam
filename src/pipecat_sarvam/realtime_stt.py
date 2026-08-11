"""Sarvam realtime speech-to-text service for Pipecat."""

from __future__ import annotations

import asyncio
import base64
import copy
import json
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import urlencode, urlsplit

from loguru import logger
from pipecat.frames.frames import (
    Frame,
    InterimTranscriptionFrame,
    StartFrame,
    STTMetadataFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.settings import (
    NOT_GIVEN,
    STTSettings,
    _NotGiven,
    assert_given,
    is_given,
)
from pipecat.services.stt_latency import SARVAM_TTFS_P99
from pipecat.services.stt_service import WebsocketSTTService
from pipecat.transcriptions.language import Language
from pipecat.turns.user_turn_strategies import ExternalUserTurnStrategies
from pipecat.utils.time import time_now_iso8601
from pipecat.utils.tracing.service_decorators import traced_stt
from websockets.protocol import State

SARVAM_REALTIME_MODEL = "saaras:v3-realtime"
SARVAM_REALTIME_URL = "wss://api.sarvam.ai/speech-to-text-realtime/ws"

LanguageCode = Literal[
    "auto",
    "en-IN",
    "hi-IN",
    "bn-IN",
    "kn-IN",
    "ml-IN",
    "mr-IN",
    "or-IN",
    "pa-IN",
    "ta-IN",
    "te-IN",
    "gu-IN",
    "as-IN",
    "ur-IN",
    "ne-IN",
    "kok-IN",
    "ks-IN",
    "sd-IN",
    "sa-IN",
    "sat-IN",
    "mni-IN",
    "brx-IN",
    "mai-IN",
    "doi-IN",
]
StreamType = Literal["fast", "balanced", "simulated"]
Endpointing = Literal["vad", "manual"]
Mode = Literal["transcribe", "translate", "verbatim", "translit", "codemix"]
Encoding = Literal["linear16"]

SUPPORTED_LANGUAGE_CODES = {
    "auto",
    "en-IN",
    "hi-IN",
    "bn-IN",
    "kn-IN",
    "ml-IN",
    "mr-IN",
    "or-IN",
    "pa-IN",
    "ta-IN",
    "te-IN",
    "gu-IN",
    "as-IN",
    "ur-IN",
    "ne-IN",
    "kok-IN",
    "ks-IN",
    "sd-IN",
    "sa-IN",
    "sat-IN",
    "mni-IN",
    "brx-IN",
    "mai-IN",
    "doi-IN",
}
SUPPORTED_STREAM_TYPES = {"fast", "balanced", "simulated"}
SUPPORTED_ENDPOINTING = {"vad", "manual"}
SUPPORTED_MODES = {"transcribe", "translate", "verbatim", "translit", "codemix"}
SUPPORTED_ENCODINGS = {"linear16"}
SUPPORTED_SAMPLE_RATES = {8000, 16000}

_BYTES_PER_SAMPLE = {"linear16": 2}
_MAX_CHUNK_DURATION_SECONDS = {
    "fast": 0.5,
    "balanced": 1.0,
    "simulated": 1.0,
}

_LANGUAGE_TO_SARVAM = {
    Language.AS_IN: "as-IN",
    Language.BN_IN: "bn-IN",
    Language.EN_IN: "en-IN",
    Language.GU_IN: "gu-IN",
    Language.HI_IN: "hi-IN",
    Language.KN_IN: "kn-IN",
    Language.KOK_IN: "kok-IN",
    Language.MAI_IN: "mai-IN",
    Language.ML_IN: "ml-IN",
    Language.MR_IN: "mr-IN",
    Language.OR_IN: "or-IN",
    Language.PA_IN: "pa-IN",
    Language.SD_IN: "sd-IN",
    Language.TA_IN: "ta-IN",
    Language.TE_IN: "te-IN",
    Language.UR_IN: "ur-IN",
}


class SarvamRealtimeSTTError(RuntimeError):
    """Fatal error reported by Sarvam's realtime STT endpoint."""


def language_to_sarvam_realtime_language(language: Language) -> str:
    """Map a Pipecat language enum to Sarvam's realtime language code."""
    try:
        return _LANGUAGE_TO_SARVAM[language]
    except KeyError as error:
        raise ValueError(f"Unsupported Sarvam realtime language: {language}") from error


def sarvam_language_to_pipecat(language_code: str | None) -> Language | None:
    """Map a Sarvam language code to a Pipecat enum when one exists."""
    if not language_code or language_code == "auto":
        return None
    try:
        return Language(language_code)
    except ValueError:
        return None


@dataclass
class SarvamRealtimeSTTSettings(STTSettings):
    """Runtime and connection settings for Sarvam realtime STT."""

    language_code: LanguageCode | str | _NotGiven = field(default_factory=lambda: NOT_GIVEN)
    stream_type: StreamType | str | _NotGiven = field(default_factory=lambda: NOT_GIVEN)
    endpointing: Endpointing | str | _NotGiven = field(default_factory=lambda: NOT_GIVEN)
    mode: Mode | str | _NotGiven = field(default_factory=lambda: NOT_GIVEN)
    encoding: Encoding | str | _NotGiven = field(default_factory=lambda: NOT_GIVEN)
    sample_rate: int | _NotGiven = field(default_factory=lambda: NOT_GIVEN)


class SarvamRealtimeSTTService(WebsocketSTTService):
    """Stream audio to Sarvam's ``saaras:v3-realtime`` WebSocket API."""

    Settings = SarvamRealtimeSTTSettings
    _settings: Settings

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = SARVAM_REALTIME_URL,
        sample_rate: int = 16000,
        settings: Settings | None = None,
        should_interrupt: bool = True,
        session_end_timeout: float = 0.5,
        session_ready_timeout: float = 10.0,
        ttfs_p99_latency: float | None = SARVAM_TTFS_P99,
        keepalive_timeout: float | None = 30.0,
        keepalive_interval: float = 5.0,
        **kwargs,
    ):
        """Initialize the Sarvam realtime STT service.

        Args:
            api_key: Sarvam API subscription key.
            base_url: Realtime WebSocket endpoint without query parameters.
            sample_rate: Input sample rate, either 8000 or 16000 Hz.
            settings: Typed Sarvam realtime settings.
            should_interrupt: Broadcast an interruption on server VAD speech start.
            session_end_timeout: Grace period for ``session.end`` during shutdown.
            session_ready_timeout: Maximum wait for ``session.begin`` before audio.
            ttfs_p99_latency: Pipecat STT latency metadata value.
            keepalive_timeout: Idle seconds before sending a protocol ``ping``.
            keepalive_interval: Seconds between idle checks.
            **kwargs: Additional arguments for :class:`WebsocketSTTService`.
        """
        if not api_key:
            raise ValueError("api_key must not be empty")

        default_settings = self.Settings(
            model=SARVAM_REALTIME_MODEL,
            language=None,
            language_code="hi-IN",
            stream_type="fast",
            endpointing="vad",
            mode="transcribe",
            encoding="linear16",
            sample_rate=sample_rate,
        )

        if settings is not None:
            has_language_code = is_given(settings.language_code)
            default_settings.apply_update(settings)
            if (
                is_given(settings.language)
                and settings.language is not None
                and not has_language_code
            ):
                language = settings.language
                if isinstance(language, str) and not isinstance(language, Language):
                    language = Language(language)
                default_settings.language_code = language_to_sarvam_realtime_language(language)

        self._validate_settings(default_settings)
        resolved_sample_rate = assert_given(default_settings.sample_rate)

        super().__init__(
            sample_rate=resolved_sample_rate,
            settings=default_settings,
            ttfs_p99_latency=ttfs_p99_latency,
            keepalive_timeout=keepalive_timeout,
            keepalive_interval=keepalive_interval,
            **kwargs,
        )

        self._api_key = api_key
        self._base_url = base_url
        self._should_interrupt = should_interrupt
        self._session_end_timeout = session_end_timeout
        self._session_ready_timeout = session_ready_timeout

        self._receive_task: asyncio.Task | None = None
        self._connect_lock = asyncio.Lock()
        self._connect_complete = asyncio.Event()
        self._connect_complete.set()
        self._session_ready = asyncio.Event()
        self._session_ended = asyncio.Event()
        self._request_id: str | None = None
        self._session_end_data: dict[str, Any] | None = None
        self._ttft_pending = False

    @staticmethod
    def _validate_settings(settings: Settings) -> None:
        """Validate a complete settings store before opening a connection."""
        model = assert_given(settings.model)
        language_code = assert_given(settings.language_code)
        stream_type = assert_given(settings.stream_type)
        endpointing = assert_given(settings.endpointing)
        mode = assert_given(settings.mode)
        encoding = assert_given(settings.encoding)
        sample_rate = assert_given(settings.sample_rate)

        if model != SARVAM_REALTIME_MODEL:
            raise ValueError(f"model must be {SARVAM_REALTIME_MODEL!r}")
        if language_code not in SUPPORTED_LANGUAGE_CODES:
            raise ValueError(f"Unsupported language_code: {language_code!r}")
        if stream_type not in SUPPORTED_STREAM_TYPES:
            raise ValueError(f"Unsupported stream_type: {stream_type!r}")
        if endpointing not in SUPPORTED_ENDPOINTING:
            raise ValueError(f"Unsupported endpointing: {endpointing!r}")
        if mode not in SUPPORTED_MODES:
            raise ValueError(f"Unsupported mode: {mode!r}")
        if encoding not in SUPPORTED_ENCODINGS:
            raise ValueError("Only linear16 PCM input is supported initially")
        if sample_rate not in SUPPORTED_SAMPLE_RATES:
            raise ValueError("sample_rate must be 8000 or 16000")

    def can_generate_metrics(self) -> bool:
        """Return whether this service emits Pipecat metrics."""
        return True

    def _record_stt_audio_usage(self, audio: bytes | bytearray):
        """Account for the configured wire encoding's bytes per sample."""
        if self.sample_rate <= 0:
            return
        encoding = assert_given(self._settings.encoding)
        self._stt_usage_pending_seconds += len(audio) / (
            self.sample_rate * _BYTES_PER_SAMPLE[encoding]
        )

    @property
    def supports_ttfs(self) -> bool:
        """Server-defined VAD boundaries do not need an additional TTFS wait."""
        return self._settings.endpointing != "vad"

    def service_metadata_frame(self) -> STTMetadataFrame:
        """Recommend external turn strategies when Sarvam owns endpointing."""
        frame = super().service_metadata_frame()
        if self._settings.endpointing == "vad":
            frame.user_turn_strategies = ExternalUserTurnStrategies()
        return frame

    def language_to_service_language(self, language: Language) -> str:
        """Convert Pipecat language values to Sarvam realtime codes."""
        return language_to_sarvam_realtime_language(language)

    async def start(self, frame: StartFrame):
        """Connect when the Pipecat pipeline starts."""
        await super().start(frame)
        await self._connect()

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """Forward local VAD boundaries when manual endpointing is configured."""
        await super().process_frame(frame, direction)

        if self._settings.endpointing != "manual":
            return
        if isinstance(frame, VADUserStartedSpeakingFrame):
            await self._begin_utterance_metrics()
            await self._send_json({"event": "speech_start"})
        elif isinstance(frame, VADUserStoppedSpeakingFrame):
            self._user_speaking = False
            await self._send_json({"event": "speech_end"})

    async def run_stt(self, audio: bytes) -> AsyncGenerator[Frame | None, None]:
        """Encode and stream audio; receive-task callbacks emit transcripts."""
        await self._connect_complete.wait()

        if not self._websocket or self._websocket.state is not State.OPEN:
            await self._connect()

        try:
            await asyncio.wait_for(
                self._session_ready.wait(),
                timeout=self._session_ready_timeout,
            )
        except TimeoutError as error:
            await self.push_error(
                "Timed out waiting for Sarvam realtime session.begin",
                exception=error,
            )
            yield None
            return

        encoding = assert_given(self._settings.encoding)
        stream_type = assert_given(self._settings.stream_type)
        sample_rate = assert_given(self._settings.sample_rate)
        bytes_per_sample = _BYTES_PER_SAMPLE[encoding]
        max_chunk_bytes = int(
            sample_rate * bytes_per_sample * _MAX_CHUNK_DURATION_SECONDS[stream_type]
        )
        max_chunk_bytes -= max_chunk_bytes % bytes_per_sample

        for offset in range(0, len(audio), max_chunk_bytes):
            chunk = audio[offset : offset + max_chunk_bytes]
            await self._send_json(
                {
                    "event": "audio_input",
                    "audio": base64.b64encode(chunk).decode("ascii"),
                }
            )

        yield None

    def _build_websocket_url(self) -> str:
        """Build the connection URL from the current settings."""
        parsed = urlsplit(self._base_url)
        if parsed.scheme != "wss" or not parsed.netloc:
            raise ValueError("base_url must be a valid wss:// URL")
        if parsed.query or parsed.fragment:
            raise ValueError("base_url must not include a query string or fragment")

        settings = self._settings
        params: dict[str, str | int] = {
            "language_code": assert_given(settings.language_code),
            "model": SARVAM_REALTIME_MODEL,
            "stream_type": assert_given(settings.stream_type),
            "mode": assert_given(settings.mode),
            "endpointing": assert_given(settings.endpointing),
            "encoding": assert_given(settings.encoding),
            "sample_rate": assert_given(settings.sample_rate),
        }
        return f"{self._base_url}?{urlencode(params)}"

    async def _connect(self):
        """Open the WebSocket and start its receive task."""
        async with self._connect_lock:
            if self._websocket and self._websocket.state is State.OPEN:
                return

            self._connect_complete.clear()
            try:
                await super()._connect()
                await self._connect_websocket()
                if self._websocket and (self._receive_task is None or self._receive_task.done()):
                    self._receive_task = self.create_task(
                        self._receive_task_handler(self._report_error),
                        name="sarvam-realtime-receive",
                    )
            finally:
                self._connect_complete.set()

    async def _disconnect(self):
        """Gracefully end the session, then stop the receive task and socket."""
        await super()._disconnect()

        websocket = self._websocket
        if websocket and websocket.state is State.OPEN:
            try:
                await websocket.send(json.dumps({"event": "end"}))
                await asyncio.wait_for(
                    self._session_ended.wait(),
                    timeout=self._session_end_timeout,
                )
            except TimeoutError:
                logger.debug(f"{self} timed out waiting for Sarvam session.end")
            except Exception as error:
                logger.debug(f"{self} error ending Sarvam session: {error}")

        if self._receive_task:
            await self.cancel_task(self._receive_task)
            self._receive_task = None

        await self._disconnect_websocket()

    async def _connect_websocket(self):
        """Open the provider WebSocket."""
        if self._websocket and self._websocket.state is State.OPEN:
            return

        self._session_ready.clear()
        self._session_ended.clear()
        self._request_id = None
        self._session_end_data = None

        try:
            self._websocket = await self._websocket_connect(
                self._build_websocket_url(),
                additional_headers={"API-SUBSCRIPTION-KEY": self._api_key},
                ping_interval=None,
            )
        except Exception as error:
            self._websocket = None
            await self.push_error(
                f"Unable to connect to Sarvam realtime STT: {error}",
                exception=error,
            )
            raise

        await self._call_event_handler("on_connected")

    async def _disconnect_websocket(self):
        """Close the current provider WebSocket."""
        websocket = self._websocket
        if websocket is None:
            return

        try:
            if websocket.state is State.OPEN:
                await websocket.close()
        finally:
            if self._websocket is websocket:
                self._websocket = None
            self._session_ready.clear()
            await self._call_event_handler("on_disconnected")

    async def _receive_messages(self):
        """Receive and dispatch Sarvam protocol events."""
        if self._websocket is None:
            raise ConnectionError("Sarvam realtime WebSocket is not connected")

        async for message in self._websocket:
            try:
                event = json.loads(message)
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"{self} received a non-JSON message")
                continue
            await self._handle_message(event)

    async def _send_json(self, message: dict[str, Any]):
        """Send one protocol message over the open socket."""
        if self._websocket is None or self._websocket.state is not State.OPEN:
            raise ConnectionError("Sarvam realtime WebSocket is not open")
        await self._websocket.send(json.dumps(message))

    async def _send_keepalive(self, silence: bytes):
        """Send Sarvam's protocol ping instead of synthetic audio."""
        del silence
        await self._send_json({"event": "ping"})

    async def _handle_message(self, message: dict[str, Any]):
        """Map one Sarvam event into Pipecat frames and state."""
        event = message.get("event")

        if event == "session.begin":
            self._request_id = message.get("request_id")
            self._session_ready.set()
            logger.info(f"{self} Sarvam session started request_id={self._request_id}")
        elif event == "vad.speech_start":
            await self._begin_utterance_metrics()
            await self.broadcast_frame(UserStartedSpeakingFrame)
            if self._should_interrupt:
                await self.broadcast_interruption()
        elif event == "vad.speech_end":
            self._user_speaking = False
            await self.broadcast_frame(UserStoppedSpeakingFrame)
        elif event == "transcript.partial":
            await self._handle_partial_transcript(message)
        elif event == "transcript.final":
            await self._handle_final_transcript(message)
        elif event == "config.updated":
            logger.debug(f"{self} Sarvam config updated: {message.get('applied', [])}")
        elif event == "session.end":
            self._session_end_data = message
            self._session_ended.set()
        elif event == "error":
            await self._handle_error(message)
        elif event != "pong":
            logger.debug(f"{self} unhandled Sarvam event: {message}")

    async def _begin_utterance_metrics(self):
        """Start first-transcript and final-transcript latency measurements."""
        await self._reset_stt_ttfb_state()
        self._user_speaking = True
        self._can_reconnect = False
        self._ttft_pending = True
        await self.start_ttfb_metrics()
        await self.start_processing_metrics()

    async def _mark_first_transcript_received(self):
        """Report latency to the first non-empty interim or final transcript."""
        if not self._ttft_pending:
            return
        self._ttft_pending = False
        await self.stop_ttfb_metrics()

    async def _finish_utterance_metrics(self):
        """Report speech-start-to-final latency and release deferred reconnects."""
        await self._mark_first_transcript_received()
        await self.stop_processing_metrics()
        self._user_speaking = False
        self._can_reconnect = True
        if self._need_reconnect:
            self.create_task(self._reconnect(), name="sarvam-realtime-settings-reconnect")

    def _language_for_message(self, message: dict[str, Any]) -> Language | None:
        language_code = message.get("language")
        if not language_code:
            configured = assert_given(self._settings.language_code)
            language_code = None if configured == "auto" else configured
        return sarvam_language_to_pipecat(language_code)

    async def _handle_partial_transcript(self, message: dict[str, Any]):
        text = str(message.get("text", "")).strip()
        if not text:
            return

        await self._mark_first_transcript_received()
        await self.push_frame(
            InterimTranscriptionFrame(
                text,
                self._user_id,
                time_now_iso8601(),
                self._language_for_message(message),
                result=self._frame_result(message),
            )
        )

    async def _handle_final_transcript(self, message: dict[str, Any]):
        text = str(message.get("text", "")).strip()
        if not text:
            await self._finish_utterance_metrics()
            return

        await self._mark_first_transcript_received()
        language = self._language_for_message(message)
        await self._trace_transcription(text, True, language)
        await self.emit_stt_usage_metrics()
        await self.push_frame(
            TranscriptionFrame(
                text,
                self._user_id,
                time_now_iso8601(),
                language,
                result=self._frame_result(message),
                finalized=True,
            )
        )
        await self._finish_utterance_metrics()

    def _frame_result(self, message: dict[str, Any]) -> dict[str, Any]:
        result = dict(message)
        if self._request_id:
            result["request_id"] = self._request_id
        return result

    async def _handle_error(self, message: dict[str, Any]):
        code = message.get("code", "unknown")
        detail = message.get("message", "Unknown Sarvam realtime STT error")
        fatal = bool(message.get("is_fatal"))
        error = SarvamRealtimeSTTError(f"{code}: {detail}")

        await self.push_error(str(error), exception=error, fatal=fatal)
        if fatal:
            self._disconnecting = True
            self._ttft_pending = False
            await self.stop_all_metrics()
            await self._call_event_handler("on_connection_error", str(error))
            raise error

    async def _update_settings(self, delta: STTSettings) -> dict[str, Any]:
        """Apply live settings with ``config.update`` or a deferred reconnect."""
        if is_given(delta.model) and delta.model != SARVAM_REALTIME_MODEL:
            raise ValueError(f"model must remain {SARVAM_REALTIME_MODEL!r}")

        if isinstance(delta, self.Settings):
            if is_given(delta.sample_rate) and delta.sample_rate != self._settings.sample_rate:
                raise ValueError("sample_rate cannot be updated while the service is running")
            if is_given(delta.encoding) and delta.encoding != self._settings.encoding:
                raise ValueError("encoding cannot be updated while the service is running")

        candidate = copy.deepcopy(self._settings)
        candidate.apply_update(delta)
        if is_given(delta.language) and delta.language is not None:
            language = delta.language
            if isinstance(language, str) and not isinstance(language, Language):
                language = Language(language)
            candidate.language_code = language_to_sarvam_realtime_language(language)
        self._validate_settings(candidate)

        changed = await super()._update_settings(delta)
        if "language" in changed and self._settings.language is not None:
            previous_language_code = self._settings.language_code
            self._settings.language_code = str(self._settings.language)
            changed.setdefault("language_code", previous_language_code)

        if not changed:
            return changed

        reconnect_fields: set[str] = set()
        old_stream_type = changed.get("stream_type")
        new_stream_type = self._settings.stream_type
        if old_stream_type is not None and (
            old_stream_type == "simulated" or new_stream_type == "simulated"
        ):
            reconnect_fields.add("stream_type")

        if changed.keys() & reconnect_fields:
            await self._request_reconnect()
            return changed

        live_fields = {
            "language_code",
            "stream_type",
            "endpointing",
            "mode",
        }
        update = {key: getattr(self._settings, key) for key in changed.keys() & live_fields}
        if update and self._websocket and self._websocket.state is State.OPEN:
            await self._send_json({"event": "config.update", **update})

        if "endpointing" in changed:
            await self.broadcast_service_metadata()

        return changed

    async def update_config(self, **fields: Any) -> dict[str, Any]:
        """Apply a typed runtime configuration update."""
        return await self._update_settings(self.Settings(**fields))

    @traced_stt
    async def _trace_transcription(
        self,
        transcript: str,
        is_final: bool,
        language: Language | None = None,
    ):
        """Record a transcription in Pipecat tracing."""
        pass
