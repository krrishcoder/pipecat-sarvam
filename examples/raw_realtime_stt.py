"""Exercise Sarvam's realtime STT WebSocket without Pipecat.

This diagnostic client intentionally has no Pipecat imports. It validates the
provider protocol independently by streaming a mono linear16 WAV or raw PCM file
and recording every server event as JSON Lines.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import wave
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

from websockets.asyncio.client import ClientConnection, connect
from websockets.protocol import State

DEFAULT_BASE_URL = "wss://api.sarvam.ai/speech-to-text-realtime/ws"
SUPPORTED_SAMPLE_RATES = {8000, 16000}


@dataclass(frozen=True)
class AudioSource:
    """Validated audio input and its wire-format metadata."""

    path: Path
    sample_rate: int
    frame_width: int
    total_frames: int
    is_wav: bool

    @property
    def duration_seconds(self) -> float:
        """Return the audio duration in seconds."""
        return self.total_frames / self.sample_rate


def _api_key_from_env_file(path: Path) -> str | None:
    """Read only SARVAM_API_KEY from a dotenv-style file without executing it."""
    if not path.is_file():
        return None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if separator and key.strip() == "SARVAM_API_KEY":
            return value.strip().strip("\"'") or None
    return None


def get_api_key(env_file: Path) -> str:
    """Resolve the API key without ever logging its value."""
    api_key = os.getenv("SARVAM_API_KEY") or _api_key_from_env_file(env_file)
    if not api_key:
        raise ValueError(
            "SARVAM_API_KEY is missing. Export it or add it to the configured env file."
        )
    return api_key


def inspect_audio(path: Path, raw_sample_rate: int) -> AudioSource:
    """Validate a mono 16-bit PCM WAV or raw linear16 PCM file."""
    if not path.is_file():
        raise ValueError(f"Audio file does not exist: {path}")

    if path.suffix.lower() == ".wav":
        with wave.open(str(path), "rb") as wav_file:
            if wav_file.getnchannels() != 1:
                raise ValueError("WAV input must be mono")
            if wav_file.getsampwidth() != 2:
                raise ValueError("WAV input must contain 16-bit linear PCM")
            if wav_file.getcomptype() != "NONE":
                raise ValueError("WAV input must be uncompressed PCM")

            sample_rate = wav_file.getframerate()
            total_frames = wav_file.getnframes()
    elif path.suffix.lower() in {".pcm", ".raw"}:
        sample_rate = raw_sample_rate
        byte_count = path.stat().st_size
        if byte_count % 2:
            raise ValueError("Raw linear16 PCM input must contain an even number of bytes")
        total_frames = byte_count // 2
    else:
        raise ValueError("Audio input must use a .wav, .pcm, or .raw extension")

    if sample_rate not in SUPPORTED_SAMPLE_RATES:
        raise ValueError(
            f"Unsupported sample rate {sample_rate}; Sarvam realtime accepts 8000 or 16000 Hz"
        )
    if total_frames == 0:
        raise ValueError("Audio input is empty")

    return AudioSource(
        path=path,
        sample_rate=sample_rate,
        frame_width=2,
        total_frames=total_frames,
        is_wav=path.suffix.lower() == ".wav",
    )


def audio_chunks(source: AudioSource, chunk_ms: int) -> Iterator[tuple[bytes, float]]:
    """Yield audio chunks and their real-time pacing duration."""
    frames_per_chunk = max(1, source.sample_rate * chunk_ms // 1000)

    if source.is_wav:
        with wave.open(str(source.path), "rb") as wav_file:
            while chunk := wav_file.readframes(frames_per_chunk):
                yield chunk, len(chunk) / (source.sample_rate * source.frame_width)
    else:
        bytes_per_chunk = frames_per_chunk * source.frame_width
        with source.path.open("rb") as pcm_file:
            while chunk := pcm_file.read(bytes_per_chunk):
                yield chunk, len(chunk) / (source.sample_rate * source.frame_width)


def build_websocket_url(args: argparse.Namespace, sample_rate: int) -> str:
    """Build and validate the realtime endpoint URL."""
    parsed = urlsplit(args.base_url)
    if parsed.scheme != "wss" or not parsed.netloc:
        raise ValueError("The Sarvam base URL must be a valid wss:// URL")
    if parsed.query or parsed.fragment:
        raise ValueError("The Sarvam base URL must not include a query string or fragment")

    query = urlencode(
        {
            "language_code": args.language_code,
            "model": "saaras:v3-realtime",
            "stream_type": args.stream_type,
            "mode": args.mode,
            "endpointing": "vad",
            "encoding": "linear16",
            "sample_rate": sample_rate,
            "return_timestamps": str(args.return_timestamps).lower(),
        }
    )
    return f"{args.base_url}?{query}"


async def receive_events(
    websocket: ClientConnection,
    event_queue: asyncio.Queue[dict[str, Any]],
    output_path: Path,
) -> None:
    """Print and persist every JSON event received from Sarvam."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as output_file:
        async for message in websocket:
            try:
                event = json.loads(message)
            except (json.JSONDecodeError, TypeError):
                print(f"NON_JSON {message!r}")
                continue

            rendered = json.dumps(event, ensure_ascii=False, sort_keys=True)
            print(rendered, flush=True)
            output_file.write(f"{rendered}\n")
            output_file.flush()
            await event_queue.put(event)


async def wait_for_event(
    event_queue: asyncio.Queue[dict[str, Any]],
    expected_event: str,
    timeout: float,
) -> dict[str, Any]:
    """Wait for one event type while surfacing fatal protocol errors."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for {expected_event!r}")

        event = await asyncio.wait_for(event_queue.get(), timeout=remaining)
        event_name = event.get("event")
        if event_name == "error" and event.get("is_fatal"):
            raise RuntimeError(
                f"Fatal Sarvam error {event.get('code', 'unknown')}: "
                f"{event.get('message', 'no message')}"
            )
        if event_name == expected_event:
            return event


async def send_audio(
    websocket: ClientConnection,
    source: AudioSource,
    chunk_ms: int,
    trailing_silence_ms: int,
    max_audio_seconds: float,
) -> None:
    """Stream audio in real time, followed by enough silence for server VAD."""
    remaining_bytes = int(source.sample_rate * source.frame_width * max_audio_seconds)
    for chunk, duration in audio_chunks(source, chunk_ms):
        if remaining_bytes <= 0:
            break
        chunk = chunk[:remaining_bytes]
        duration = len(chunk) / (source.sample_rate * source.frame_width)
        message = {
            "event": "audio_input",
            "audio": base64.b64encode(chunk).decode("ascii"),
        }
        await websocket.send(json.dumps(message))
        await asyncio.sleep(duration)
        remaining_bytes -= len(chunk)

    silence_bytes = source.sample_rate * source.frame_width * trailing_silence_ms // 1000
    silence_chunk_bytes = source.sample_rate * source.frame_width * chunk_ms // 1000
    while silence_bytes > 0:
        chunk_size = min(silence_bytes, silence_chunk_bytes)
        silence = b"\x00" * chunk_size
        await websocket.send(
            json.dumps(
                {
                    "event": "audio_input",
                    "audio": base64.b64encode(silence).decode("ascii"),
                }
            )
        )
        await asyncio.sleep(chunk_size / (source.sample_rate * source.frame_width))
        silence_bytes -= chunk_size


async def run(args: argparse.Namespace) -> None:
    """Run one raw Sarvam realtime STT session."""
    if args.chunk_ms <= 0:
        raise ValueError("--chunk-ms must be greater than zero")
    max_chunk_ms = 500 if args.stream_type == "fast" else 1000
    if args.chunk_ms > max_chunk_ms:
        raise ValueError(
            f"--chunk-ms cannot exceed {max_chunk_ms} for stream type {args.stream_type!r}"
        )
    if args.max_audio_seconds <= 0:
        raise ValueError("--max-audio-seconds must be greater than zero")
    if args.trailing_silence_ms < 0:
        raise ValueError("--trailing-silence-ms cannot be negative")
    if args.final_timeout <= 0:
        raise ValueError("--final-timeout must be greater than zero")

    source = inspect_audio(args.audio, args.sample_rate)
    websocket_url = build_websocket_url(args, source.sample_rate)
    api_key = get_api_key(args.env_file)

    if args.dry_run:
        print(
            json.dumps(
                {
                    "audio": str(source.path),
                    "duration_seconds": round(source.duration_seconds, 3),
                    "streamed_seconds": min(
                        round(source.duration_seconds, 3), args.max_audio_seconds
                    ),
                    "sample_rate": source.sample_rate,
                    "chunk_ms": args.chunk_ms,
                    "language_code": args.language_code,
                    "stream_type": args.stream_type,
                    "events_output": str(args.events_output),
                    "api_key_present": bool(api_key),
                },
                indent=2,
            )
        )
        return

    event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    headers = {"API-SUBSCRIPTION-KEY": api_key}

    async with connect(
        websocket_url,
        additional_headers=headers,
        open_timeout=args.timeout,
        ping_interval=None,
    ) as websocket:
        receiver = asyncio.create_task(
            receive_events(websocket, event_queue, args.events_output),
            name="sarvam-realtime-events",
        )
        session_error: Exception | None = None
        final_received = False

        try:
            await wait_for_event(event_queue, "session.begin", args.timeout)
            await send_audio(
                websocket,
                source,
                chunk_ms=args.chunk_ms,
                trailing_silence_ms=args.trailing_silence_ms,
                max_audio_seconds=args.max_audio_seconds,
            )
            await wait_for_event(event_queue, "transcript.final", args.final_timeout)
            final_received = True
        except TimeoutError:
            # A truncated utterance may finalize only after the graceful `end`
            # event, even when trailing silence matches the configured threshold.
            pass
        except Exception as error:
            session_error = error
        finally:
            if websocket.state is State.OPEN:
                await websocket.send(json.dumps({"event": "end"}))

        if not final_received and session_error is None:
            try:
                await wait_for_event(event_queue, "transcript.final", args.final_timeout)
            except Exception as error:
                session_error = error

        try:
            await wait_for_event(event_queue, "session.end", args.timeout)
        except Exception as error:
            if session_error is None:
                session_error = error
        finally:
            try:
                await asyncio.wait_for(receiver, timeout=2.0)
            except TimeoutError:
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)

        if session_error:
            raise session_error


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("audio", type=Path, help="16-bit mono WAV or raw linear16 PCM file")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--language-code", default="hi-IN")
    parser.add_argument("--stream-type", choices=("fast", "balanced", "simulated"), default="fast")
    parser.add_argument(
        "--mode",
        choices=("transcribe", "translate", "verbatim", "translit", "codemix"),
        default="transcribe",
    )
    parser.add_argument("--sample-rate", type=int, default=16000, help="Used for raw PCM input")
    parser.add_argument("--chunk-ms", type=int, default=100)
    parser.add_argument("--max-audio-seconds", type=float, default=15.0)
    parser.add_argument("--trailing-silence-ms", type=int, default=1500)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--final-timeout", type=float, default=5.0)
    parser.add_argument("--return-timestamps", action="store_true")
    parser.add_argument(
        "--events-output",
        type=Path,
        default=Path("artifacts/sarvam-realtime-events.jsonl"),
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    """Run the command-line client."""
    try:
        asyncio.run(run(parse_args()))
    except (OSError, TimeoutError, ValueError, RuntimeError) as error:
        raise SystemExit(f"error: {error}") from error


if __name__ == "__main__":
    main()
