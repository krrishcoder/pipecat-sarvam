# Pipecat Sarvam

Community-maintained Sarvam realtime STT integration for
[Pipecat](https://github.com/pipecat-ai/pipecat).

Uses Sarvam's `saaras:v3-realtime` model through the realtime WebSocket API.

> [!IMPORTANT]
> This is an early community integration. It is not an official Pipecat or
> Sarvam package.

## Features

- Streaming speech-to-text without audio accumulation
- Interim and finalized Pipecat transcription frames
- Sarvam server-side VAD mapped to Pipecat user-speaking frames
- Barge-in through Pipecat interruption broadcasts
- Manual endpointing when local VAD is preferred
- Pipecat metadata, usage, first-transcript, and final-transcript metrics
- Runtime configuration updates
- Bounded reconnects, explicit errors, keepalive, and graceful shutdown

## Installation

Python 3.11 or newer is required. Until a package release is published, install
directly from the repository:

```bash
python -m pip install \
  "git+https://github.com/krrishcoder/pipecat-sarvam.git"
```

For local development:

```bash
git clone https://github.com/krrishcoder/pipecat-sarvam.git
cd pipecat-sarvam
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Configuration

Create a Sarvam API key and keep it outside source control:

```bash
export SARVAM_API_KEY="your-api-key"
```

The repository ignores `.env` files. The raw diagnostic client can read
`SARVAM_API_KEY` from `.env`; application code should use its normal secret
loader.

`SarvamRealtimeSTTService.Settings` intentionally starts with six
provider-specific options:

- `language_code`: Sarvam language code; default `hi-IN`
- `mode`: `transcribe`, `translate`, `verbatim`, `translit`, or `codemix`
- `stream_type`: `fast`, `balanced`, or `simulated`
- `endpointing`: `vad` for Sarvam VAD or `manual` for local boundaries
- `encoding`: currently `linear16`
- `sample_rate`: `8000` or `16000`

The model is fixed to `saaras:v3-realtime`. Input encoding and sample rate
cannot change after construction. Language, mode, stream type, and endpointing
support runtime updates where the Sarvam protocol permits them.

Connection policy is configured with constructor parameters such as
`reconnect_on_error`, `max_reconnect_attempts`, `connection_timeout`, and
`session_ready_timeout`.

## Basic usage

```python
import os

from pipecat_sarvam import SarvamRealtimeSTTService

stt = SarvamRealtimeSTTService(
    api_key=os.environ["SARVAM_API_KEY"],
    should_interrupt=True,
    settings=SarvamRealtimeSTTService.Settings(
        language_code="hi-IN",
        mode="transcribe",
        stream_type="fast",
        endpointing="vad",
        encoding="linear16",
        sample_rate=16000,
    ),
)
```

Place the service immediately after the input transport in a Pipecat pipeline:

```python
from pipecat.pipeline.pipeline import Pipeline

pipeline = Pipeline(
    [
        transport.input(),
        stt,
        # Context aggregation, LLM, TTS, and transport.output() follow.
    ]
)
```

Sarvam partials become `InterimTranscriptionFrame` instances. Final events
become finalized `TranscriptionFrame` instances. With `endpointing="vad"`,
Sarvam speech boundaries become `UserStartedSpeakingFrame` and
`UserStoppedSpeakingFrame`; speech start also broadcasts a Pipecat interruption
when `should_interrupt=True`.

Runtime-supported settings can be updated without replacing the service:

```python
await stt.update_config(mode="codemix")
```

See `examples/realtime_stt.py` for service construction.

## Supported languages

Sarvam realtime accepts automatic language identification or these language
codes:

- `auto` — automatic language identification
- `as-IN` — Assamese
- `bn-IN` — Bengali
- `brx-IN` — Bodo
- `doi-IN` — Dogri
- `en-IN` — English (India)
- `gu-IN` — Gujarati
- `hi-IN` — Hindi
- `kn-IN` — Kannada
- `kok-IN` — Konkani
- `ks-IN` — Kashmiri
- `mai-IN` — Maithili
- `ml-IN` — Malayalam
- `mni-IN` — Manipuri
- `mr-IN` — Marathi
- `ne-IN` — Nepali
- `or-IN` — Odia
- `pa-IN` — Punjabi
- `sa-IN` — Sanskrit
- `sat-IN` — Santali
- `sd-IN` — Sindhi
- `ta-IN` — Tamil
- `te-IN` — Telugu
- `ur-IN` — Urdu

Not every code has a corresponding Pipecat `Language` enum in Pipecat 1.7.0.
Those codes remain usable through the service's `language_code` setting.

## Connection and error policy

Recovery is explicit and bounded:

- Initial connection failures use exponential backoff.
- Dropped sockets reconnect up to `max_reconnect_attempts`.
- Failed sends and `session.begin` timeouts perform one full reconnect and
  retry the current message once.
- Non-fatal Sarvam API errors produce non-fatal Pipecat error frames.
- Fatal API errors terminate immediately.
- Every malformed message produces an error frame; three consecutive malformed
  messages terminate the session.
- Unexpected `session.end` events reconnect. Expected shutdown does not.
- Exhausted or disabled recovery produces a fatal Pipecat error frame.

Provider failover is deliberately application-controlled. A pipeline can react
to the fatal error and select another STT service.

## Pipecat compatibility

- Requires `pipecat-ai>=1.7.0`
- Requires `websockets>=13.0`
- Supports Python 3.11 and newer
- Live-tested with Python 3.12, `pipecat-ai` 1.7.0, and `websockets` 17.0.1

The service subclasses Pipecat's `WebsocketSTTService` and uses the typed
`STTSettings` API introduced in current Pipecat releases. Compatibility with
older Pipecat versions is not provided.

## Known limitations

- Audio input is mono linear16 PCM at 8 kHz or 16 kHz. The adapter does not yet
  transcode linear32, mu-law, or A-law input.
- The adapter uses Sarvam's WebSocket protocol directly because the tested
  Sarvam Python SDK does not expose this realtime API surface.
- Automatic failover to another STT provider is not built in.
- Runtime updates are limited to the initial public settings supported safely
  by the live protocol.
- The short-interruption suite verifies adapter behavior from Sarvam VAD events
  onward. Acoustic live tests for `नहीं`, `हाँ`, `रुकिए`, and `एक मिनट` require
  corresponding local recordings.
- A multi-region, multi-network production latency benchmark has not yet been
  published.

## Latency benchmark

Pipecat metrics report:

- Time to first transcript: Sarvam `vad.speech_start` to the first non-empty
  partial, or to the final transcript when no partial arrives
- Final transcript latency: Sarvam `vad.speech_start` to `transcript.final`

No stable benchmark numbers are claimed yet. The existing live test is a
correctness check, not a statistically useful benchmark:

```bash
RUN_SARVAM_INTEGRATION=1 \
  pytest -q -s tests/test_live_realtime_stt.py
```

A publishable benchmark should report the language, utterance set, audio
duration, stream type, endpointing mode, client region, network path, sample
count, median, P95, and P99 for both latency measurements.

## Raw protocol verification

The provider-only client validates mono linear16 WAV/PCM input, streams it in
real time, prints every server event, and records JSON Lines:

```bash
python examples/raw_realtime_stt.py test_audio/hindi.wav --dry-run
python examples/raw_realtime_stt.py test_audio/hindi.wav --return-timestamps
```

Events are written to the ignored
`artifacts/sarvam-realtime-events.jsonl` file. Live protocol verification
observed `session.begin`, VAD boundaries, partial and final transcripts, errors,
and `session.end`. For `stream_type=fast`, the endpoint reported a 16,000-byte
frame cap for 16 kHz linear16 input.

## Tests

Run the local suite:

```bash
pytest
```

Focused suites cover connection, final and interim transcription, VAD,
interruption, and errors. The interruption matrix runs Pipecat's real
`broadcast_interruption()` path against a simulated bot-audio sink and verifies
final transcription for `नहीं`, `हाँ`, `रुकिए`, and `एक मिनट`.

## Attribution

This project is maintained independently by community contributors.

- [Pipecat](https://github.com/pipecat-ai/pipecat) provides the pipeline,
  frames, service lifecycle, metrics, and WebSocket STT base classes.
- [Sarvam AI](https://www.sarvam.ai/) provides the realtime speech-to-text API
  and `saaras:v3-realtime` model.
- Protocol behavior was implemented against the
  [Sarvam realtime API documentation](https://docs.sarvam.ai/api/api-guides-tutorials/speech-to-text/realtime-api)
  and verified against the live endpoint.
- The local Hindi development recording is attributed in
  `test_audio/README.md` to the Open Speech Repository.

Pipecat and Sarvam names and trademarks belong to their respective owners. No
affiliation or endorsement is implied.

## License

BSD 2-Clause. See `LICENSE`.
