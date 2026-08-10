# Pipecat Sarvam

Community-maintained Sarvam AI integration for
[Pipecat](https://github.com/pipecat-ai/pipecat).

The first service is `SarvamRealtimeSTTService`, a streaming
speech-to-text adapter for Sarvam's `saaras:v3-realtime` WebSocket API.

> [!NOTE]
> This is an early community integration tested with `pipecat-ai` 1.7.0.

## Features

- Streaming PCM audio to Sarvam over WebSocket
- Interim and final Pipecat transcription frames
- Server-side or manual endpointing
- Server VAD mapped to Pipecat user-speaking frames
- Barge-in/interruption support
- Runtime `config.update` support
- Metrics, keepalive, errors, reconnects, and graceful session shutdown

## Development setup

Python 3.11 or 3.12 is recommended.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Keep the API key in an environment variable or an ignored `.env` file:

```bash
export SARVAM_API_KEY="your-api-key"
```

## Usage

```python
import os

from pipecat_sarvam import SarvamRealtimeSTTService

stt = SarvamRealtimeSTTService(
    api_key=os.environ["SARVAM_API_KEY"],
    settings=SarvamRealtimeSTTService.Settings(
        language_code="hi-IN",
        stream_type="fast",
        endpointing="vad",
        mode="transcribe",
        encoding="linear16",
        sample_rate=16000,
    ),
)
```

Insert `stt` after `transport.input()` in a normal Pipecat pipeline. Server VAD
is advertised through `STTMetadataFrame` as the external user-turn strategy.
See `examples/realtime_stt.py` for service construction.

The initial public settings are deliberately limited to `language_code`,
`mode`, `stream_type`, `endpointing`, `encoding`, and `sample_rate`. Input is
mono linear16 PCM at 8 kHz or 16 kHz; unsupported encodings fail during
construction rather than sending mislabeled audio.

Runtime-supported settings can be changed without replacing the service:

```python
await stt.update_config(mode="codemix")
```

The model is locked to `saaras:v3-realtime`. Input encoding and sample rate
cannot be changed after construction.

With Pipecat metrics enabled, Sarvam `vad.speech_start` starts both latency
clocks. The first non-empty partial (or final when no partial arrives) emits
TTFB as time to first transcript; the final transcript emits processing time as
speech-start-to-final latency. Server-owned endpointing is also advertised with
an `STTMetadataFrame` and external user-turn strategies.

## Raw protocol verification

The provider-only diagnostic client has no Pipecat imports. It safely reads
`SARVAM_API_KEY` from the environment or `.env`, validates mono linear16 audio,
streams in real time, and records every server event:

```bash
python examples/raw_realtime_stt.py test_audio/hindi.wav --dry-run
python examples/raw_realtime_stt.py test_audio/hindi.wav --return-timestamps
```

Events are written to the ignored
`artifacts/sarvam-realtime-events.jsonl` file. The live protocol returned:

- `session.begin` with resolved configuration and `request_id`
- `vad.speech_start` and `vad.speech_end` with `utterance_idx`
- `transcript.partial` with `text`
- `transcript.final` with `text`, `start_s`, and `end_s`
- `session.end` with audio/session duration and utterance count
- `error` with `code`, `message`, `is_fatal`, and constraint metadata

For `stream_type=fast`, the live endpoint reported a 16,000-byte per-frame cap
for 16 kHz linear16 input. It also resolved `silence_duration_ms` to 1000 when
the parameter was omitted.

## Tests

Run local tests:

```bash
pytest
```

Run the opt-in live adapter test:

```bash
RUN_SARVAM_INTEGRATION=1 pytest -q -s tests/test_live_realtime_stt.py
```

## Compatibility

Tested with Python 3.12, `pipecat-ai` 1.7.0, and `websockets` 17.0.1.

## Project status

The source code is intended to remain in this community repository. A separate
documentation pull request can register the integration in Pipecat's supported
services catalog after broader testing and review.

## License

BSD 2-Clause. See `LICENSE`.
