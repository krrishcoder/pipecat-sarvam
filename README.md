# Pipecat Sarvam

Community-maintained Sarvam AI integration for
[Pipecat](https://github.com/pipecat-ai/pipecat).

The first planned service is `SarvamRealtimeSTTService`, a streaming
speech-to-text adapter for Sarvam's `saaras:v3-realtime` WebSocket API.

> [!NOTE]
> This repository is currently an implementation scaffold. The realtime
> service is not usable yet.

## Planned features

- Streaming PCM audio to Sarvam over WebSocket
- Interim and final Pipecat transcription frames
- Server-side VAD and user speaking frames
- Barge-in/interruption support
- Metrics, errors, and connection lifecycle handling

## Development setup

Python 3.11 or 3.12 is recommended.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Run the tests:

```bash
pytest
```

## Configuration

The completed service will read the Sarvam API key from the constructor. Keep
the key in an environment variable and never commit it:

```bash
export SARVAM_API_KEY="your-api-key"
```

## Planned usage

After the service is implemented, it will be importable as:

```python
from pipecat_sarvam import SarvamRealtimeSTTService
```

See `examples/realtime_stt.py` for the foundational example as it is developed.

## Compatibility

Pipecat compatibility will be documented after the first working
implementation is tested against a released `pipecat-ai` version.

## Project status

The source code is intended to remain in this community repository. A separate
documentation pull request can register the integration in Pipecat's supported
services catalog once the implementation, example, and tests are complete.

## License

BSD 2-Clause. See `LICENSE`.
