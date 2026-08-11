# Local test audio

Place a Hindi recording here as `hindi.wav`. Audio files in this directory are
ignored by Git so recordings are not committed accidentally.

The local `hindi.wav` used during development is
`OSR_in_000_0062_16k.wav` from the
[Open Speech Repository](https://www.voiptroubleshooter.com/open_speech/india.html).
Its usage terms permit testing and research with source attribution.

The raw protocol client accepts:

- Mono, uncompressed, 16-bit PCM WAV at 8 kHz or 16 kHz
- Headerless linear16 PCM (`.pcm` or `.raw`) at 8 kHz or 16 kHz

Convert an existing recording with FFmpeg:

```bash
ffmpeg -i input-audio.ext -ac 1 -ar 16000 -c:a pcm_s16le test_audio/hindi.wav
```

Validate it without connecting:

```bash
python examples/raw_realtime_stt.py test_audio/hindi.wav --dry-run
```

Run the live protocol test:

```bash
python examples/raw_realtime_stt.py test_audio/hindi.wav
```

The client streams at most the first 15 seconds by default to keep diagnostic
API usage small. Override this with `--max-audio-seconds`.

Every provider event is printed and written to
`artifacts/sarvam-realtime-events.jsonl`.
