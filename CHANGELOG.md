# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial Python package, example, and test structure.
- Raw Sarvam realtime WebSocket protocol probe with WAV/PCM validation.
- `SarvamRealtimeSTTService` with typed settings, server VAD, transcript frame
  mapping, interruption support, metrics, keepalive, reconnection, runtime
  updates, graceful shutdown, and error handling.
- Bounded retry and fatal termination policies for connection failures,
  malformed messages, session timeouts, and unexpected session termination.
- Unit and opt-in live integration tests.
- Browser harness under `ui/` that runs the service inside a real Pipecat
  pipeline and displays the frames it emits, served over one port with no
  dependency beyond `websockets`.
- `resolve_sarvam_language_code` is now exported from the package root.
- `SUPPORTED_LANGUAGE_CODES` is now exported from the package root, and the
  browser harness validates against it instead of mirroring the list.
- README section comparing this package with Pipecat's built-in
  `SarvamSTTService`, which targets a different Sarvam endpoint.
- `examples/realtime_stt_bot.py`, a runnable single-file Pipecat bot: browser
  microphone in, Sarvam transcription out, printing each partial and then the
  final with the elapsed time since Sarvam detected speech.
- Packaging metadata for a PyPI release: keywords, fuller classifiers,
  Documentation and Changelog URLs, and an explicit source-distribution file
  list that excludes the browser harness and local audio fixtures.
- A test asserting that `pipecat_sarvam.__version__` and the version in
  `pyproject.toml` agree, so a release cannot ship a mismatched version.

### Removed

- `examples/realtime_stt.py`, which only constructed the service. Superseded by
  `examples/realtime_stt_bot.py`.

### Fixed

- Report real utterance latency when interruptions are enabled. Pipecat flushes
  all active metrics on an interruption broadcast, so time to first transcript
  and final-transcript latency were reported as a few milliseconds of internal
  overhead instead of the real values. The utterance is now anchored before the
  broadcast, and the timers are re-armed if an interruption arrives from
  elsewhere in the pipeline mid-utterance.
- Cancel the previous keepalive task before reconnecting. `WebsocketSTTService`
  overwrites the handle unconditionally, so a stale task outlived its socket.
- Latch termination after a fatal error. `WebsocketService._connect` resets
  `_disconnecting`, which let the next audio frame silently rebuild a connection
  the pipeline had already been told was dead.
- Replay audio buffered during recovery instead of discarding it, so an
  utterance spanning a reconnect is no longer truncated.
- Restore the Sarvam time-to-first-byte anchor under `endpointing="manual"`.
  Pipecat re-anchors to the end of the VAD segment, which reported a much
  smaller number than the documented benchmark.
- Give an explicitly supplied `language_code` precedence over `language` in
  runtime updates, matching how the constructor already resolved them.
- Accept the Sarvam language codes that have no Pipecat `Language` member, so
  every advertised code stays reachable.
- Spawn only one deferred settings reconnect when finals arrive back to back.
- Accept Sarvam's other Odia spelling. This realtime endpoint requires `or-IN` and
  rejects `od-IN`, but Sarvam's batch, SDK-streaming, translation, and
  text-to-speech APIs use `od-IN`, so `od-IN` is now normalized instead of
  rejected. A transcript tagged either way maps back to `Language.OR_IN`.
