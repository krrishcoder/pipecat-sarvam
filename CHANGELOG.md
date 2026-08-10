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
- Unit and opt-in live integration tests.
