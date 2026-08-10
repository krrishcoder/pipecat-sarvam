"""Construct Sarvam realtime STT for use in a Pipecat pipeline."""

import os

from pipecat_sarvam import SarvamRealtimeSTTService


def create_stt() -> SarvamRealtimeSTTService:
    """Create the service that can be inserted into a Pipecat pipeline."""
    api_key = os.getenv("SARVAM_API_KEY")
    if not api_key:
        raise RuntimeError("Export SARVAM_API_KEY before creating the service")

    return SarvamRealtimeSTTService(
        api_key=api_key,
        settings=SarvamRealtimeSTTService.Settings(
            language_code="hi-IN",
            stream_type="fast",
            endpointing="vad",
            mode="transcribe",
            return_timestamps=True,
        ),
    )


if __name__ == "__main__":
    stt = create_stt()
    print(f"Created {stt.name}; add it after transport.input() in a Pipecat pipeline.")
