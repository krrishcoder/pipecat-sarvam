"""Foundational tests for the Sarvam realtime STT package."""

import pipecat_sarvam
import pipecat_sarvam.realtime_stt


def test_package_is_importable() -> None:
    """The source-layout package and service module should be importable."""
    assert pipecat_sarvam.__version__ == "0.1.0.dev0"
