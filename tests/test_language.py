"""Language resolution and live settings-update tests."""

import pytest
from pipecat.transcriptions.language import Language

from pipecat_sarvam import SarvamRealtimeSTTService, resolve_sarvam_language_code
from pipecat_sarvam.realtime_stt import sarvam_language_to_pipecat

# Sarvam advertises these, but Pipecat has no matching Language member.
CODES_WITHOUT_PIPECAT_LANGUAGE = ["brx-IN", "doi-IN", "ks-IN", "mni-IN", "ne-IN", "sa-IN", "sat-IN"]


@pytest.mark.parametrize("supplied", ["or-IN", "od-IN"])
def test_odia_normalizes_to_the_realtime_spelling(supplied: str) -> None:
    """The realtime endpoint accepts or-IN and rejects od-IN.

    Sarvam's other speech APIs use od-IN, and upstream Pipecat's SarvamSTTService
    sends od-IN to /speech-to-text/ws, so callers arriving from either place will
    try either spelling. Accept both; put or-IN on the wire.
    """
    assert resolve_sarvam_language_code(supplied) == "or-IN"

    service = SarvamRealtimeSTTService(
        api_key="test-key",
        settings=SarvamRealtimeSTTService.Settings(language_code=supplied),
    )

    assert service._settings.language_code == "or-IN"
    assert "language_code=or-IN" in service._build_websocket_url()


def test_odia_enum_resolves_to_the_realtime_spelling() -> None:
    """Language.OR_IN already matches what this endpoint wants."""
    assert resolve_sarvam_language_code(Language.OR_IN) == "or-IN"


def test_odia_round_trips_back_to_a_pipecat_language() -> None:
    """A transcript tagged with either spelling must keep its language.

    ``Language("od-IN")`` raises, so od-IN needs an explicit alias to avoid
    silently returning None.
    """
    assert sarvam_language_to_pipecat("or-IN") is Language.OR_IN
    assert sarvam_language_to_pipecat("od-IN") is Language.OR_IN


@pytest.mark.parametrize("code", CODES_WITHOUT_PIPECAT_LANGUAGE)
def test_codes_without_a_pipecat_language_pass_through(code: str) -> None:
    """Every advertised Sarvam code must stay reachable as a plain string."""
    assert resolve_sarvam_language_code(code) == code


def test_pipecat_language_maps_to_a_sarvam_code() -> None:
    """A Language enum still resolves through the mapping table."""
    assert resolve_sarvam_language_code(Language.HI_IN) == "hi-IN"


def test_language_outside_sarvam_support_is_rejected() -> None:
    """A real Pipecat language Sarvam does not serve must fail loudly."""
    with pytest.raises(ValueError, match="Unsupported Sarvam realtime language"):
        resolve_sarvam_language_code("fr-FR")


@pytest.mark.parametrize("code", CODES_WITHOUT_PIPECAT_LANGUAGE)
def test_service_accepts_a_code_with_no_pipecat_language(code: str) -> None:
    """Constructing the service with such a code must not raise."""
    service = SarvamRealtimeSTTService(
        api_key="test-key",
        settings=SarvamRealtimeSTTService.Settings(language_code=code),
    )
    assert service._settings.language_code == code


def test_init_gives_language_code_precedence_over_language() -> None:
    """An explicitly supplied language_code is the wire parameter Sarvam sees."""
    service = SarvamRealtimeSTTService(
        api_key="test-key",
        settings=SarvamRealtimeSTTService.Settings(
            language=Language.TA_IN,
            language_code="bn-IN",
        ),
    )
    assert service._settings.language_code == "bn-IN"


@pytest.mark.asyncio
async def test_language_alone_derives_the_language_code() -> None:
    """Updating the generic language field must reach the wire parameter."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    assert service._settings.language_code == "hi-IN"

    changed = await service.update_config(language=Language.TA_IN)

    assert service._settings.language_code == "ta-IN"
    assert changed["language_code"] == "hi-IN"


@pytest.mark.asyncio
async def test_update_keeps_language_code_precedence() -> None:
    """A runtime update must resolve the same way __init__ does."""
    service = SarvamRealtimeSTTService(api_key="test-key")

    await service.update_config(language=Language.TA_IN, language_code="bn-IN")

    assert service._settings.language_code == "bn-IN"
