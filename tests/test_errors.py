"""Protocol and provider error tests."""

from unittest.mock import AsyncMock

import pytest
from websockets.protocol import State

from pipecat_sarvam import SarvamRealtimeSTTError, SarvamRealtimeSTTService


class IncomingWebSocket:
    """WebSocket that yields a predefined sequence of server messages."""

    state = State.OPEN

    def __init__(self, messages: list[str]) -> None:
        self.messages = messages

    async def __aiter__(self):
        for message in self.messages:
            yield message


@pytest.mark.asyncio
async def test_single_malformed_message_is_reported_without_termination() -> None:
    """An isolated malformed message should be visible while the session continues."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service._websocket = IncomingWebSocket(  # type: ignore[assignment]
        ["not-json", '{"event": "pong"}']
    )
    service.push_error = AsyncMock()  # type: ignore[method-assign]
    service._call_event_handler = AsyncMock()  # type: ignore[method-assign]

    await service._receive_messages()

    service.push_error.assert_awaited_once()
    assert service.push_error.await_args.kwargs.get("fatal", False) is False
    assert not service._disconnecting


@pytest.mark.asyncio
async def test_fatal_provider_error_emits_fatal_pipecat_error() -> None:
    """Sarvam fatal errors should terminate rather than enter a retry loop."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service.push_error = AsyncMock()  # type: ignore[method-assign]
    service.stop_all_metrics = AsyncMock()  # type: ignore[method-assign]
    service._call_event_handler = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(SarvamRealtimeSTTError, match="invalid_api_key"):
        await service._handle_error(
            {
                "event": "error",
                "code": "invalid_api_key",
                "message": "authentication failed",
                "is_fatal": True,
            }
        )

    assert service.push_error.await_args.kwargs["fatal"] is True
    assert service._disconnecting
