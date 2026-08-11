"""Connection contract tests for Sarvam realtime STT."""

from unittest.mock import AsyncMock

import pytest
from websockets.protocol import State

from pipecat_sarvam import SarvamRealtimeSTTService


class OpenWebSocket:
    """Minimal connected WebSocket."""

    state = State.OPEN


@pytest.mark.asyncio
async def test_connection_uses_subscription_key_and_timeout() -> None:
    """The provider handshake should carry auth and an explicit timeout."""
    service = SarvamRealtimeSTTService(
        api_key="secret-key",
        connection_timeout=7.5,
        keepalive_timeout=None,
    )
    websocket = OpenWebSocket()
    service._websocket_connect = AsyncMock(  # type: ignore[method-assign]
        return_value=websocket
    )
    service._call_event_handler = AsyncMock()  # type: ignore[method-assign]

    await service._connect_websocket()

    _, kwargs = service._websocket_connect.await_args
    assert kwargs["additional_headers"] == {"API-SUBSCRIPTION-KEY": "secret-key"}
    assert kwargs["open_timeout"] == 7.5
    assert kwargs["ping_interval"] is None
    service._call_event_handler.assert_awaited_once_with("on_connected")
