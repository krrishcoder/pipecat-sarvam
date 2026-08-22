"""Keepalive, termination and reconnect recovery tests."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from pipecat.frames.frames import InputAudioRawFrame
from pipecat.processors.frame_processor import FrameDirection
from websockets.protocol import State

from pipecat_sarvam import SarvamRealtimeSTTService


def audio_frame() -> InputAudioRawFrame:
    """Build a minimal 16 kHz mono frame for buffer-replay assertions."""
    return InputAudioRawFrame(audio=b"\x00\x00", sample_rate=16000, num_channels=1)


class FakeWebSocket:
    """WebSocket that records sends and can be made to fail every send."""

    def __init__(self, *, fail: bool = False) -> None:
        self.state = State.OPEN
        self.fail = fail
        self.sent: list[str] = []

    async def send(self, payload: str) -> None:
        if self.fail:
            raise ConnectionError("socket is gone")
        self.sent.append(payload)


def collect_tasks(service: SarvamRealtimeSTTService) -> list[MagicMock]:
    """Replace ``create_task`` with a stub and return the list it appends to.

    Nothing is scheduled: the coroutine is closed so it cannot warn about never
    being awaited, and the stand-in task starts out as not done.
    """
    tasks: list[MagicMock] = []

    def create_task(coroutine, name=None, context=None):
        coroutine.close()
        task = MagicMock()
        task.done.return_value = False
        tasks.append(task)
        return task

    service.create_task = create_task  # type: ignore[method-assign]
    return tasks


@pytest.mark.asyncio
async def test_reconnect_does_not_orphan_the_previous_keepalive_task() -> None:
    """A second connect must cancel the keepalive task from the first.

    ``WebsocketSTTService._connect`` calls ``_create_keepalive_task`` and
    overwrites the handle unconditionally, so without an explicit cancel the
    old task would keep pinging a socket that is already gone.
    """
    service = SarvamRealtimeSTTService(api_key="test-key")
    tasks = collect_tasks(service)
    service.cancel_task = AsyncMock()  # type: ignore[method-assign]
    service._connect_websocket = AsyncMock()  # type: ignore[method-assign]

    await service._connect()
    await service._connect()

    assert len(tasks) == 2
    service.cancel_task.assert_awaited_once_with(tasks[0])
    assert service._keepalive_task is tasks[1]


@pytest.mark.asyncio
async def test_connect_is_inert_after_a_fatal_error() -> None:
    """A fatal error ends recovery; the pipeline owns provider failover."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service.push_error = AsyncMock()  # type: ignore[method-assign]
    service.stop_all_metrics = AsyncMock()  # type: ignore[method-assign]
    service._call_event_handler = AsyncMock()  # type: ignore[method-assign]
    service._disconnect_websocket = AsyncMock()  # type: ignore[method-assign]

    await service._terminate_connection("authentication failed")
    assert service._terminated is True

    service._connect_websocket = AsyncMock()  # type: ignore[method-assign]
    service.create_task = MagicMock()  # type: ignore[method-assign]

    await service._connect()

    service._connect_websocket.assert_not_awaited()
    service.create_task.assert_not_called()


@pytest.mark.asyncio
async def test_run_stt_stops_sending_after_a_fatal_error() -> None:
    """The next audio frame must not resurrect a terminated session."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service._terminated = True
    service._connect = AsyncMock()  # type: ignore[method-assign]
    service._send_json = AsyncMock()  # type: ignore[method-assign]

    assert [frame async for frame in service.run_stt(b"\x00\x00")] == [None]

    service._connect.assert_not_awaited()
    service._send_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_stt_stops_when_connect_terminates_the_session() -> None:
    """A connect that exhausts its attempts terminates; audio must not follow."""
    service = SarvamRealtimeSTTService(api_key="test-key")

    async def terminate() -> None:
        service._terminated = True

    service._connect = AsyncMock(side_effect=terminate)  # type: ignore[method-assign]
    service._ensure_session_ready = AsyncMock()  # type: ignore[method-assign]
    service._send_json = AsyncMock()  # type: ignore[method-assign]

    assert [frame async for frame in service.run_stt(b"\x00\x00")] == [None]

    service._connect.assert_awaited_once()
    service._ensure_session_ready.assert_not_awaited()
    service._send_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_recovery_replays_audio_buffered_during_the_outage() -> None:
    """Audio arriving mid-recovery is buffered, so it has to be replayed."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    frame = audio_frame()

    async def reconnect() -> None:
        # This is what STTService.process_audio_frame does while _reconnecting.
        assert service._reconnecting is True
        service._reconnect_audio_buffer.append((frame, FrameDirection.DOWNSTREAM))
        service._session_ready.set()

    service._do_reconnect = AsyncMock(side_effect=reconnect)  # type: ignore[method-assign]
    service.process_audio_frame = AsyncMock()  # type: ignore[method-assign]

    assert await service._recover_connection("dropped socket") is True

    service.process_audio_frame.assert_awaited_once_with(frame, FrameDirection.DOWNSTREAM)
    assert service._reconnect_audio_buffer == []
    assert service._reconnecting is False


@pytest.mark.asyncio
async def test_replay_drains_audio_that_arrives_while_it_runs() -> None:
    """Replay must re-check the buffer, not take a single snapshot of it."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    first, second = audio_frame(), audio_frame()
    replayed: list[InputAudioRawFrame] = []

    async def process(frame, direction) -> None:
        assert service._replaying_audio_buffer is True
        replayed.append(frame)
        if frame is first:
            service._reconnect_audio_buffer.append((second, direction))

    service.process_audio_frame = AsyncMock(side_effect=process)  # type: ignore[method-assign]
    service._reconnect_audio_buffer.append((first, FrameDirection.DOWNSTREAM))

    await service._replay_buffered_audio()

    assert replayed == [first, second]
    assert service._replaying_audio_buffer is False


@pytest.mark.asyncio
async def test_replay_is_guarded_against_re_entry() -> None:
    """Replaying goes back through run_stt, which can trigger another recovery.

    A nested call must leave the buffer to the replay already draining it,
    rather than delivering the same audio twice.
    """
    service = SarvamRealtimeSTTService(api_key="test-key")
    service.process_audio_frame = AsyncMock()  # type: ignore[method-assign]
    service._reconnect_audio_buffer.append((audio_frame(), FrameDirection.DOWNSTREAM))
    service._replaying_audio_buffer = True

    await service._replay_buffered_audio()

    service.process_audio_frame.assert_not_awaited()
    assert len(service._reconnect_audio_buffer) == 1


@pytest.mark.asyncio
async def test_failed_send_recovers_once_and_resends_the_event() -> None:
    """A socket that dies mid-send must not silently swallow the event."""
    service = SarvamRealtimeSTTService(api_key="test-key")
    service._websocket = FakeWebSocket(fail=True)  # type: ignore[assignment]
    service._report_recoverable_error = AsyncMock()  # type: ignore[method-assign]
    healthy = FakeWebSocket()

    async def recover(reason: str) -> bool:
        service._websocket = healthy  # type: ignore[assignment]
        return True

    service._recover_connection = AsyncMock(side_effect=recover)  # type: ignore[method-assign]

    await service._send_json({"event": "speech_start"})

    service._recover_connection.assert_awaited_once()
    assert [json.loads(payload) for payload in healthy.sent] == [{"event": "speech_start"}]


@pytest.mark.asyncio
async def test_back_to_back_finals_spawn_one_settings_reconnect() -> None:
    """``_need_reconnect`` is only cleared once ``_reconnect`` actually runs.

    Two finals in quick succession would otherwise start overlapping reconnect
    tasks that fight over the same socket.
    """
    service = SarvamRealtimeSTTService(api_key="test-key")
    service.stop_processing_metrics = AsyncMock()  # type: ignore[method-assign]
    service._need_reconnect = True
    tasks = collect_tasks(service)

    await service._finish_utterance_metrics()
    await service._finish_utterance_metrics()

    assert len(tasks) == 1
    assert service._settings_reconnect_task is tasks[0]

    # Once the deferred reconnect has finished, a later turn may start another.
    tasks[0].done.return_value = True
    await service._finish_utterance_metrics()

    assert len(tasks) == 2
