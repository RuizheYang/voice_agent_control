import unittest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch, MagicMock

from app.clue_handler.clue import ClueHandler
from app.clue_handler.contracts import (
    BaseCancelCallHandler,
    CancelClueResult,
    BaseNewClueHandler,
    BaseScheduleHandler,
    BasePostCallHandler,
)
from app.clue_handler.stages.postcall import DefaultPostCallHandler as PostCallHandler

from app.schemas.clue_create import NormalizedClue, ScheduledTaskRequest
from app.schemas.call_end import CallEndRequest, CallEndData


@asynccontextmanager
async def mock_process_lock(clue_id: str, action_name: str):
    yield


class TestClueHandler(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.mock_cancel = AsyncMock(spec=BaseCancelCallHandler)
        self.mock_cancel.handle.return_value = CancelClueResult(success=True)
        self.mock_handlers = {
            "new_clue_handler": AsyncMock(spec=BaseNewClueHandler),
            "schedule_handler": AsyncMock(spec=BaseScheduleHandler),
            "cancel_call_handler": self.mock_cancel,
            "postcall_handler": AsyncMock(spec=PostCallHandler),
        }
        self.clue_handler = ClueHandler(**self.mock_handlers)

        self.lock_patcher = patch("app.clue_handler.clue.process_lock", mock_process_lock)
        self.lock_patcher.start()

        self.mock_dq = MagicMock()
        self.mock_dq.remove_task = AsyncMock()
        self.mock_dq.add_task = AsyncMock()

        self.dq_patcher = patch(
            "app.clue_handler.clue.redis_manager.get_delayed_queue",
            return_value=self.mock_dq,
        )
        self.dq_patcher.start()

        self.eb_patcher = patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock)
        self.eb_patcher.start()

        self.webhook_patcher = patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock)
        self.webhook_patcher.start()

    async def asyncTearDown(self):
        self.lock_patcher.stop()
        self.dq_patcher.stop()
        self.eb_patcher.stop()
        self.webhook_patcher.stop()

    @patch("app.clue_handler.clue.crud.increment_total_call_count")
    @patch("app.clue_handler.clue.crud.update_clue_state_or_raise")
    @patch("app.clue_handler.clue.crud.get_clue_state", return_value="created")
    @patch("app.clue_handler.clue.crud.get_clue_info")
    async def test_on_call_start(self, mock_get_info, mock_get_state, mock_update_state, mock_inc):
        clue_id = "test_123"
        mock_info = MagicMock()
        mock_info.agent_id = "agent_a"
        mock_info.strategy_id = "strat_s"
        mock_get_info.return_value = mock_info

        await self.clue_handler.on_call_start(clue_id)
        mock_update_state.assert_called_once_with(clue_id, "calling")
        mock_inc.assert_called_once_with(clue_id)

    async def test_on_cancel(self):
        clue_id = "test_cancel_123"
        await self.clue_handler.on_cancel(clue_id)
        self.mock_handlers["cancel_call_handler"].handle.assert_called_once_with(clue_id, None)

    @patch("app.clue_handler.clue.create_clue")
    @patch("app.clue_handler.clue.redis_manager.rpush_list", new_callable=AsyncMock)
    async def test_on_clue_pushed(self, mock_rpush, mock_create_clue):
        mock_clue = NormalizedClue.model_construct(clueId="push_123")
        await self.clue_handler.on_clue_pushed(mock_clue)
        mock_rpush.assert_called_once()
        mock_create_clue.assert_called_once_with(mock_clue)

    @patch("app.clue_handler.clue.create_clue")
    async def test_on_new_clue(self, mock_create_clue):
        mock_clue = NormalizedClue.model_construct(clueId="new_123")
        await self.clue_handler.on_new_clue(mock_clue)
        mock_create_clue.assert_called_once_with(mock_clue)
        self.mock_handlers["new_clue_handler"].handle.assert_called_once_with(mock_clue)

    async def test_on_scheduled_task(self):
        mock_task = ScheduledTaskRequest.model_construct(clueId="sched_123")
        await self.clue_handler.on_scheduled_task(mock_task)
        self.mock_handlers["schedule_handler"].handle.assert_called_once_with(mock_task)

    async def test_on_call_end(self):
        mock_data = CallEndData.model_construct(clueId="end_123")
        mock_info = CallEndRequest.model_construct(data=mock_data, requestId="req_999")
        await self.clue_handler.on_call_end(mock_info)
        self.mock_handlers["postcall_handler"].handle.assert_called_once_with(mock_info)

    async def test_on_clue_queue(self):
        mock_clue = NormalizedClue.model_construct(clueId="queue_123")
        await self.clue_handler.on_clue_queue(mock_clue)
        self.mock_handlers["new_clue_handler"].handle.assert_called_once_with(mock_clue)


if __name__ == "__main__":
    unittest.main()
