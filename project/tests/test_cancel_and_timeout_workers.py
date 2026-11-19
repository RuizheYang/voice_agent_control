import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from app.clue_handler.stages.cancel import DefaultCancelHandler as CancelCallHandler, CancelClueResult
from app.clue_handler.state import ClueState
from app.observability.events_bus import ClueEndEvent, ClueTimeoutWarningEvent
from app.utils.workers import TaskDispatcher


class TestCancelCallHandler(unittest.IsolatedAsyncioTestCase):

    async def test_cancel_call_handler_normal_flow(self):
        """测试正常取消/超时兜底流程：置为 REJECTED、清理延迟队列、推送 Webhook clue_end 与 ClueEndEvent"""
        handler = CancelCallHandler()
        clue_id = "clue_cancel_test_001"
        close_reason = "calling_timeout"

        with patch("app.dbmodels.crud.get_clue_state", new_callable=AsyncMock) as mock_get_state, \
             patch("app.dbmodels.crud.update_clue_state_or_raise", new_callable=AsyncMock) as mock_update_state, \
             patch("app.dbmodels.crud.save_close_reason", new_callable=AsyncMock) as mock_save_reason, \
             patch("app.redis_client.redis_manager.redis_manager.get_delayed_queue") as mock_get_dq, \
             patch("app.apis.webhook_dispatcher.dispatch_webhook_event", new_callable=AsyncMock) as mock_dispatch_webhook, \
             patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock) as mock_emit, \
             patch("app.dbmodels.models.ClueInfo.filter") as mock_clue_filter:

            mock_get_state.return_value = "calling"
            mock_dq = MagicMock()
            mock_dq.remove_task = AsyncMock()
            mock_get_dq.return_value = mock_dq

            mock_clue_obj = MagicMock()
            mock_clue_obj.workflow_id = "wf_cancel_test"
            mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue_obj)

            result = await handler.handle(clue_id, closeReason=close_reason)

            self.assertTrue(result.success)
            mock_update_state.assert_called_once_with(clue_id, ClueState.REJECTED.value)
            mock_save_reason.assert_called_once_with(clue_id, close_reason)

            # 验证延迟队列清理
            self.assertEqual(mock_dq.remove_task.call_count, 4)

            # V2 修复重复推送 Bug：统一由 EventBus 发射 ClueEndEvent 投递（不再重复直调 dispatch_webhook_event）
            mock_dispatch_webhook.assert_not_called()

            # 验证 EventBus 发射 ClueEndEvent
            mock_emit.assert_called_once()
            emitted_event = mock_emit.call_args[0][0]
            self.assertIsInstance(emitted_event, ClueEndEvent)
            self.assertEqual(emitted_event.clue_id, clue_id)
            self.assertEqual(emitted_event.workflow_id, "wf_cancel_test")
            self.assertEqual(emitted_event.event_data["code"], 300)
            self.assertEqual(emitted_event.event_data["reason"], close_reason)

    async def test_cancel_call_handler_terminal_state_idempotent(self):
        """测试终态幂等保护：如果线索已经处于终态 (如 accepted)，跳过取消与状态更新"""
        handler = CancelCallHandler()
        clue_id = "clue_cancel_terminal_002"

        with patch("app.dbmodels.crud.get_clue_state", new_callable=AsyncMock) as mock_get_state, \
             patch("app.dbmodels.crud.update_clue_state_or_raise", new_callable=AsyncMock) as mock_update_state:

            mock_get_state.return_value = "accepted"

            result = await handler.handle(clue_id, closeReason="test_cancel")

            self.assertTrue(result.success)
            mock_update_state.assert_not_called()


class TestTimeoutWorkers(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.dispatcher = TaskDispatcher()

    async def test_handle_calling_grace_when_still_calling(self):
        """测试通话中超时：线索仍处于 CALLING 状态，成功调用 on_cancel(closeReason='calling_timeout')"""
        clue_id = "clue_calling_timeout_001"
        mock_handler = MagicMock()
        mock_handler.on_cancel = AsyncMock(return_value=CancelClueResult(success=True))

        with patch("app.utils.workers.get_clue_state", new_callable=AsyncMock) as mock_get_state, \
             patch("app.clue_handler.factory.ClueHandlerFactory.get_handler_by_clue", new_callable=AsyncMock) as mock_get_h, \
             patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock) as mock_emit:

            mock_get_state.return_value = ClueState.CALLING.value
            mock_get_h.return_value = mock_handler

            await self.dispatcher.handle_calling_grace(clue_id)

            mock_handler.on_cancel.assert_called_once_with(clue_id, closeReason="calling_timeout")
            mock_emit.assert_called_once()
            warn_event = mock_emit.call_args[0][0]
            self.assertIsInstance(warn_event, ClueTimeoutWarningEvent)
            self.assertEqual(warn_event.event_type, "calling_timeout_warning")

    async def test_handle_calling_grace_skipped_if_state_changed(self):
        """测试通话中超时：若线索已不在 CALLING 状态 (例如已挂机 accepted)，跳过兜底"""
        clue_id = "clue_calling_skipped_002"
        mock_handler = MagicMock()
        mock_handler.on_cancel = AsyncMock()

        with patch("app.utils.workers.get_clue_state", new_callable=AsyncMock) as mock_get_state, \
             patch("app.clue_handler.factory.ClueHandlerFactory.get_handler_by_clue", new_callable=AsyncMock) as mock_get_h:

            mock_get_state.return_value = ClueState.ACCEPTED.value
            mock_get_h.return_value = mock_handler

            await self.dispatcher.handle_calling_grace(clue_id)

            mock_handler.on_cancel.assert_not_called()

    async def test_handle_wait_for_calling_timeout(self):
        """测试 wait_for_calling 超时：调用 on_cancel(closeReason='wait_for_calling_timeout')"""
        clue_id = "clue_wfc_timeout_003"
        mock_handler = MagicMock()
        mock_handler.on_cancel = AsyncMock(return_value=CancelClueResult(success=True))

        with patch("app.utils.workers.get_clue_state", new_callable=AsyncMock) as mock_get_state, \
             patch("app.clue_handler.factory.ClueHandlerFactory.get_handler_by_clue", new_callable=AsyncMock) as mock_get_h, \
             patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock):

            mock_get_state.return_value = ClueState.WAIT_FOR_CALLING.value
            mock_get_h.return_value = mock_handler

            await self.dispatcher.handle_wait_for_calling(clue_id)

            mock_handler.on_cancel.assert_called_once_with(clue_id, closeReason="wait_for_calling_timeout")

    async def test_handle_summary_timeout(self):
        """测试 summary 超时：调用 on_cancel(closeReason='summary_timeout')"""
        clue_id = "clue_summary_timeout_004"
        mock_handler = MagicMock()
        mock_handler.on_cancel = AsyncMock(return_value=CancelClueResult(success=True))

        with patch("app.utils.workers.get_clue_state", new_callable=AsyncMock) as mock_get_state, \
             patch("app.clue_handler.factory.ClueHandlerFactory.get_handler_by_clue", new_callable=AsyncMock) as mock_get_h, \
             patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock):

            mock_get_state.return_value = ClueState.SUMMARY.value
            mock_get_h.return_value = mock_handler

            await self.dispatcher.handle_summary_timeout(clue_id)

            mock_handler.on_cancel.assert_called_once_with(clue_id, closeReason="summary_timeout")

    async def test_handle_pending_timeout(self):
        """测试 pending 超时：调用 on_cancel(closeReason='pending_timeout')"""
        clue_id = "clue_pending_timeout_005"
        mock_handler = MagicMock()
        mock_handler.on_cancel = AsyncMock(return_value=CancelClueResult(success=True))

        with patch("app.utils.workers.get_clue_state", new_callable=AsyncMock) as mock_get_state, \
             patch("app.clue_handler.factory.ClueHandlerFactory.get_handler_by_clue", new_callable=AsyncMock) as mock_get_h, \
             patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock):

            mock_get_state.return_value = ClueState.PENDING.value
            mock_get_h.return_value = mock_handler

            await self.dispatcher.handle_pending(clue_id)

            mock_handler.on_cancel.assert_called_once_with(clue_id, closeReason="pending_timeout")


if __name__ == "__main__":
    unittest.main()
