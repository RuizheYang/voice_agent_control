import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from app.apis.telephony import LineType, send_call
from app.call_schedule.contracts import ScheduledCallRequest
from app.clue_handler.stages.schedule import DefaultScheduleHandler as ScheduleHandler
from app.clue_handler.state import ClueState
from app.observability.events_bus import VoiceAgentCallRequestEvent
from app.schemas.telephony import SubmitResult


class TestVoiceAgentAndTelephony(unittest.IsolatedAsyncioTestCase):

    async def test_schedule_handler_send_call_landline(self):
        """测试正常线索排期到期触发，默认使用普通座机线路 (LineType.LANDLINE)"""
        mock_send_call = AsyncMock(return_value=SubmitResult("accepted", "mock_ok"))
        handler = ScheduleHandler(send_call_fn=mock_send_call)

        request = ScheduledCallRequest(
            clueId="clue_test_landline_001",
            taskId=101,
            executionToken="token_landline_101",
        )

        with patch("app.dbmodels.crud.get_clue_state", new_callable=AsyncMock) as mock_state, \
             patch("app.dbmodels.crud.get_clue_info", new_callable=AsyncMock) as mock_info, \
             patch("app.dbmodels.crud.update_clue_state_or_raise", new_callable=AsyncMock) as mock_update_state:
            mock_state.return_value = "scheduled"
            clue_mock = MagicMock()
            clue_mock.agentData = {"next_call_use_work_phone": False}
            mock_info.return_value = clue_mock

            success = await handler.handle(request)

            self.assertTrue(success)
            mock_update_state.assert_called_once_with(request.clueId, ClueState.WAIT_FOR_CALLING.value)
            mock_send_call.assert_called_once_with(request, line_type=LineType.LANDLINE)

    async def test_schedule_handler_send_call_mobile_workphone(self):
        """测试包含工作手机标记的线索到期触发，智能选择工作手机线路 (LineType.MOBILE)"""
        mock_send_call = AsyncMock(return_value=SubmitResult("accepted", "mock_mobile_ok"))
        handler = ScheduleHandler(send_call_fn=mock_send_call)

        request = ScheduledCallRequest(
            clueId="clue_test_mobile_002",
            taskId=102,
            executionToken="token_mobile_102",
        )

        with patch("app.dbmodels.crud.get_clue_state", new_callable=AsyncMock) as mock_state, \
             patch("app.dbmodels.crud.get_clue_info", new_callable=AsyncMock) as mock_info, \
             patch("app.dbmodels.crud.update_clue_state_or_raise", new_callable=AsyncMock) as mock_update_state:
            mock_state.return_value = "scheduled"
            clue_mock = MagicMock()
            clue_mock.agentData = {"next_call_use_work_phone": True}
            mock_info.return_value = clue_mock

            success = await handler.handle(request)

            self.assertTrue(success)
            mock_update_state.assert_called_once_with(request.clueId, ClueState.WAIT_FOR_CALLING.value)
            mock_send_call.assert_called_once_with(request, line_type=LineType.MOBILE)

    async def test_schedule_handler_terminal_skips_call(self):
        """测试终态线索（如 accepted）自动拦截，不调用 send_call"""
        mock_send_call = AsyncMock()
        handler = ScheduleHandler(send_call_fn=mock_send_call)

        request = ScheduledCallRequest(
            clueId="clue_test_terminal_003",
            taskId=103,
            executionToken="token_terminal_103",
        )

        with patch("app.dbmodels.crud.get_clue_state", new_callable=AsyncMock) as mock_state:
            mock_state.return_value = "accepted"

            success = await handler.handle(request)

            self.assertTrue(success)
            mock_send_call.assert_not_called()

    async def test_send_call_emits_voice_agent_call_request_event(self):
        """测试 send_call 送号流程中完整发出 VoiceAgentCallRequestEvent 内部事件，并携带全部送号参数"""
        mock_record = MagicMock()
        mock_record.clue_id = "clue_event_test_001"
        mock_record.call_id = "call_event_test_001"
        mock_record.agent_id = "agent_test_999"

        # 模拟话务快照数据
        mock_tele_payload = {
            "accessorId": "acc_001",
            "strategyId": "line_landline_001",
            "speechcraftId": "tts_craft_001",
            "tradeNo": "trade_no_123456",
            "batchId": "batch_id_123456",
            "timestamp": "20261021175000000",
            "execStartTime": "2026-10-21 17:50:00",
            "execEndTime": "2026-10-21 21:00:00",
            "execLimit": 40,
            "phoneList": [
                {
                    "calledPhone": "13812345678",
                    "businessId": "clue_event_test_001",
                    "callParams": json.dumps({
                        "clueId": "clue_event_test_001",
                        "callId": "call_event_test_001",
                        "agentId": "agent_test_999",
                        "agentData": {"customerName": "王五"},
                    }),
                }
            ],
        }

        mock_record.config_snapshot = {
            "telephonyRequest": json.dumps(mock_tele_payload),
            "telephonyMetadata": {
                "batchId": "batch_id_123456",
                "tradeNo": "trade_no_123456",
                "agentVersion": "v2.1.0",
                "lineType": "landline",
            },
            "phoneNumber": "13812345678",
        }

        mock_scheduler = MagicMock()
        mock_scheduler.prepare = AsyncMock(return_value=mock_record)
        mock_scheduler.finish = AsyncMock()

        mock_gateway = AsyncMock(
            return_value=SubmitResult(
                status="accepted",
                code="already_exists",
                provider_call_id="prov_call_8888",
            )
        )

        request = ScheduledCallRequest(
            clueId="clue_event_test_001",
            taskId=201,
            executionToken="token_event_201",
        )

        with patch("app.utils.decrypt.crypted_manager.decrypt_field", side_effect=lambda x: x), \
             patch("app.observability.events_bus.event_bus.emit", new_callable=AsyncMock) as mock_emit:

            result = await send_call(
                request,
                line_type=LineType.LANDLINE,
                scheduler=mock_scheduler,
                gateway=mock_gateway,
            )

            self.assertIsNotNone(result)
            self.assertEqual(result.status, "accepted")
            self.assertEqual(result.provider_call_id, "prov_call_8888")

            # 验证 VoiceAgentCallRequestEvent 是否被发出
            mock_emit.assert_called_once()
            emitted_event = mock_emit.call_args[0][0]
            self.assertIsInstance(emitted_event, VoiceAgentCallRequestEvent)
            self.assertEqual(emitted_event.clue_id, "clue_event_test_001")
            self.assertEqual(emitted_event.call_id, "call_event_test_001")

            data = emitted_event.event_data
            self.assertEqual(data["task_id"], 201)
            self.assertEqual(data["clue_id"], "clue_event_test_001")
            self.assertEqual(data["call_id"], "call_event_test_001")
            self.assertEqual(data["agent_id"], "agent_test_999")
            self.assertEqual(data["agent_version"], "v2.1.0")
            self.assertEqual(data["line_type"], "landline")
            self.assertEqual(data["accessor_id"], "acc_001")
            self.assertEqual(data["strategy_id"], "line_landline_001")
            self.assertEqual(data["speechcraft_id"], "tts_craft_001")
            self.assertEqual(data["batch_id"], "batch_id_123456")
            self.assertEqual(data["trade_no"], "trade_no_123456")
            self.assertEqual(data["exec_start_time"], "2026-10-21 17:50:00")
            self.assertEqual(data["exec_end_time"], "2026-10-21 21:00:00")
            self.assertEqual(data["exec_limit"], 40)
            self.assertEqual(data["called_phone_masked"], "138****5678")
            self.assertEqual(data["call_params"]["clueId"], "clue_event_test_001")
            self.assertEqual(data["call_params"]["agentData"]["customerName"], "王五")
            self.assertEqual(data["submit_status"], "accepted")
            self.assertEqual(data["submit_code"], "already_exists")
            self.assertEqual(data["provider_call_id"], "prov_call_8888")


if __name__ == "__main__":
    unittest.main()
