import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from app.config import config
from app.observability.events_bus import (
    EventBus,
    BaseEvent,
    AgentEvent,
    CallScheduleEvent,
    CallStartEvent,
    CallEndEvent,
    ActionWechatEvent,
    ActionSmsEvent,
    ActionEmailEvent,
    ClueEndEvent,
    ClueIntakeEvent,
    StateTransferEvent,
    VoiceAgentCallRequestEvent,
    VoiceAgentCancelSuccessEvent,
    ClueTimeoutWarningEvent,
    WebhookDeliveryAuditEvent,
    STANDARD_WEBHOOK_EVENT_TYPES,
    STANDARD_WEBHOOK_EVENT_CLASSES,
)
from app.apis.webhook_dispatcher import resolve_webhook_target, dispatch_webhook_event


class TestWebhookTargetResolution(unittest.IsolatedAsyncioTestCase):
    """测试 Webhook 回调地址的严格两级解析机制（无全局兜底）"""

    async def test_priority_1_clue_override_takes_precedence(self):
        """第一优先级（线索级动态覆盖）：overrides.callback_url 优先于策略表"""
        mock_clue = MagicMock()
        mock_clue.overrides = {"callback_url": "https://override.example.com/webhook"}
        mock_clue.strategy_id = "strat_001"
        mock_clue.workflow_id = None

        mock_strategy = MagicMock()
        mock_strategy.webhook = {
            "url": "https://strategy.example.com/webhook",
            "events": ["call_start", "call_end"],
        }

        with patch("app.dbmodels.models.ClueInfo.filter") as mock_clue_filter, \
             patch("app.dbmodels.models.OutboundStrategy.filter") as mock_strat_filter:

            mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)
            mock_strat_filter.return_value.first = AsyncMock(return_value=mock_strategy)

            url, events = await resolve_webhook_target("clue_priority_1")
            self.assertEqual(url, "https://override.example.com/webhook")
            self.assertEqual(events, ["call_start", "call_end"])

    async def test_priority_2_strategy_webhook_url_used_when_no_override(self):
        """第二优先级（策略配置基准）：线索无 overrides 时使用 outboundStrategy.webhook.url"""
        mock_clue = MagicMock()
        mock_clue.overrides = None
        mock_clue.strategy_id = "strat_002"
        mock_clue.workflow_id = None

        mock_strategy = MagicMock()
        mock_strategy.webhook = {
            "url": "https://strategy.example.com/webhook",
            "events": ["call_schedule", "call_end"],
        }

        with patch("app.dbmodels.models.ClueInfo.filter") as mock_clue_filter, \
             patch("app.dbmodels.models.OutboundStrategy.filter") as mock_strat_filter:

            mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)
            mock_strat_filter.return_value.first = AsyncMock(return_value=mock_strategy)

            url, events = await resolve_webhook_target("clue_priority_2")
            self.assertEqual(url, "https://strategy.example.com/webhook")
            self.assertEqual(events, ["call_schedule", "call_end"])

    async def test_no_webhook_configured_returns_none(self):
        """当 overrides 和 strategy 均未配置时，默认返回 None (不推送)"""
        mock_clue = MagicMock()
        mock_clue.overrides = {}
        mock_clue.strategy_id = "strat_empty"
        mock_clue.workflow_id = None

        mock_strategy = MagicMock()
        mock_strategy.webhook = None

        with patch("app.dbmodels.models.ClueInfo.filter") as mock_clue_filter, \
             patch("app.dbmodels.models.OutboundStrategy.filter") as mock_strat_filter:

            mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)
            mock_strat_filter.return_value.first = AsyncMock(return_value=mock_strategy)

            url, events = await resolve_webhook_target("clue_no_webhook")
            self.assertIsNone(url)
            self.assertIsNone(events)


class TestEventBusRoutingAndDispatch(unittest.IsolatedAsyncioTestCase):
    """测试 EventBus 的路由分发与多渠道消费机制"""

    def setUp(self):
        self.mock_redis = MagicMock()
        self.mock_redis.rpush_list = AsyncMock()
        self.mock_redis.queue_pop = AsyncMock()
        self.bus = EventBus(redis=self.mock_redis, queue_name="test_event_queue")

    async def test_all_7_standard_events_route_to_mysql_and_webhook(self):
        """验证 7 大标准事件均包含在 STANDARD_WEBHOOK_EVENT_CLASSES 中且映射到 ['mysql', 'webhook']"""
        expected_classes = {
            "CallScheduleEvent",
            "CallStartEvent",
            "CallEndEvent",
            "ActionWechatEvent",
            "ActionSmsEvent",
            "ActionEmailEvent",
            "ClueEndEvent",
        }
        self.assertEqual(STANDARD_WEBHOOK_EVENT_CLASSES, expected_classes)

        for event_name in expected_classes:
            self.assertIn(event_name, self.bus.routes)
            self.assertEqual(self.bus.routes[event_name], ["mysql", "webhook"])

    async def test_event_bus_safe_process_event_dual_dispatch(self):
        """测试消费标准事件时并发双写: 落库 MySQL + 推送 Webhook"""
        raw_event = json.dumps({
            "event_class": "CallEndEvent",
            "payload": {
                "clue_id": "clue_dual_01",
                "call_id": "call_123",
                "event_type": "call_end",
                "event_data": {"talk_duration": 65},
            }
        })

        with patch.object(self.bus, "_dispatch_to_mysql", new_callable=AsyncMock) as mock_mysql, \
             patch.object(self.bus, "_dispatch_to_webhook", new_callable=AsyncMock) as mock_webhook:

            await self.bus._safe_process_event(raw_event)

            mock_mysql.assert_awaited_once()
            mock_webhook.assert_awaited_once()

            # 验证 payload 参数正确传入
            self.assertEqual(mock_mysql.call_args[0][0]["clue_id"], "clue_dual_01")
            self.assertEqual(mock_webhook.call_args[0][0]["call_id"], "call_123")
            self.assertEqual(mock_webhook.call_args[0][1], "call_end")

    async def test_event_bus_safe_process_event_internal_event_only_mysql(self):
        """测试内部审计/状态事件仅分发至 MySQL，不触发 Webhook"""
        raw_event = json.dumps({
            "event_class": "StateTransferEvent",
            "payload": {
                "clue_id": "clue_audit_01",
                "event_type": "state_transfer",
                "event_data": {"from_state": "pending", "to_state": "calling"},
            }
        })

        with patch.object(self.bus, "_dispatch_to_mysql", new_callable=AsyncMock) as mock_mysql, \
             patch.object(self.bus, "_dispatch_to_webhook", new_callable=AsyncMock) as mock_webhook:

            await self.bus._safe_process_event(raw_event)

            mock_mysql.assert_awaited_once()
            mock_webhook.assert_not_called()

    async def test_event_bus_safe_process_event_unmapped_event_defaults_to_mysql(self):
        """测试未显式注册的业务事件默认兜底落库 MySQL，且不触发 Webhook"""
        raw_event = json.dumps({
            "event_class": "CustomInternalEvent",
            "payload": {
                "clue_id": "clue_custom_01",
                "event_type": "custom_audit",
                "event_data": {"foo": "bar"},
            }
        })

        with patch.object(self.bus, "_dispatch_to_mysql", new_callable=AsyncMock) as mock_mysql, \
             patch.object(self.bus, "_dispatch_to_webhook", new_callable=AsyncMock) as mock_webhook:

            await self.bus._safe_process_event(raw_event)

            mock_mysql.assert_awaited_once()
            mock_webhook.assert_not_called()

    async def test_dispatch_to_webhook_subscription_filter(self):
        """测试 _dispatch_to_webhook 对事件订阅白名单的过滤控制"""
        payload = {
            "clue_id": "clue_sub_test",
            "call_id": "call_sub_01",
            "event_data": {"status": "ok"},
        }

        with patch("app.apis.webhook_dispatcher.resolve_webhook_target") as mock_target, \
             patch("app.apis.webhook_dispatcher._send_http_with_retry", new_callable=AsyncMock) as mock_send:

            # 策略只订阅了 call_start 和 call_end
            mock_target.return_value = (
                "https://api.example.com/webhook",
                ["call_start", "call_end"]
            )

            # 1. 发送未订阅的 action_email -> 应该拦截跳过
            await self.bus._dispatch_to_webhook(payload, "action_email")
            mock_send.assert_not_called()

            # 2. 发送已订阅的 call_start -> 应当推送
            await self.bus._dispatch_to_webhook(payload, "call_start")
            self.assertEqual(mock_send.call_count, 1)
            send_args = mock_send.call_args[1]
            self.assertEqual(send_args["callback_url"], "https://api.example.com/webhook")
            self.assertEqual(send_args["event"], "call_start")
            self.assertEqual(send_args["payload"]["data"], {"status": "ok"})

            # 3. 如果订阅列表为空或未配置，坚决不推送
            mock_send.reset_mock()
            mock_target.return_value = ("https://api.example.com/webhook", None)
            await self.bus._dispatch_to_webhook(payload, "call_start")
            mock_send.assert_not_called()

            mock_target.return_value = ("https://api.example.com/webhook", [])
            await self.bus._dispatch_to_webhook(payload, "call_start")
            mock_send.assert_not_called()

    async def test_dispatch_to_webhook_no_url_skips(self):
        """测试 _dispatch_to_webhook 在目标地址为空时静默跳过"""
        payload = {"clue_id": "clue_no_url"}

        with patch("app.apis.webhook_dispatcher.resolve_webhook_target") as mock_target, \
             patch("app.apis.webhook_dispatcher._send_http_with_retry", new_callable=AsyncMock) as mock_send:

            mock_target.return_value = (None, None)
            await self.bus._dispatch_to_webhook(payload, "call_start")
            mock_send.assert_not_called()

    async def test_emit_pushes_envelope_to_redis(self):
        """测试 emit 方法将事件信封序列化后推入 Redis 队列"""
        event = CallStartEvent(
            clue_id="clue_emit_01",
            call_id="call_999",
            event_data={"ring_timestamp": 1714608475000}
        )

        await self.bus.emit(event)

        self.mock_redis.rpush_list.assert_awaited_once()
        call_args = self.mock_redis.rpush_list.call_args[0]
        self.assertEqual(call_args[0], "test_event_queue")

        envelope = json.loads(call_args[1])
        self.assertEqual(envelope["event_class"], "CallStartEvent")
        self.assertEqual(envelope["payload"]["clue_id"], "clue_emit_01")
        self.assertEqual(envelope["payload"]["call_id"], "call_999")
        self.assertEqual(envelope["payload"]["event_type"], "call_start")

    async def test_dispatch_to_mysql_task_type_and_workflow_id(self):
        """测试 MySQL 落库严格区分 task_type ('webhook' vs 'internal_event') 并透传 workflow_id"""
        with patch("app.dbmodels.crud.save_clue_event", new_callable=AsyncMock) as mock_save:
            # 1. Webhook 事件 -> taskType 必须为 "webhook"
            webhook_payload = {
                "clue_id": "clue_wh_01",
                "workflow_id": "wf_wh_01",
                "event_type": "call_start",
                "event_id": "ev_001",
                "event_data": {"caller": "1001"},
            }
            await self.bus._dispatch_to_mysql(webhook_payload, "CallStartEvent")
            mock_save.assert_awaited_once()
            call_kwargs = mock_save.call_args[1]
            self.assertEqual(call_kwargs["clueId"], "clue_wh_01")
            self.assertEqual(call_kwargs["taskType"], "webhook")
            self.assertEqual(call_kwargs["workflowId"], "wf_wh_01")
            self.assertEqual(call_kwargs["eventId"], "ev_001")

            # 2. 内部流转事件 -> taskType 必须为 "internal_event"
            mock_save.reset_mock()
            internal_payload = {
                "clue_id": "clue_int_01",
                "workflow_id": "wf_int_01",
                "event_type": "clue_intake",
                "event_id": "ev_002",
                "event_data": {"source": "ad"},
            }
            await self.bus._dispatch_to_mysql(internal_payload, "ClueIntakeEvent")
            mock_save.assert_awaited_once()
            call_kwargs = mock_save.call_args[1]
            self.assertEqual(call_kwargs["clueId"], "clue_int_01")
            self.assertEqual(call_kwargs["taskType"], "internal_event")
            self.assertEqual(call_kwargs["workflowId"], "wf_int_01")
            self.assertEqual(call_kwargs["eventId"], "ev_002")

    async def test_dispatch_to_webhook_includes_workflow_id_and_updates_execution_task(self):
        """测试 Webhook 推送顶层 payload 包含 workflow_id 并在发送完成后更新 executionTask 状态"""
        payload = {
            "clue_id": "clue_wh_wf_01",
            "workflow_id": "wf_outbound_99",
            "call_id": "call_wh_01",
            "event_id": "ev_wh_123",
            "event_data": {"status": "started"},
        }

        with patch("app.apis.webhook_dispatcher.resolve_webhook_target") as mock_target, \
             patch("app.apis.webhook_dispatcher._send_http_with_retry", new_callable=AsyncMock) as mock_send, \
             patch("app.dbmodels.models.ExecutionTask.filter") as mock_task_filter:

            mock_target.return_value = ("https://api.example.com/webhook", ["call_start"])
            mock_send.return_value = True
            mock_query = MagicMock()
            mock_query.update = AsyncMock(return_value=1)
            mock_task_filter.return_value = mock_query

            await self.bus._dispatch_to_webhook(payload, "call_start")

            mock_send.assert_awaited_once()
            send_payload = mock_send.call_args[1]["payload"]
            self.assertEqual(send_payload["workflow_id"], "wf_outbound_99")
            self.assertEqual(send_payload["clue_id"], "clue_wh_wf_01")
            self.assertEqual(send_payload["event"], "call_start")

            # 验证 ExecutionTask 状态被更新为 succeeded
            mock_task_filter.assert_called_with(event_id="ev_wh_123", task_type="webhook")
            mock_query.update.assert_awaited_once()
            update_kwargs = mock_query.update.call_args[1]
            self.assertEqual(update_kwargs["status"], "succeeded")


class TestAgentEventSubclassing(unittest.TestCase):
    """测试 AgentEvent 抽象基类约束与具体子类继承"""

    def test_agent_event_cannot_be_instantiated_directly(self):
        """AgentEvent 是抽象基类，直接实例化必须抛出 TypeError"""
        with self.assertRaises(TypeError) as ctx:
            AgentEvent(clue_id="clue_base_01", event_type="test_event")
        self.assertIn("AgentEvent 是抽象事件基类，禁止直接实例化", str(ctx.exception))

    def test_all_concrete_subclasses_instantiable(self):
        """所有 Webhook 与内部事件子类均可正常实例化并包含核心字段"""
        subclasses = [
            # 7 大 Webhook 事件
            (CallScheduleEvent, "call_schedule"),
            (CallStartEvent, "call_start"),
            (CallEndEvent, "call_end"),
            (ActionWechatEvent, "action_wechat"),
            (ActionSmsEvent, "action_sms"),
            (ActionEmailEvent, "action_email"),
            (ClueEndEvent, "clue_end"),
            # 6 大内部事件
            (ClueIntakeEvent, "clue_intake"),
            (StateTransferEvent, "state_transfer"),
            (VoiceAgentCallRequestEvent, "voice_agent_call_request"),
            (VoiceAgentCancelSuccessEvent, "voice_agent_cancel_success"),
            (ClueTimeoutWarningEvent, "clue_timeout_warning"),
            (WebhookDeliveryAuditEvent, "webhook_delivery_audit"),
        ]

        for cls, expected_type in subclasses:
            ev = cls(clue_id="clue_sub_test", workflow_id="wf_sub_test", event_data={"key": "val"})
            self.assertEqual(ev.clue_id, "clue_sub_test")
            self.assertEqual(ev.workflow_id, "wf_sub_test")
            self.assertEqual(ev.event_type, expected_type)
            self.assertIsInstance(ev, AgentEvent)
            self.assertIsNot(ev.__class__, AgentEvent)

