import asyncio
import hashlib
import hmac
import json
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid
from fastapi import HTTPException

from app.auth.signature import (
    verify_inflow_signature,
    check_and_lock_request_id,
    DEV_APP_SECRETS,
)
from app.schemas.strategy_config import (
    StrategyCreateRequest,
    FollowupConfig,
    SipConfig,
    WebhookSubscription,
)
from app.schemas.voice_agent_config import (
    VoiceAgentCreateRequest,
    VoiceAgentGlobalSettings,
    SttConfig,
    TtsConfig,
    TurnTakingConfig,
    LlmConfig,
)
from app.schemas.workflow_config import (
    WorkflowCreateRequest,
    ControlPauseRequest,
    ControlResumeRequest,
)
from app.schemas.clue_create import (
    ClueCreateRequest,
    BatchClueCreateRequest,
    BatchClueRecord,
    NormalizedClue,
)
from app.apis.webhook_dispatcher import (
    dispatch_webhook_event,
    resolve_webhook_target,
    WEBHOOK_TIMEOUT_SECONDS,
    MAX_WEBHOOK_RETRIES,
)


def generate_signature(app_id: str, secret: str, timestamp: str, nonce: str, raw_body: str) -> str:
    string_to_sign = f"{app_id}.{timestamp}.{nonce}.{raw_body}"
    return hmac.new(
        secret.encode("utf-8"),
        string_to_sign.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


class DummyRequest:
    def __init__(self, headers: dict, body_bytes: bytes = b""):
        self.headers = headers
        self._body = body_bytes

    async def body(self):
        return self._body


class TestVoiceAgentRefactoredPlatform(unittest.IsolatedAsyncioTestCase):

    # ==================== 1. 签名与鉴权测试 ====================

    async def test_signature_verification_success(self):
        app_id = "app_test"
        secret = DEV_APP_SECRETS[app_id]
        now_ms = str(int(time.time() * 1000))
        nonce = str(uuid.uuid4())
        raw_body = json.dumps({"test": "hello"}, ensure_ascii=False)
        sig = generate_signature(app_id, secret, now_ms, nonce, raw_body)

        headers = {
            "X-App-Id": app_id,
            "X-Timestamp": now_ms,
            "X-Nonce": nonce,
            "X-Signature": sig,
        }
        req = DummyRequest(headers)

        with patch("app.auth.signature.redis_manager._get_client") as mock_get_client, \
             patch("app.auth.signature.get_app_auth", return_value=None):
            mock_redis = AsyncMock()
            mock_redis.set.return_value = True
            mock_get_client.return_value = mock_redis

            verified_app = await verify_inflow_signature(req, raw_body=raw_body, workflow_id="car_loan")
            self.assertEqual(verified_app, app_id)

    async def test_signature_verification_expired_timestamp(self):
        app_id = "app_test"
        secret = DEV_APP_SECRETS[app_id]
        expired_ms = str(int((time.time() - 400) * 1000))  # 超过5分钟漂移
        nonce = str(uuid.uuid4())
        raw_body = "{}"
        sig = generate_signature(app_id, secret, expired_ms, nonce, raw_body)

        req = DummyRequest({
            "X-App-Id": app_id,
            "X-Timestamp": expired_ms,
            "X-Nonce": nonce,
            "X-Signature": sig,
        })

        with self.assertRaises(HTTPException) as ctx:
            await verify_inflow_signature(req, raw_body=raw_body)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["code"], 40002)

    async def test_signature_verification_tampered_body(self):
        app_id = "app_test"
        secret = DEV_APP_SECRETS[app_id]
        now_ms = str(int(time.time() * 1000))
        nonce = str(uuid.uuid4())
        raw_body = "original"
        tampered_body = "tampered"
        sig = generate_signature(app_id, secret, now_ms, nonce, raw_body)

        req = DummyRequest({
            "X-App-Id": app_id,
            "X-Timestamp": now_ms,
            "X-Nonce": nonce,
            "X-Signature": sig,
        })

        with patch("app.auth.signature.redis_manager._get_client") as mock_get_client, \
             patch("app.auth.signature.get_app_auth", return_value=None):
            mock_redis = AsyncMock()
            mock_redis.set.return_value = True
            mock_get_client.return_value = mock_redis

            with self.assertRaises(HTTPException) as ctx:
                await verify_inflow_signature(req, raw_body=tampered_body)
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertEqual(ctx.exception.detail["code"], 40002)

    async def test_request_id_idempotency(self):
        with patch("app.auth.signature.redis_manager._get_client") as mock_get_client:
            mock_redis = AsyncMock()
            mock_redis.set.return_value = True
            mock_get_client.return_value = mock_redis
            self.assertTrue(await check_and_lock_request_id("req_1001"))

            # 第二次重复请求，被分布式锁拦截
            mock_redis.set.return_value = False
            with self.assertRaises(HTTPException) as ctx:
                await check_and_lock_request_id("req_1001")
            self.assertEqual(ctx.exception.detail["code"], 40005)

    # ==================== 2. Workflow 未配置返回 code: 40004 及 Overrides 覆写测试 ====================

    async def test_clue_inflow_workflow_missing_returns_code_40004(self):
        from app.routers.clue_inflow import create_single_clue

        payload = {
            "clue_id": "clue_unconfigured",
            "request_id": "req_unconf_01",
            "workflow_id": "unknown_workflow_123",
            "phone_number": "13800138000",
        }
        raw_body = json.dumps(payload)
        req = DummyRequest(headers={}, body_bytes=raw_body.encode("utf-8"))

        with patch("app.routers.clue_inflow.get_workflow_config") as mock_get_wf:
            mock_get_wf.return_value = None  # 模拟未查询到 Workflow 配置

            resp = await create_single_clue(req)
            self.assertEqual(resp["code"], 40004)
            self.assertIn("未查询到 workflow_id 'unknown_workflow_123' 对应的业务线配置", resp["msg"])

    async def test_clue_inflow_workflow_found_and_bind_config(self):
        from app.routers.clue_inflow import create_single_clue

        payload = {
            "clue_id": "clue_bound",
            "request_id": "req_bound_01",
            "workflow_id": "car_loan",
            "phone_number": "13800138000",
            "agent_data": {
                "variables": {"name": "张三", "city": "北京"}
            }
        }
        raw_body = json.dumps(payload)
        req = DummyRequest(headers={}, body_bytes=raw_body.encode("utf-8"))

        # 模拟已配置的 Workflow
        mock_wf = MagicMock()
        mock_wf.agent_id = "agent_car_loan_v1"
        mock_wf.strategy_id = "strategy_car_loan_v1"
        mock_wf.is_active = True

        with patch("app.routers.clue_inflow.get_workflow_config", return_value=mock_wf), \
             patch("app.routers.clue_inflow.get_voice_agent_config_by_id", return_value=MagicMock()), \
             patch("app.routers.clue_inflow.get_strategy_config", return_value=MagicMock()), \
             patch("app.routers.clue_inflow.verify_inflow_signature", return_value="app_test"), \
             patch("app.routers.clue_inflow.check_and_lock_request_id", return_value=True), \
             patch("app.routers.clue_inflow.ClueHandlerFactory.get_handler_by_workflow", new_callable=AsyncMock) as mock_get_handler:

            mock_handler = MagicMock()
            mock_handler.on_clue_pushed = AsyncMock()
            mock_get_handler.return_value = mock_handler

            resp = await create_single_clue(req)
            self.assertEqual(resp["code"], 0)
            self.assertEqual(resp["data"]["status"], "accepted")

            # 校验推入 handler 的 NormalizedClue 中直接绑定了 Workflow 的 agentId 与 strategyId
            pushed_clue: NormalizedClue = mock_handler.on_clue_pushed.call_args[0][0]
            self.assertEqual(pushed_clue.agentId, "agent_car_loan_v1")
            self.assertEqual(pushed_clue.strategyId, "strategy_car_loan_v1")

    async def test_clue_inflow_workflow_missing_paired_agent_returns_code_40004(self):
        """测试单条线索流入时虽有 workflow_config 但底表未配套 agent，拦截并返回 code: 40004"""
        from app.routers.clue_inflow import create_single_clue

        payload = {
            "clue_id": "clue_unpaired",
            "request_id": "req_unpaired_01",
            "workflow_id": "unpaired_wf",
            "phone_number": "13800138000",
        }
        raw_body = json.dumps(payload)
        req = DummyRequest(headers={}, body_bytes=raw_body.encode("utf-8"))

        mock_wf = MagicMock()
        mock_wf.agent_id = "missing_agent"
        mock_wf.strategy_id = "missing_strat"
        mock_wf.is_active = True

        with patch("app.routers.clue_inflow.get_workflow_config", return_value=mock_wf), \
             patch("app.routers.clue_inflow.get_voice_agent_config_by_id", return_value=None), \
             patch("app.routers.clue_inflow.get_voice_agent_config", return_value=None), \
             patch("app.routers.clue_inflow.get_strategy_config", return_value=MagicMock()):

            resp = await create_single_clue(req)
            self.assertEqual(resp["code"], 40004)
            self.assertEqual(resp["msg"], "此workflow_id业务线没有对应的agent可以配套")

    # ==================== 3. Retail AI Webhook 规范单测 ====================

    def test_webhook_constants_align_with_retail_ai_spec(self):
        # 严格对齐需求规范：10 秒超时，最多 3 次重试
        self.assertEqual(WEBHOOK_TIMEOUT_SECONDS, 10.0)
        self.assertEqual(MAX_WEBHOOK_RETRIES, 3)

    async def test_webhook_non_blocking_execution(self):
        with patch("app.apis.webhook_dispatcher.resolve_webhook_target") as mock_target, \
             patch("app.apis.webhook_dispatcher._send_http_with_retry", new_callable=AsyncMock) as mock_send, \
             patch("app.observability.events_bus.event_bus.emit"):

            mock_target.return_value = ("https://api.example.com/webhook", ["call_start"])

            # 默认 blocking=False 立即返回 True，并在后台发射 task
            res = await dispatch_webhook_event(
                event="call_started",
                clue_id="clue_non_block",
                data={"start_timestamp": 1714608475945},
                blocking=False
            )
            self.assertTrue(res)
            # 等待协程切换调度
            await asyncio.sleep(0.01)
            self.assertTrue(mock_send.called)

    async def test_webhook_retry_and_timeout_behavior(self):
        """测试 10s 超时与重试达 3 次的行为"""
        from app.apis.webhook_dispatcher import _send_http_with_retry

        # 模拟 aiohttp 始终抛出 Timeout 异常
        with patch("aiohttp.ClientSession.post", side_effect=asyncio.TimeoutError("10s timeout")), \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:

            success = await _send_http_with_retry(
                callback_url="https://api.example.com/webhook",
                payload={"event": "call_started"},
                event="call_started",
                event_id="evt_123",
                clue_id="clue_test"
            )
            self.assertFalse(success)
            # 应当重试 3 次 (即初始 1 次 + 重试 3 次，共 4 次尝试，休眠 3 次)
            self.assertEqual(mock_sleep.call_count, 3)

    async def test_webhook_idempotency_key_header_present(self):
        """测试 Header 包含 X-Event-Id 供使用方实现幂等去重 (基于 do_post)"""
        from app.apis.webhook_dispatcher import _send_http_with_retry

        with patch("app.apis.webhook_dispatcher.do_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = (200, "OK", {})
            success = await _send_http_with_retry(
                callback_url="https://api.example.com/webhook",
                payload={"event": "call_ended"},
                event="call_ended",
                event_id="evt_unique_uuid_456",
                clue_id="clue_test"
            )
            self.assertTrue(success)
            # 检查 headers 中包含 X-Event-Id 与 X-Event-Type
            call_kwargs = mock_post.call_args.kwargs
            headers = call_kwargs.get("headers", {})
            self.assertEqual(headers.get("X-Event-Id"), "evt_unique_uuid_456")
            self.assertEqual(headers.get("X-Event-Type"), "call_ended")

    # ==================== 4. Voice Agent & Strategy & Pause 逻辑测试 ====================

    def test_voice_agent_schema_creation_and_validation(self):
        # 1. 正常传入 global_settings 并自动生成 agent_id
        agent = VoiceAgentCreateRequest(
            agent_name="AgentDemo",
            global_settings=VoiceAgentGlobalSettings(
                instruction="You are a helpful assistant",
                tts=TtsConfig(voice="female"),
            )
        )
        self.assertTrue(agent.agent_id.startswith("agent_"))
        self.assertEqual(agent.instruction, "You are a helpful assistant")
        self.assertEqual(agent.tts.voice, "female")

        # 2. instruction 为空时应抛出校验错误
        with self.assertRaises(ValueError):
            VoiceAgentCreateRequest(
                agent_name="AgentEmpty",
                instruction=""
            )

    def test_strategy_schema_creation_with_schedule_id(self):
        strategy = StrategyCreateRequest(
            strategy_id="strategy-test-01",
            schedule_id="agent_driven",
            followup=FollowupConfig(
                summary_type="single_call", summary_prompt="recommend-common-interest",
                max_followup_hours=72,
            )
        )
        self.assertEqual(strategy.strategy_id, "strategy-test-01")
        self.assertEqual(strategy.schedule_id, "agent_driven")

    async def test_system_control_pause_and_resume(self):
        from app.routers.config_admin import control_pause, control_resume
        from app.utils.workers import is_inflow_paused, is_schedule_paused

        with patch("app.routers.config_admin.redis_manager._get_client") as mock_get_client, \
             patch("app.utils.workers.redis_manager._get_client") as mock_worker_redis, \
             patch("app.routers.config_admin.redis_manager.set_json", new_callable=AsyncMock) as mock_set, \
             patch("app.routers.config_admin.redis_manager.delete", new_callable=AsyncMock) as mock_del, \
             patch("app.utils.workers.redis_manager.exists", new_callable=AsyncMock) as mock_exists:

            # 暂停流入
            await control_pause(ControlPauseRequest(scope="inflow", reason="运维维护"))
            mock_set.assert_called_with("system:pause:inflow", {"paused": True, "reason": "运维维护"})

            # 恢复流入
            await control_resume(ControlResumeRequest(scope="inflow"))
            mock_del.assert_called_with("system:pause:inflow")

    async def test_batch_clue_inflow_workflow_missing_returns_code_40004(self):
        from app.routers.clue_inflow import create_batch_clues

        payload = {
            "request_id": "req_batch_unconf_01",
            "workflow_id": "nonexistent_wf",
            "records": [
                {"clue_id": "clue_b1", "phone_number": "13800000001"}
            ]
        }
        raw_body = json.dumps(payload)
        req = DummyRequest(headers={}, body_bytes=raw_body.encode("utf-8"))

        with patch("app.routers.clue_inflow.get_workflow_config", return_value=None):
            resp = await create_batch_clues(req)
            self.assertEqual(resp["code"], 40004)
            self.assertIn("未查询到 workflow_id 'nonexistent_wf' 对应的业务线配置", resp["msg"])

    async def test_file_clue_inflow_workflow_missing_returns_code_40004(self):
        from app.routers.clue_inflow import create_file_clues

        req = DummyRequest(headers={})
        with patch("app.routers.clue_inflow.get_workflow_config", return_value=None):
            resp = await create_file_clues(
                request=req,
                request_id="req_file_01",
                workflow_id="missing_file_wf",
                file_type="jsonl",
                file=None,
                file_url="http://dummy.url"
            )
            self.assertEqual(resp["code"], 40004)
            self.assertIn("未查询到 workflow_id 'missing_file_wf' 对应的业务线配置", resp["msg"])

    async def test_resolve_webhook_target_from_strategy(self):
        """测试 Webhook 地址解析: 根据线索关联的 strategy_id 获取 OutboundStrategy.webhook"""
        from app.apis.webhook_dispatcher import resolve_webhook_target
        from app.dbmodels.models import ClueInfo, OutboundStrategy

        mock_clue = MagicMock()
        mock_clue.strategy_id = "strat_loan_01"
        mock_clue.workflow_id = "car_loan"

        mock_strategy = MagicMock()
        mock_strategy.webhook = {
            "url": "https://strategy.webhook.com/event",
            "events": ["call_started", "call_ended"]
        }

        with patch.object(ClueInfo, "filter") as mock_clue_filter, \
             patch.object(OutboundStrategy, "filter") as mock_strat_filter:

            mock_clue_qs = MagicMock()
            mock_clue_qs.first = AsyncMock(return_value=mock_clue)
            mock_clue_filter.return_value = mock_clue_qs

            mock_strat_qs = MagicMock()
            mock_strat_qs.first = AsyncMock(return_value=mock_strategy)
            mock_strat_filter.return_value = mock_strat_qs

            url, events = await resolve_webhook_target("clue_01")
            self.assertEqual(url, "https://strategy.webhook.com/event")
            self.assertIn("call_started", events)


if __name__ == "__main__":
    unittest.main()

