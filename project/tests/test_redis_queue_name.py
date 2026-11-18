import asyncio
import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock

from app.clue_handler.clue import ClueHandler
from app.schemas.clue_create import NormalizedClue
from app.config import Config, config
from app.redis_client.redis_manager import redis_manager
from app.utils.exceptions import LockTimeoutError
from app.utils.workers import clue_consumer_loop


class TestRedisQueueNameIsolation(unittest.IsolatedAsyncioTestCase):
    """验证 Redis 队列名称全链路隔离，确保全部切换为 voice_agent_hub"""

    def test_config_files_redis_queue_name(self):
        """测试 config.example.json 中的 redis_queue_name 配置为 voice_agent_hub"""
        configs_dir = Path(__file__).parent.parent / "app" / "configs"
        example_json_path = configs_dir / "config.example.json"
        with open(example_json_path, "r", encoding="utf-8") as f:
            example_cfg = json.load(f)
        self.assertEqual(
            example_cfg.get("redis_queue_name"),
            "voice_agent_hub",
            "config.example.json 中的 redis_queue_name 必须为 voice_agent_hub",
        )

    def test_current_runtime_config_queue_name(self):
        """测试当前运行时的 config.redis_queue_name 为 voice_agent_hub"""
        self.assertEqual(config.redis_queue_name, "voice_agent_hub")

    def test_config_fallback_defaults(self):
        """测试当无配置文件且未提供队列环境变量时，Config 缺省 fallback 为 voice_agent_hub"""
        with patch.object(Config, "_load_json_dict", return_value={}):
            c = Config.from_config()
            self.assertEqual(c.redis_queue_name, "voice_agent_hub")
            self.assertEqual(c.event_queue_name, "voice_agent_hub_events")

    def test_delayed_queue_default_key(self):
        """测试延迟队列的缺省 key 隔离为 voice_agent_hub:delayed_tasks"""
        dq = redis_manager.get_delayed_queue()
        self.assertEqual(dq.queue_key, "voice_agent_hub:delayed_tasks")

    @patch("app.clue_handler.clue.create_clue")
    @patch("app.clue_handler.clue.redis_manager.rpush_list", new_callable=AsyncMock)
    async def test_clue_pushed_to_voice_agent_hub_queue(self, mock_rpush, mock_create_clue):
        """测试线索推送逻辑严格推入 voice_agent_hub 队列"""
        handler = ClueHandler(
            new_clue_handler=MagicMock(),
            postcall_handler=MagicMock(),
            schedule_handler=MagicMock(),
            cancel_call_handler=MagicMock(),
        )
        mock_clue = NormalizedClue.model_construct(clueId="clue_hub_test_01")

        await handler.on_clue_pushed(mock_clue)
        mock_rpush.assert_called_once()
        # 验证推送的队列名为 voice_agent_hub
        target_queue = mock_rpush.call_args[0][0]
        self.assertEqual(target_queue, "voice_agent_hub")
        self.assertNotEqual(target_queue, "wechat-agent")

    @patch("app.utils.workers.is_inflow_paused", new_callable=AsyncMock, return_value=False)
    @patch("app.utils.workers.redis_manager.queue_pop", new_callable=AsyncMock)
    @patch("app.utils.workers.redis_manager.rpush_list", new_callable=AsyncMock)
    @patch("app.utils.workers.ClueHandlerFactory.get_handler_by_workflow")
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_clue_consumer_pop_and_retry_from_voice_agent_hub(
        self, mock_sleep, mock_factory, mock_rpush, mock_queue_pop, mock_paused
    ):
        """测试 workers.clue_consumer_loop 消费并重试推回 voice_agent_hub 队列"""
        mock_queue_pop.return_value = {
            "clueId": "clue_worker_test_01",
            "phoneNumber": "13800000000",
            "workflowId": "default",
        }
        mock_handler = MagicMock()
        mock_handler.on_clue_queue = AsyncMock(side_effect=LockTimeoutError("Lock timeout"))
        mock_factory.return_value = mock_handler

        # 让第一次 sleep (推回队列后的 sleep(1)) 立即抛出 CancelledError 中止循环
        mock_sleep.side_effect = asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await clue_consumer_loop()

        # 验证消费时读取的是 voice_agent_hub 队列
        mock_queue_pop.assert_called_with("voice_agent_hub")

        # 验证加锁超时重试时推回的是 voice_agent_hub 队列
        mock_rpush.assert_called_once()
        retry_queue = mock_rpush.call_args[0][0]
        self.assertEqual(retry_queue, "voice_agent_hub")
        self.assertNotEqual(retry_queue, "wechat-agent")
