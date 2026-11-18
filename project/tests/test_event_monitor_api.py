import unittest
from datetime import datetime
from unittest.mock import patch
import httpx
from tortoise import Tortoise

from app.clue_handler.state import ClueState
from app.dbmodels.models import CallRecord, ClueInfo, ExecutionTask
from app.main import app


class DummyLock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class TestEventMonitorApi(unittest.IsolatedAsyncioTestCase):
    """测试 event_monitor.html 静态页面与单接口聚合排障 API"""

    async def asyncSetUp(self):
        await Tortoise.init(
            db_url="sqlite://:memory:",
            modules={"models": ["app.dbmodels.models"]},
        )
        await Tortoise.generate_schemas()

        self.lock_patcher = patch(
            "app.redis_client.redis_manager.redis_manager.get_lock",
            return_value=DummyLock(),
        )
        self.lock_patcher.start()

    async def asyncTearDown(self):
        self.lock_patcher.stop()
        await Tortoise.close_connections()

    async def test_event_monitor_html_rendered(self):
        """测试 /event-monitor 页面正常响应，包含现代化大屏元素且不包含定时器轮询"""
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/event-monitor")
            self.assertEqual(response.status_code, 200)
            content = response.text

            # 验证包含现代化看板核心组件
            self.assertIn("Voice Agent Hub 全生命周期状态与事件溯源大屏", content)
            self.assertIn("状态机生命周期闭环推进", content)
            self.assertIn("线索基础画像与业务归属", content)
            self.assertIn("状态机跃迁时序轨迹", content)
            self.assertIn("通联与大模型质检记录", content)
            self.assertIn("全链路事件流转溯源时间线", content)

            # 核心保证：严格杜绝自动轮询定时器
            self.assertNotIn("setInterval", content)

    async def test_single_api_clue_state_aggregated_response(self):
        """测试单接口 GET /database/clue_state 一次性聚合返回状态、画像、通话与事件流"""
        clue_id = "clue_monitor_test_001"

        # 1. 准备 ClueInfo 数据
        await ClueInfo.create(
            clue_id=clue_id,
            status=ClueState.SCHEDULED.value,
            phone_number="13800000001",
            customer_name="张三测试",
            workflow_id="wf_monitor_001",
            strategy_id="strat_monitor_001",
            agent_id="agent_titan_01",
            total_call_count=1,
            close_reason=None,
            state_version=2,
            agent_data={"customerCity": "北京"},
        )

        # 2. 准备 CallRecord 数据
        await CallRecord.create(
            call_id="call_rec_001",
            clue_id=clue_id,
            agent_id="agent_titan_01",
            strategy_id="strat_monitor_001",
            attempt_no=1,
            status="ended",
            answered=1,
            duration_seconds=42,
            recording_url="http://recordings.example.com/audio001.mp3",
            decision="success",
            common_call_events={"call_summary": "用户有意向，沟通良好"},
        )

        # 3. 准备事件数据 (ExecutionTask)
        await ExecutionTask.create(
            task_key="task_key_001",
            task_type="internal_event",
            clue_id=clue_id,
            event_type="state_transfer",
            payload={"from": "created", "to": "scheduled", "reason": "首呼排期入池", "success": True},
            status="succeeded",
            run_at=datetime.now(),
        )
        await ExecutionTask.create(
            task_key="task_key_002",
            task_type="internal_event",
            clue_id=clue_id,
            event_type="voice_agent_call_request",
            payload={"line_type": "landline", "target": "13800000001"},
            status="succeeded",
            run_at=datetime.now(),
        )

        # 4. 执行单次查询 GET /api/observability/clue-trace?clueId=...
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(f"/api/observability/clue-trace?clueId={clue_id}")
            self.assertEqual(response.status_code, 200)
            data = response.json()

            # 验证顶层字段向后兼容
            self.assertEqual(data["clueId"], clue_id)
            self.assertEqual(data["state"], "scheduled")

            # 验证包含完整的画像 detail
            detail = data.get("detail")
            self.assertIsNotNone(detail)
            self.assertEqual(detail["status"], "scheduled")
            self.assertEqual(detail["state_version"], 2)
            self.assertEqual(detail["workflow_id"], "wf_monitor_001")
            self.assertEqual(detail["strategy_id"], "strat_monitor_001")
            self.assertEqual(detail["customer_name"], "张三测试")
            self.assertEqual(detail["customer_city"], "北京")
            self.assertEqual(detail["phone_number"], "13800000001")
            self.assertEqual(detail["total_call_count"], 1)

            # 验证包含通话记录 call_records
            call_records = detail.get("call_records")
            self.assertEqual(len(call_records), 1)
            self.assertEqual(call_records[0]["call_id"], "call_rec_001")
            self.assertEqual(call_records[0]["duration_seconds"], 42)
            self.assertEqual(call_records[0]["answered"], 1)
            self.assertEqual(call_records[0]["recording_url"], "http://recordings.example.com/audio001.mp3")
            self.assertEqual(call_records[0]["common_call_events"]["call_summary"], "用户有意向，沟通良好")

            # 验证包含事件流 events
            events = data.get("events")
            self.assertIsNotNone(events)
            self.assertEqual(len(events), 2)
            self.assertEqual(events[0]["event_type"], "state_transfer")
            self.assertEqual(events[0]["event_data"]["from"], "created")
            self.assertEqual(events[0]["event_data"]["to"], "scheduled")
            self.assertEqual(events[1]["event_type"], "voice_agent_call_request")

    async def test_clue_trace_alias_endpoint(self):
        """测试路由 GET /api/observability/clue-trace?clueId=... 支持单接口聚合查询"""
        clue_id = "clue_trace_alias_002"
        await ClueInfo.create(
            clue_id=clue_id,
            status=ClueState.CALLING.value,
            phone_number="13900000002",
            agent_id="default",
        )

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(f"/api/observability/clue-trace?clueId={clue_id}")
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["clueId"], clue_id)
            self.assertEqual(data["state"], "calling")
            self.assertEqual(data["detail"]["status"], "calling")

    async def test_clue_events_get_and_post_support(self):
        """测试 /api/observability/clue-events 支持 POST 与 GET 双通道查询"""
        clue_id = "clue_events_test_003"
        await ExecutionTask.create(
            task_key="ev_task_003",
            task_type="webhook",
            clue_id=clue_id,
            event_type="clue_ended",
            payload={"code": 200, "reason": "达标结案"},
            status="succeeded",
            run_at=datetime.now(),
        )

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            # POST 查询
            post_resp = await client.post("/api/observability/clue-events", json={"clueId": clue_id})
            self.assertEqual(post_resp.status_code, 200)
            post_data = post_resp.json()
            self.assertEqual(post_data["code"], 0)
            self.assertEqual(len(post_data["data"]), 1)
            self.assertEqual(post_data["data"][0]["event_type"], "clue_ended")

            # GET 查询
            get_resp = await client.get(f"/api/observability/clue-events?clueId={clue_id}")
            self.assertEqual(get_resp.status_code, 200)
            get_data = get_resp.json()
            self.assertEqual(get_data["code"], 0)
            self.assertEqual(len(get_data["data"]), 1)
            self.assertEqual(get_data["data"][0]["event_type"], "clue_ended")
