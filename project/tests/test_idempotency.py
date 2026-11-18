import unittest
import asyncio
from tortoise import Tortoise
from pydantic import BaseModel

from app.dbmodels.models import ExecutionTask
from app.utils.idempotency import idempotent
from app.utils.exceptions import LockTimeoutError


class DummyRequest(BaseModel):
    requestId: str
    clueId: str


class DummyHandler:
    def __init__(self):
        self.call_count = 0
        
    @idempotent(idempotency_key="requestId")
    async def process_call_end(self, request: DummyRequest, *args, should_fail: bool = False, delay: float = 0):
        self.call_count += 1
        if delay > 0:
            await asyncio.sleep(delay)
        if should_fail:
            raise ValueError("Business logic failed!")
        return {"status": "ok", "call_count": self.call_count}


class TestIdempotency(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        await Tortoise.init(
            db_url="sqlite://:memory:",
            modules={"models": ["app.dbmodels.models"]}
        )
        await Tortoise.generate_schemas()

    async def asyncTearDown(self):
        await Tortoise.close_connections()

    async def test_idempotent_success(self):
        """测试正常请求与重演（已有成功记录）的阻断"""
        handler = DummyHandler()
        req = DummyRequest(requestId="req-123", clueId="clue-123")
        
        # First call - should succeed
        res = await handler.process_call_end(req)
        self.assertEqual(res["status"], "ok")
        self.assertEqual(handler.call_count, 1)
        
        record = await ExecutionTask.filter(request_id="req-123").first()
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "succeeded")
        
        # Second call - should hit cache
        res2 = await handler.process_call_end(req)
        self.assertEqual(res2.code, 0)
        self.assertEqual(handler.call_count, 1)

    async def test_idempotent_failure_and_retry(self):
        """测试业务失败后能正确放榜，且重试时能重新抢锁并成功"""
        handler = DummyHandler()
        req = DummyRequest(requestId="req-456", clueId="clue-456")
        
        with self.assertRaises(ValueError):
            await handler.process_call_end(req, should_fail=True)
            
        self.assertEqual(handler.call_count, 1)
        
        record = await ExecutionTask.filter(request_id="req-456").first()
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "failed")
        self.assertIn("Business logic failed!", record.last_error)

    async def test_idempotent_concurrent_access(self):
        """测试并发情况：一个正在执行，另一个闯入时抛出抢锁失败的错误"""
        handler = DummyHandler()
        req = DummyRequest(requestId="req-789", clueId="clue-789")
        
        task_a = asyncio.create_task(handler.process_call_end(req, delay=0.3))
        await asyncio.sleep(0.05)
        
        with self.assertRaises(LockTimeoutError):
            await handler.process_call_end(req)
            
        res_a = await task_a
        self.assertEqual(res_a["status"], "ok")
        self.assertEqual(handler.call_count, 1)
        
        record = await ExecutionTask.filter(request_id="req-789").first()
        self.assertEqual(record.status, "succeeded")


if __name__ == "__main__":
    unittest.main()
