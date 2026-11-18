import pytest
import asyncio
from tortoise import Tortoise
from app.dbmodels.models import ClueInfo
from app.dbmodels.crud import update_clue_state_or_raise
from app.redis_client.redis_manager import redis_manager
from app.utils.exceptions import StateTransitionError, LockTimeoutError

import pytest_asyncio

# 标记这是一个异步测试模块
pytestmark = pytest.mark.asyncio

from unittest.mock import AsyncMock, patch

class InMemoryLock:
    _locks = {}

    def __init__(self, key, *args, **kwargs):
        self.key = key
        if key not in InMemoryLock._locks:
            InMemoryLock._locks[key] = asyncio.Lock()
        self._lock = InMemoryLock._locks[key]

    async def lock(self, retry_interval_ms: int = 10, timeout_ms: int = 5000):
        start_time = asyncio.get_event_loop().time() * 1000
        while (asyncio.get_event_loop().time() * 1000 - start_time) < timeout_ms:
            if await self.try_lock():
                return True
            await asyncio.sleep(retry_interval_ms / 1000.0)
        return False

    async def try_lock(self):
        if not self._lock.locked():
            await self._lock.acquire()
            return True
        return False

    async def unlock(self):
        if self._lock.locked():
            try:
                self._lock.release()
            except RuntimeError:
                pass

    async def __aenter__(self):
        acquired = await self.lock(retry_interval_ms=10, timeout_ms=5000)
        if not acquired:
            raise TimeoutError(f"获取分布式锁超时 [{self.key}]")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.unlock()

@pytest_asyncio.fixture(autouse=True)
async def setup_env():
    """
    测试环境初始化：
    1. 使用内存在内存中启动一个干净的 SQLite 数据库
    2. 使用 InMemoryLock 模拟 Redis 锁
    """
    InMemoryLock._locks.clear()
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["app.dbmodels.models"]}
    )
    await Tortoise.generate_schemas()

    async def mock_delete(key):
        for k, lk in list(InMemoryLock._locks.items()):
            if k in key or key in k:
                if lk.locked():
                    try:
                        lk.release()
                    except RuntimeError:
                        pass
                InMemoryLock._locks[k] = asyncio.Lock()
        return True

    with patch.object(redis_manager, "init_redis", AsyncMock()), \
         patch.object(redis_manager, "close", AsyncMock()), \
         patch.object(redis_manager, "delete", side_effect=mock_delete), \
         patch.object(redis_manager, "get_lock", side_effect=lambda k, *a, **kw: InMemoryLock(k)), \
         patch("app.dbmodels.crud.event_bus.emit", new_callable=AsyncMock):
        yield

    await Tortoise.close_connections()

async def test_concurrent_state_update_with_lock():
    """
    测试并发修改线索状态，验证分布式锁与状态机是否正常工作。
    """
    clue_id = "test_clue_concurrency_001"
    
    # 1. 预置数据：创建一条处于 'pending' (等待动作完成) 状态的线索
    await ClueInfo.create(clue_id=clue_id, status="pending")
    
    result_a, result_b = await asyncio.gather(
        update_clue_state_or_raise(clue_id, "accepted"),
        update_clue_state_or_raise(clue_id, "rejected"),
        return_exceptions=True
    )
    
    assert isinstance(result_a, Exception) != isinstance(result_b, Exception)
    
    final_clue = await ClueInfo.filter(clue_id=clue_id).first()
    if not isinstance(result_a, Exception):
        assert final_clue.status == "accepted"
    else:
        assert final_clue.status == "rejected"


async def test_concurrent_same_state_update():
    """测试同向并发场景（幂等性）"""
    clue_id = "test_clue_concurrency_002"
    await ClueInfo.create(clue_id=clue_id, status="pending")
    
    result_a, result_b = await asyncio.gather(
        update_clue_state_or_raise(clue_id, "accepted"),
        update_clue_state_or_raise(clue_id, "accepted"),
        return_exceptions=True
    )
    
    assert not isinstance(result_a, Exception) and not isinstance(result_b, Exception)
    final_clue = await ClueInfo.filter(clue_id=clue_id).first()
    assert final_clue.status == "accepted"


async def test_invalid_state_transition():
    """测试非法状态流转防御"""
    clue_id = "test_clue_concurrency_003"
    await ClueInfo.create(clue_id=clue_id, status="created")
    
    with pytest.raises(StateTransitionError):
        await update_clue_state_or_raise(clue_id, "accepted")
    
    final_clue = await ClueInfo.filter(clue_id=clue_id).first()
    assert final_clue.status == "created"


async def test_high_concurrency_stress():
    """测试极端高并发下的稳定性（防击穿与锁竞争）"""
    clue_id = "test_clue_concurrency_004"
    await ClueInfo.create(clue_id=clue_id, status="created")
    
    tasks = []
    for _ in range(50):
        tasks.append(update_clue_state_or_raise(clue_id, "calling", max_retries=10))
        tasks.append(update_clue_state_or_raise(clue_id, "rejected", max_retries=10))
        
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    final_clue = await ClueInfo.filter(clue_id=clue_id).first()
    assert final_clue.status in ["calling", "rejected"]
    success_count = sum(1 for r in results if not isinstance(r, Exception))
    assert success_count >= 50


async def test_lock_timeout_rejection():
    """测试锁超时排队防卡死机制"""
    clue_id = "test_clue_concurrency_005"
    await ClueInfo.create(clue_id=clue_id, status="created")
    
    lock = redis_manager.get_lock(clue_id)
    await lock.lock()
    
    try:
        start_time = asyncio.get_event_loop().time()
        with pytest.raises(LockTimeoutError):
            await update_clue_state_or_raise(clue_id, "calling")
        end_time = asyncio.get_event_loop().time()
        wait_time = end_time - start_time
        assert 15.0 <= wait_time <= 18.0
        
        final_clue = await ClueInfo.filter(clue_id=clue_id).first()
        assert final_clue.status == "created"
    finally:
        await lock.unlock()


async def test_non_existent_clue_update():
    """测试线索不存在时的防御"""
    clue_id = "test_clue_concurrency_ghost_001"
    with pytest.raises(StateTransitionError):
        await update_clue_state_or_raise(clue_id, "calling")

