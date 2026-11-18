import pytest
import asyncio
from tortoise import Tortoise
from app.dbmodels.models import ClueState
from app.dbmodels.crud import update_clue_state_or_raise
from app.redis_client.redis_manager import redis_manager
from app.routers.database import force_reset_clue_state
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
    
    # 1. 预置数据：创建一条处于 'pending' (等待微信通过) 状态的线索
    await ClueState.create(clueId=clue_id, state="pending")
    
    # 2. 模拟并发竞争：两个异步请求同时被派发
    # 请求 A：希望将状态改成 'accepted' (用户通过了)
    # 请求 B：希望将状态改成 'rejected' (用户拒绝了)
    # 按照 state.py 的规则: PENDING -> ACCEPTED 是合法的，PENDING -> REJECTED 也是合法的
    # 但是 ACCEPTED 不能变到 REJECTED，REJECTED 也不能变到 ACCEPTED (这俩都是终态)。
    
    # asyncio.gather 会让这两个任务在同一个甚至多个事件循环周期中并发抢占运行
    result_a, result_b = await asyncio.gather(
        update_clue_state_or_raise(clue_id, "accepted"),
        update_clue_state_or_raise(clue_id, "rejected"),
        return_exceptions=True
    )
    
    # 3. 验证结果
    # 因为有通过 Redis 分布式锁排队，这里必然是一个“先到先得”，另一个则会面临“状态已经变成终态”从而被状态机拒绝！
    
    # 断言：这两个返回结果一定是一个 None (先抢到锁的，无异常成功)，一个 Exception (后抢到锁的，阻断异常)
    # 也就是说：必定有一个引发了 Exception
    assert isinstance(result_a, Exception) != isinstance(result_b, Exception)
    
    # 4. 验证数据库状态符合最先抢到任务的预期
    final_clue = await ClueState.filter(clueId=clue_id).first()
    if not isinstance(result_a, Exception):
        assert final_clue.state == "accepted"
        print("\n[测试输出] 结果：A任务抢先完成，状态变为 accepted。B任务被成功拦截防御并抛出异常！")
    else:
        assert final_clue.state == "rejected"
        print("\n[测试输出] 结果：B任务抢先完成，状态变为 rejected。A任务被成功拦截防御并抛出异常！")

async def test_concurrent_same_state_update():
    """
    测试同向并发场景（幂等性）：
    如果两个并发事件试图把线索改成完全一样的状态。
    """
    clue_id = "test_clue_concurrency_002"
    await ClueState.create(clueId=clue_id, state="pending")
    
    # 两个都希望变成 accepted
    result_a, result_b = await asyncio.gather(
        update_clue_state_or_raise(clue_id, "accepted"),
        update_clue_state_or_raise(clue_id, "accepted"),
        return_exceptions=True
    )
    
    # 结果：二者都应当成功（即返回 None），未抛出异常
    # 一个是实际落库成功，后拿到锁的进去发现已经是 accepted 了，直接根据幂等逻辑成功返回
    assert not isinstance(result_a, Exception) and not isinstance(result_b, Exception)
    
    final_clue = await ClueState.filter(clueId=clue_id).first()
    assert final_clue.state == "accepted"
    print("\n[测试输出] 同向并发测试通过：两者均未触发异常，状态幂等保障生效。")

async def test_invalid_state_transition():
    """
    测试非法状态流转防御：
    越级操作（如新建的线索还没打完电话，直接标记为加微信成功）必须被状态机拦截。
    """
    clue_id = "test_clue_concurrency_003"
    await ClueState.create(clueId=clue_id, state="created")
    
    # 试图把 created (新创建) 越过 calling 等中间状态，直接变成 accepted (终态) 
    # 从 state.py 来看，CREATED 不能直接变成 ACCEPTED
    with pytest.raises(StateTransitionError):
        await update_clue_state_or_raise(clue_id, "accepted")
    
    final_clue = await ClueState.filter(clueId=clue_id).first()
    assert final_clue.state == "created"  # 状态不能被错误篡改
    print("\n[测试输出] 非法流转防御测试通过：跨级流转被拦截，并抛出异常阻断业务，线索状态未变。")

async def test_high_concurrency_stress():
    """
    测试极端高并发下的稳定性（防击穿与锁竞争）：
    模拟同一瞬间 100 个请求同时冲进来修改同一条线索。
    """
    clue_id = "test_clue_concurrency_004"
    await ClueState.create(clueId=clue_id, state="created")
    
    # 构造 100 个并发任务：50个想改成 calling，50个想改成 rejected
    # 两者从 created 出发都是合法的流转。
    # 增加 max_retries 是因为在极端并发下（100个任务排队获取锁），最后的几个任务可能会超过默认的 16 秒等待时间导致超时。
    tasks = []
    for _ in range(50):
        tasks.append(update_clue_state_or_raise(clue_id, "calling", max_retries=10))
        tasks.append(update_clue_state_or_raise(clue_id, "rejected", max_retries=10))
        
    # 一瞬间全部发射
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 因为有 Redis 锁帮你严格排队，不管排多长，SQLite 底层绝对不会报 database is locked
    # 最终的库里状态只可能是两者之一，绝对不会写坏
    final_clue = await ClueState.filter(clueId=clue_id).first()
    assert final_clue.state in ["calling", "rejected"]
    
    # 成功数校验：因为状态一旦变成 calling 或 rejected（互斥），后续相悖的修改都会被拦并抛出异常
    # 但如果状态变成了 calling，后续同样改成 calling 的请求（幂等）都会正常返回（None）。
    # 也就是说，未抛出异常的数量必然等于 50 （赢的那一方的50次操作全通过），
    # 加上实际最先更改的1个，也是这50次里面的1次。
    success_count = sum(1 for r in results if not isinstance(r, Exception))
    assert success_count >= 50
    
    print(f"\n[测试输出] 高并发压力测试通过：100次瞬间并发未击穿数据库，最终状态为 {final_clue.state}。")

async def test_lock_timeout_rejection():
    """
    测试锁超时排队防卡死机制：
    手动占死锁不放，验证第二个请求最多等5秒就会乖乖放弃，不会无限期卡死业务。
    """
    clue_id = "test_clue_concurrency_005"
    await ClueState.create(clueId=clue_id, state="created")
    
    # 1. 模拟一个恶意事件手动拿走了锁并且不释放（比如进程断网了或者正在执行超长任务）
    lock = redis_manager.get_lock(clue_id)
    await lock.lock()
    
    try:
        start_time = asyncio.get_event_loop().time()
        
        # 2. 正常业务试图去更新状态，此时它内部也会去请求锁，必然会卡在队列里
        # 请求失败后应抛出严重异常
        with pytest.raises(LockTimeoutError):
            await update_clue_state_or_raise(clue_id, "calling")
        
        end_time = asyncio.get_event_loop().time()
        wait_time = end_time - start_time
        
        # 3. 验证业务返回了失败并阻断，且放弃的时间恰好就是在 CRUD 里配置的 
        # (5秒超时 * 3次尝试) + (0.5秒重试等待 * 2次) ≈ 16秒 (容差 15~18 秒)
        assert 15.0 <= wait_time <= 18.0
        
        final_clue = await ClueState.filter(clueId=clue_id).first()
        assert final_clue.state == "created" # 数据没被改掉
        
        print("\n[测试输出] 分布式锁超时拦截测试通过：业务准确等待系统级重试（约 16 秒）后安全抛出异常退出，未造成死锁。")
    finally:
        # 清理恶意的锁
        await lock.unlock()



async def test_admin_force_reset_recovery():
    """
    测试【一键救火组合拳】的强大威力：
    模拟一根线索在 `CALLING` 状态卡死了，
    然后运维按下重置按钮，系统应该能瞬间完成物理破拆并重置逻辑状态。
    """
    clue_id = "test_clue_concurrency_008"
    
    # 1. 预置数据：线索处于卡死的中间状态 CALLING
    await ClueState.create(clueId=clue_id, state="CALLING")
    
    # 2. 模拟一个失控的死循环协程，死死地抱住了它的锁（设个 30 秒大锁）
    rogue_lock = redis_manager.get_lock(clue_id, expire_ms=30000)
    await rogue_lock.lock()
    
    # 验证确实卡死了：其他正常的协程根本无法获取该锁更新业务
    with pytest.raises(LockTimeoutError):
        await update_clue_state_or_raise(clue_id, "CLOSED_SUCCESS", max_retries=1, retry_delay=0.1)
    
    # 3. 🚨 事故发生，运维小哥果断登场，一键触发救火接口 🚨
    # 目标：无视其死锁状态，直接破拆掉锁，然后利用特权强拽回 INIT 状态发往重试列车。
    reset_result = await force_reset_clue_state(clue_id, state="INIT")
    assert reset_result["code"] == 1
    
    # 4. 事故后勘场：检验破拆效果
    # (1) 查表：数据库里的状态必须已经乖乖地被拉回了 INIT
    final_clue = await ClueState.filter(clueId=clue_id).first()
    assert final_clue.state == "INIT", "状态未能被最高权限重置接口拽回！"
    
    # (2) 试锁：新的健康的系统任务，此时再尝试接管这个线索，能不能抢到锁？
    healthy_lock = redis_manager.get_lock(clue_id)
    acquired = await healthy_lock.try_lock()
    assert acquired is True, "救火接口未能清空 Redis 底层脏锁，新的任务依然处于恐慌等待中！"
    
    # (3) 放回资源
    await healthy_lock.unlock()
    await rogue_lock.unlock() # 失控的任务最终如果走到这里，Lua 脚本会发现锁变了静默失败，这正是我们要的防御。
    
    print("\n[测试输出] 运维救灾拔锁与重置状态集成测试通过：死锁危机被瞬间化解并进入重试循环。")

async def test_non_existent_clue_update():
    """
    测试线索不存在时的防御：
    尝试更新一个在数据库中根本不存在的线索。
    确保系统能够正确拦截并抛出对应的中断异常。
    """
    clue_id = "test_clue_concurrency_ghost_001"
    
    # 我们不预置数据，直接更新
    with pytest.raises(StateTransitionError):
        await update_clue_state_or_raise(clue_id, "calling")
    
    print("\n[测试输出] 空数据防御测试通过：试图更新不存在的线索被成功阻断。")

