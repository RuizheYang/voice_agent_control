from __future__ import annotations
import logging
from datetime import datetime

from app.utils.time_utils import ensure_tz_aware, get_now

logger = logging.getLogger(__name__)
import asyncio
import json

import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional, Union

import redis
from app.config import config
from redis import asyncio as aioredis
from redis.asyncio import RedisCluster
from redis.asyncio.client import PubSub
from redis.asyncio.cluster import ClusterNode  # 必须引入


class RedisManager:
    def __init__(self):
        self._redis: Optional[Union[aioredis.Redis, RedisCluster]] = None
        self._initialized: bool = False
        self._lock = asyncio.Lock()

    async def init_redis(self):
        """生产级初始化：修复 AttributeError 并彻底消除 Unclosed 警告"""
        if self._initialized and self._redis:
            return

        async with self._lock:
            if self._initialized and self._redis:
                return

            client = None
            try:
                if config.redis_config.cluster_mode:
                    # 1. 解析节点并创建 ClusterNode 对象（修复 'dict' object has no attribute 'host'）
                    startup_nodes = []
                    for node_url in config.redis_config.cluster_nodes:
                        try:
                            clean_node = node_url.strip().split("//")[-1]
                            host, port = clean_node.split(":")
                            # 必须使用 ClusterNode 对象
                            startup_nodes.append(ClusterNode(host, int(port)))
                        except Exception as e:
                            logger.warning(f"解析节点 {node_url} 失败: {e}")

                    if not startup_nodes:
                        raise ValueError("没有有效的集群节点配置")

                    # 2. 创建集群实例
                    client = RedisCluster(
                        startup_nodes=startup_nodes,
                        decode_responses=True,
                        password=config.redis_config.password,
                        max_connections=config.redis_config.max_connections,
                        socket_timeout=config.redis_config.socket_timeout,
                        socket_connect_timeout=config.redis_config.socket_connect_timeout,
                        health_check_interval=30,
                        cluster_error_retry_attempts=3,
                        require_full_coverage=False,
                        reinitialize_steps=5
                    )

                    # 3. 显式初始化
                    await client.initialize()
                else:
                    # 单节点模式
                    client = aioredis.from_url(
                        config.redis_config.redis_url,
                        decode_responses=True,
                        max_connections=config.redis_config.max_connections,
                        health_check_interval=30
                    )

                # 4. 测试连通性
                await client.ping()

                self._redis = client
                self._initialized = True
                logger.info("Redis [Production] 连接初始化成功")

            except Exception as e:
                logger.error(f"Redis 连接初始化致命错误: {e}")
                # --- 核心修复：如果初始化过程中任何一步失败，必须立即关闭 client ---
                # 这会关闭所有 startup_nodes 里的潜在连接，消除 Unclosed 警告
                if client:
                    await client.aclose()
                self._initialized = False
                raise

    async def _get_client(self) -> Union[aioredis.Redis, RedisCluster]:
        if not self._initialized or self._redis is None:
            await self.init_redis()
        return self._redis

    async def _invalidate_client(self, client) -> None:
        """淘汰发生连接异常的客户端，允许下一次请求重建连接和集群拓扑。"""
        async with self._lock:
            if self._redis is not client:
                return
            self._redis = None
            self._initialized = False

        try:
            await client.aclose()
        except Exception as e:
            logger.warning(f"释放失效 Redis 客户端时出错: {e}")

    async def close(self):
        """安全关闭"""
        async with self._lock:
            if self._redis:
                try:
                    await self._redis.aclose()
                    logger.info("Redis 连接池已安全释放")
                except Exception as e:
                    logger.error(f"关闭 Redis 时出错: {e}")
                finally:
                    self._redis = None
                    self._initialized = False

    # ------------------- 业务方法（保持完整） -------------------

    async def set_json(self, key: str, value: Any, expire: int = None) -> bool:
        try:
            client = await self._get_client()
            return await client.set(key, json.dumps(value), ex=expire)
        except Exception as e:
            logger.error(f"Redis set_json 失败 [key={key}]: {e}")
            return False

    async def get_json(self, key: str) -> Optional[Any]:
        try:
            client = await self._get_client()
            value = await client.get(key)
            return json.loads(value) if value else None
        except Exception as e:
            logger.error(f"Redis get_json 失败 [key={key}]: {e}")
            return None

    async def exists(self, key: str) -> bool:
        try:
            client = await self._get_client()
            return await client.exists(key) > 0
        except Exception as e:
            logger.error(f"Redis exists 失败: {e}")
            return False

    async def delete(self, key: str) -> bool:
        try:
            client = await self._get_client()
            return await client.delete(key) > 0
        except Exception as e:
            logger.error(f"Redis delete 失败: {e}")
            return False

    async def rpush_list(self, key: str, *values: Any) -> Optional[int]:
        try:
            client = await self._get_client()
            str_values = [json.dumps(v) for v in values]
            return await client.rpush(key, *str_values)
        except Exception as e:
            logger.error(f"Redis rpush_list 失败: {e}")
            return None

    async def queue_pop(self, queue_name: str, timeout: int = 0) -> Optional[Any]:
        client = None
        try:
            client = await self._get_client()
            if timeout > 0:
                res = await client.blpop(queue_name, timeout=timeout)
                return json.loads(res[1]) if res else None
            value = await client.lpop(queue_name)
            return json.loads(value) if value else None
        except Exception as e:
            logger.error(f"Redis queue_pop 失败: {e}")
            if client is not None:
                await self._invalidate_client(client)
            raise

    @asynccontextmanager
    async def get_pubsub_scope(self) -> AsyncGenerator[PubSub, None]:
        client = await self._get_client()
        ps = client.pubsub()
        try:
            yield ps
        finally:
            await ps.close()

    async def haskey(self, key: str) -> bool:
        try:
            client = await self._get_client()
            return await client.exists(key) > 0
        except Exception as e:
            logger.error(f"检查key存在失败: {e}")
            return False
        
    def get_lock(self, clueId:str, expire_ms=30000) -> 'AsyncRedisDistributedLock':
        """获取一个分布式锁对象，锁的 key 会基于 clueId 生成"""
        lock_key = f"lock:clue:{clueId}"
        # 传入自身 (manager) 而非底层 redis实例，避免获取到尚未初始化完成的 None对象
        return AsyncRedisDistributedLock(self, lock_key, expire_ms)

    def get_delayed_queue(self, queue_key: str = "voice_agent_hub:delayed_tasks") -> 'AsyncRedisDelayedQueue':
        """获取一个延迟队列实例"""
        return AsyncRedisDelayedQueue(self, queue_key)
        
class AsyncRedisDistributedLock:
    
    def __init__(
            self, 
            redis_manager: RedisManager, 
            lock_key: str, 
            expire_ms: int = 30000
        ):
        self.redis_manager = redis_manager
        self.lock_key = lock_key
        self.expire_ms = expire_ms
        self.local_value = None  # 存储当前协程生成的唯一标识

    async def _get_redis(self) -> Union[aioredis.Redis, RedisCluster]:
        """动态获取客户端，保证在使用锁时客户端一定初始化完毕"""
        return await self.redis_manager._get_client()

    async def try_lock(self) -> bool:
        """异步尝试获取锁（非阻塞）"""
        if self.local_value is not None:
            logger.warning(f"锁实例被重复使用 [{self.lock_key}]，请重新获取新锁或先释放当前锁")
            return False

        self.local_value = str(uuid.uuid4())
        
        redis = None
        try:
            redis = await self._get_redis()
            result = await redis.set(
                self.lock_key, 
                self.local_value, 
                nx=True, 
                px=self.expire_ms
            )
            
            if result:
                return True
            else:
                self.local_value = None  # 获取失败重置凭据
                return False
        except Exception as e:
            logger.error(f"尝试获取 Redis 锁异常 [{self.lock_key}]: {e}")
            self.local_value = None
            if redis is not None:
                await self.redis_manager._invalidate_client(redis)
            raise

    async def lock(self, retry_interval_ms: int = 100, timeout_ms: int = 5000) -> bool:
        """异步阻塞式获取锁，带超时时间"""
        
        start_time = asyncio.get_event_loop().time() * 1000
        while (asyncio.get_event_loop().time() * 1000 - start_time) < timeout_ms:
            if await self.try_lock():
                return True
            
            # time.sleep 是同步阻塞的，会卡死整个事件循环。
            await asyncio.sleep(retry_interval_ms / 1000.0)
            
        return False

    async def unlock(self) -> bool:
        """异步释放锁（使用 Lua 脚本保证原子性）"""
        if not self.local_value:
            return False

        lua_script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
        """
        
        # 第二步：安全删锁
        try:
            redis = await self._get_redis()
            result = await redis.eval(lua_script, 1, self.lock_key, self.local_value)
            return bool(result)
        except Exception as e:
            logger.error(f"释放Redis锁异常 [{self.lock_key}]: {e}")
            return False
        finally:
            self.local_value = None

    async def __aenter__(self):
        """支持 async with 上下文管理器，防止异常导致忘解锁"""
        # 使用带超时的获取方法，超时直接抛出异常
        acquired = await self.lock(timeout_ms=5000)
        if not acquired:
            raise TimeoutError(f"获取分布式锁超时 [{self.lock_key}]")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """块退出，确保必定解锁"""
        await self.unlock()

class AsyncRedisDelayedQueue:
    """
    基于 Redis ZSet 和 Lua 脚本的分布式延迟队列
    """
    def __init__(self, redis_manager: RedisManager, queue_key: str):
        self.redis_manager = redis_manager
        self.queue_key = queue_key
        # Lua 脚本：原子性拉取并移除过期任务
        self.lua_pop_expired = """
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local limit = tonumber(ARGV[2])

            local members = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'LIMIT', 0, limit)
            if #members > 0 then
                redis.call('ZREM', key, unpack(members))
            end
            return members
        """
        self._script = None

    async def _get_redis(self) -> Union[aioredis.Redis, RedisCluster]:
        """动态获取客户端，保证客户端一定初始化完毕"""
        return await self.redis_manager._get_client()

    async def _get_script_obj(self):
        """懒加载 Lua 脚本对象"""
        if self._script is None:
            redis = await self._get_redis()
            self._script = redis.register_script(self.lua_pop_expired)
        return self._script

    async def add_task(self, task_id: str, delay_seconds: int) -> bool:
        """添加延迟任务 (使用当前时间 + 延迟秒数作为 Score)"""
        execute_at = int(time.time()) + delay_seconds
        try:
            redis = await self._get_redis()
            # Redis ZSet 添加语法： mapping 为 {member: score}
            await redis.zadd(self.queue_key, {task_id: execute_at})
            # ZADD 返回0表示更新已有成员，同样是写入成功。
            return True
        except Exception as e:
            logger.error(f"添加延迟任务 {task_id} 失败: {e}")
            return False

    async def add_schedule_status_task(self, task_id: str, start_time: datetime | str) -> bool:
        """
        专门为 schedule 状态定制的延迟任务。

        参数:
            task_id: 任务唯一标识
            start_time_str: 字符串格式的时间，例如 "2026-06-29 19:00:00"
        """
        try:
            # 1. 将字符串解析为 datetime 对象（假设输入为本地时区，如 Asia/Shanghai）
            #    请根据你的实际服务器时区调整 ZoneInfo
            if isinstance(start_time, datetime):
                dt = start_time
            elif isinstance(start_time, str):
                dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            else:
                logger.error(f"添加任务失败: start_time 参数类型不支持 ({type(start_time)})")
                return False
            dt = ensure_tz_aware(dt)

            # 2. 转换为基础的 Unix 时间戳（秒级整型）
            start_timestamp = int(dt.timestamp())

            # 3. 核心业务逻辑：(start_time - 当前时间 + 4小时) + 当前时间 = start_time + 4小时
            four_hours_in_seconds = 4 * 3600
            execute_at = start_timestamp + four_hours_in_seconds

            # 4. 写入 Redis 延迟队列
            redis = await self._get_redis()
            result = await redis.zadd(self.queue_key, {task_id: execute_at})
            logger.info(f"添加 schedule 状态任务 {task_id} 成功，计划执行时间: {dt}, 实际执行时间: {datetime.fromtimestamp(execute_at)}")
            return bool(result)

        except ValueError as ve:
            logger.error(f"时间格式解析失败，期望 %Y-%m-%d %H:%M:%S, 收到: {start_time}, 错误: {ve}")
            return False
        except Exception as e:
            logger.error(f"添加 schedule 状态任务 {task_id} 失败: {e}")
            return False

    async def remove_task(self, task_id: str) -> bool:
        """从队列中提前移除任务（例如任务被正常处理完，不需要报警了）"""
        try:
            redis = await self._get_redis()
            result = await redis.zrem(self.queue_key, task_id)
            return bool(result)
        except Exception as e:
            logger.error(f"移除延迟任务 {task_id} 失败: {e}")
            return False

    async def pop_expired_tasks(self, batch_size: int = 50) -> list[str]:
        """原子拉取并删除已超时的任务"""
        now = int(time.time())
        try:
            script = await self._get_script_obj()
            # 执行 Lua 脚本，Keys 传递队列名，Args 传递当前时间戳和单次拉取限制
            members = await script(keys=[self.queue_key], args=[now, batch_size])
            # 处理 Redis 返回的 Bytes 类型
            return [m.decode('utf-8') if isinstance(m, bytes) else m for m in members]
        except Exception as e:
            logger.error(f"拉取过期任务异常: {e}")
            return []

# 全局单例
redis_manager = RedisManager()
