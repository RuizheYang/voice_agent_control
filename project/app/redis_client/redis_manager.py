"""Slim V3 Redis connection pool & distributed lock manager."""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional, Union

from redis import asyncio as aioredis
from redis.asyncio import RedisCluster
from redis.asyncio.client import PubSub
from redis.asyncio.cluster import ClusterNode

from app.config import config

logger = logging.getLogger(__name__)


class RedisManager:
    """Optional Redis connection pool supporting single-node & cluster modes plus distributed locks."""

    def __init__(self) -> None:
        self._redis: Optional[Union[aioredis.Redis, RedisCluster]] = None
        self._initialized: bool = False
        self._lock = asyncio.Lock()

    async def init_redis(self) -> None:
        """Initialize Redis pool safely and close leaked clients upon failure."""
        if self._initialized and self._redis:
            return

        async with self._lock:
            if self._initialized and self._redis:
                return

            client = None
            try:
                if config.redis_config.cluster_mode:
                    startup_nodes = []
                    for node_url in config.redis_config.cluster_nodes:
                        try:
                            clean_node = node_url.strip().split("//")[-1]
                            host, port = clean_node.split(":")
                            startup_nodes.append(ClusterNode(host, int(port)))
                        except Exception as err:
                            logger.warning("解析 Redis 集群节点 %s 失败: %s", node_url, err)

                    if not startup_nodes:
                        raise ValueError("没有有效的 Redis 集群节点配置")

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
                        reinitialize_steps=5,
                    )
                    await client.initialize()
                else:
                    client = aioredis.from_url(
                        config.redis_config.redis_url,
                        decode_responses=True,
                        max_connections=config.redis_config.max_connections,
                        health_check_interval=30,
                    )

                await client.ping()
                self._redis = client
                self._initialized = True
                logger.info("Redis 连接初始化成功")
            except Exception as err:
                logger.error("Redis 连接初始化失败: %s", err)
                if client is not None:
                    await client.aclose()
                self._initialized = False
                raise

    async def _get_client(self) -> Union[aioredis.Redis, RedisCluster]:
        if not self._initialized or self._redis is None:
            await self.init_redis()
        assert self._redis is not None
        return self._redis

    async def _invalidate_client(self, client: Any) -> None:
        async with self._lock:
            if self._redis is not client:
                return
            self._redis = None
            self._initialized = False

        try:
            await client.aclose()
        except Exception as err:
            logger.warning("释放失效 Redis 客户端时出错: %s", err)

    async def close(self) -> None:
        async with self._lock:
            if self._redis is not None:
                try:
                    await self._redis.aclose()
                    logger.info("Redis 连接池已安全释放")
                except Exception as err:
                    logger.error("关闭 Redis 时出错: %s", err)
                finally:
                    self._redis = None
                    self._initialized = False

    async def set_json(self, key: str, value: Any, expire: Optional[int] = None) -> bool:
        try:
            client = await self._get_client()
            return bool(await client.set(key, json.dumps(value), ex=expire))
        except Exception as err:
            logger.error("Redis set_json 失败 [key=%s]: %s", key, err)
            return False

    async def get_json(self, key: str) -> Optional[Any]:
        try:
            client = await self._get_client()
            value = await client.get(key)
            return json.loads(value) if value else None
        except Exception as err:
            logger.error("Redis get_json 失败 [key=%s]: %s", key, err)
            return None

    async def exists(self, key: str) -> bool:
        try:
            client = await self._get_client()
            return await client.exists(key) > 0
        except Exception as err:
            logger.error("Redis exists 失败: %s", err)
            return False

    async def haskey(self, key: str) -> bool:
        return await self.exists(key)

    async def delete(self, key: str) -> bool:
        try:
            client = await self._get_client()
            return await client.delete(key) > 0
        except Exception as err:
            logger.error("Redis delete 失败: %s", err)
            return False

    @asynccontextmanager
    async def get_pubsub_scope(self) -> AsyncGenerator[PubSub, None]:
        client = await self._get_client()
        ps = client.pubsub()
        try:
            yield ps
        finally:
            await ps.close()

    def get_lock(self, entity_id: str, expire_ms: int = 30000) -> "AsyncRedisDistributedLock":
        """Acquire a Redis distributed lock keyed per canonical entity."""
        return AsyncRedisDistributedLock(self, f"lock:entity:{entity_id}", expire_ms)


class AsyncRedisDistributedLock:
    """Async Redis distributed mutex using unique ownership tokens and atomic Lua release."""

    def __init__(
        self,
        redis_mgr: RedisManager,
        lock_key: str,
        expire_ms: int = 30000,
    ) -> None:
        self.redis_manager = redis_mgr
        self.lock_key = lock_key
        self.expire_ms = expire_ms
        self.local_value: Optional[str] = None

    async def _get_redis(self) -> Union[aioredis.Redis, RedisCluster]:
        return await self.redis_manager._get_client()

    async def try_lock(self) -> bool:
        if self.local_value is not None:
            logger.warning("锁实例被重复使用 [%s]", self.lock_key)
            return False

        self.local_value = str(uuid.uuid4())
        client = None
        try:
            client = await self._get_redis()
            result = await client.set(
                self.lock_key,
                self.local_value,
                nx=True,
                px=self.expire_ms,
            )
            if result:
                return True
            self.local_value = None
            return False
        except Exception as err:
            logger.error("尝试获取 Redis 锁异常 [%s]: %s", self.lock_key, err)
            self.local_value = None
            if client is not None:
                await self.redis_manager._invalidate_client(client)
            raise

    async def lock(self, retry_interval_ms: int = 100, timeout_ms: int = 5000) -> bool:
        loop = asyncio.get_running_loop()
        start_time = loop.time() * 1000
        while (loop.time() * 1000 - start_time) < timeout_ms:
            if await self.try_lock():
                return True
            await asyncio.sleep(retry_interval_ms / 1000.0)
        return False

    async def unlock(self) -> bool:
        if not self.local_value:
            return False

        lua_script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
        """
        try:
            client = await self._get_redis()
            result = await client.eval(lua_script, 1, self.lock_key, self.local_value)
            return bool(result)
        except Exception as err:
            logger.error("释放 Redis 锁异常 [%s]: %s", self.lock_key, err)
            return False
        finally:
            self.local_value = None

    async def __aenter__(self) -> "AsyncRedisDistributedLock":
        acquired = await self.lock(timeout_ms=5000)
        if not acquired:
            raise TimeoutError(f"获取分布式锁超时 [{self.lock_key}]")
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.unlock()


redis_manager = RedisManager()
