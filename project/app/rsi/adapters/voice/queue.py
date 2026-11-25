"""Standalone Redis Lua priority + global throttle queue for the edge Voice Adapter."""
from __future__ import annotations

import json
from typing import Any, Optional

PUBLISH = """
local delay, metadata = KEYS[1], KEYS[4]
if redis.call('HEXISTS', metadata, ARGV[1]) == 1 then return 0 end
redis.call('HSET', metadata, ARGV[1], ARGV[4])
redis.call('ZADD', delay, ARGV[2], ARGV[1])
return 1
"""

RESERVE = """
local delay, ready, processing = KEYS[1], KEYS[2], KEYS[3]
local metadata, tokens, throttle = KEYS[4], KEYS[5], KEYS[6]
local expired = redis.call('ZRANGEBYSCORE', processing, '-inf', ARGV[1], 'LIMIT', 0, 200)
for _, id in ipairs(expired) do
    redis.call('ZREM', processing, id)
    redis.call('HDEL', tokens, id)
    if redis.call('HEXISTS', metadata, id) == 1 then
        redis.call('ZADD', delay, ARGV[1], id)
    end
end
local due = redis.call('ZRANGEBYSCORE', delay, '-inf', ARGV[1], 'LIMIT', 0, 500)
for _, id in ipairs(due) do
    local raw = redis.call('HGET', metadata, id)
    redis.call('ZREM', delay, id)
    if raw then
        local meta = cjson.decode(raw)
        redis.call('ZADD', ready, -meta.priority, meta.member)
    end
end
local remaining = redis.call('ZRANGEBYSCORE', delay, '-inf', ARGV[1], 'LIMIT', 0, 1)
if #remaining > 0 then return false end
local next_at = tonumber(redis.call('GET', throttle) or '0')
if next_at > tonumber(ARGV[1]) then return false end
local first = redis.call('ZRANGE', ready, 0, 0)
if #first == 0 then return false end
local member = first[1]
local id = string.match(member, ':0*(%d+)$')
redis.call('ZREM', ready, member)
redis.call('ZADD', processing, tonumber(ARGV[1]) + tonumber(ARGV[3]), id)
redis.call('HSET', tokens, id, ARGV[2])
redis.call('SET', throttle, tonumber(ARGV[1]) + tonumber(ARGV[4]), 'PX', math.max(1, tonumber(ARGV[4])))
return id
"""

ACK = """
local delay, ready, processing = KEYS[1], KEYS[2], KEYS[3]
local metadata, tokens = KEYS[4], KEYS[5]
if redis.call('HGET', tokens, ARGV[1]) ~= ARGV[2] then return 0 end
local raw = redis.call('HGET', metadata, ARGV[1])
if raw then redis.call('ZREM', ready, cjson.decode(raw).member) end
redis.call('ZREM', delay, ARGV[1])
redis.call('ZREM', processing, ARGV[1])
redis.call('HDEL', metadata, ARGV[1])
redis.call('HDEL', tokens, ARGV[1])
return 1
"""


class VoicePriorityQueue:
    """Rebuildable Redis priority-and-throttle index scoped to {rsi_voice} hash tag."""

    def __init__(self, client: Any, *, interval_ms: int = 1000) -> None:
        self.client = client
        self.interval_ms = max(1, int(interval_ms))
        self.keys = [
            f"rsi:voice:{{rsi_voice}}:{suffix}"
            for suffix in ("delay", "ready", "processing", "meta", "tokens", "throttle")
        ]

    async def publish(self, task_id: int, run_at_ms: int, priority: int) -> bool:
        member = f"{int(run_at_ms):016d}:{int(task_id):020d}"
        meta = json.dumps(
            {"member": member, "priority": int(priority)},
            separators=(",", ":"),
        )
        res = await self.client.eval(
            PUBLISH,
            len(self.keys),
            *self.keys,
            str(task_id),
            int(run_at_ms),
            int(priority),
            meta,
        )
        return bool(res)

    async def reserve(
        self,
        now_ms: int,
        token: str,
        lease_ms: int = 180000,
    ) -> Optional[int]:
        value = await self.client.eval(
            RESERVE,
            len(self.keys),
            *self.keys,
            int(now_ms),
            token,
            int(lease_ms),
            self.interval_ms,
        )
        return int(value) if value else None

    async def ack(self, task_id: int, token: str) -> bool:
        res = await self.client.eval(
            ACK,
            len(self.keys),
            *self.keys,
            str(task_id),
            token,
        )
        return bool(res)
