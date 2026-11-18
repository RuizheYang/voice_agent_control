"""Redis 只保存可重建的任务索引；所有脚本键共享 {call} hash tag。"""
import json


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
-- 过期 Redis 租约先归还；DB 层另有权威租约与 submitting 恢复检查。
local expired = redis.call('ZRANGEBYSCORE', processing, '-inf', ARGV[1], 'LIMIT', 0, 200)
for _, id in ipairs(expired) do
    redis.call('ZREM', processing, id)
    redis.call('HDEL', tokens, id)
    if redis.call('HEXISTS', metadata, id) == 1 then
        redis.call('ZADD', delay, ARGV[1], id)
    end
end
-- 到期时间单独判断，未来高优先级任务不会遮挡已到期任务。
local due = redis.call('ZRANGEBYSCORE', delay, '-inf', ARGV[1], 'LIMIT', 0, 500)
for _, id in ipairs(due) do
    local raw = redis.call('HGET', metadata, id)
    redis.call('ZREM', delay, id)
    if raw then
        local meta = cjson.decode(raw)
        redis.call('ZADD', ready, -meta.priority, meta.member)
    end
end
-- 单次Lua有界执行；还有到期任务时先继续搬运，避免批次边界破坏优先级。
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


class CallQueue:
    """只维护 Redis 索引；领取成功不表示拥有 DB 执行权。"""
    def __init__(self, client, *, interval_ms=1000):
        self.client = client
        self.interval_ms = interval_ms
        self.keys = [f"voice:{{call}}:{suffix}" for suffix in
                     ("delay", "ready", "processing", "meta", "tokens", "throttle")]

    async def publish(self, task_id, run_at_ms, priority):
        """幂等加入延迟索引；member 让同优先级按时间、任务 ID 稳定排序。"""
        member = f"{int(run_at_ms):016d}:{int(task_id):020d}"
        meta = json.dumps({"member": member, "priority": int(priority)}, separators=(",", ":"))
        return bool(await self.client.eval(PUBLISH, len(self.keys), *self.keys,
                                           str(task_id), int(run_at_ms), priority, meta))

    async def reserve(self, now_ms, token, lease_ms=180000):
        """原子恢复租约、搬运到期任务并领取；本次未领取任务时返回 None。"""
        value = await self.client.eval(RESERVE, len(self.keys), *self.keys,
                                       int(now_ms), token, lease_ms, self.interval_ms)
        return int(value) if value else None

    async def ack(self, task_id, token):
        """仅领取者 token 可删除索引；调用前必须先提交 DB 结果。"""
        return bool(await self.client.eval(ACK, len(self.keys), *self.keys, str(task_id), token))


async def get_call_queue():
    from app.redis_client.redis_manager import redis_manager
    from app.config import call_schedule_settings as settings
    return CallQueue(await redis_manager._get_client(), interval_ms=settings.dispatch_interval_ms)
