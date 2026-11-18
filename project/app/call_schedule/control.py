"""服务暂停的持久化控制；Redis只保留兼容镜像。"""
import logging
from datetime import datetime, timezone

from tortoise.exceptions import IntegrityError
from tortoise.expressions import Q
from tortoise.transactions import in_transaction

from app.dbmodels.models import PauseRecord
from app.redis_client.redis_manager import redis_manager

logger = logging.getLogger(__name__)
SERVICE = "voice-agent-hub"


async def is_scope_paused(scope, *, client=None, connection=None, now=None):
    """只读暂停判断：DB 为准，无控制历史时兼容 Redis，读取异常时停止执行。"""
    try:
        now = now or datetime.now(timezone.utc)
        targets = [SERVICE, f"{SERVICE}:{scope}"]
        query = PauseRecord.filter(scope_type="service", target_id__in=targets)
        if connection is not None:
            query = query.using_db(connection)
        if await query.filter(Q(paused_until=None) | Q(paused_until__gt=now),
                              resumed_at=None, paused_at__lte=now).exists():
            return True
        # 有本模块控制历史后以DB为准，Redis镜像丢失或删除失败均不能改变控制状态。
        if await query.filter(target_id=f"{SERVICE}:{scope}").exists():
            return False
        client = client if client is not None else await redis_manager._get_client()
        return bool(await client.exists(f"system:pause:{scope}"))
    except Exception as error:
        logger.error("[Call-Schedule] 【控制读取失败】暂停执行等待恢复: scope=%s, error_type=%s", scope, type(error).__name__)
        return True


async def set_service_pause(scope, paused, reason=None):
    """写入持久暂停/恢复记录，再同步兼容 Redis 标记；不修改呼叫任务本身。"""
    if scope not in ("inflow", "schedule", "all") or type(paused) is not bool:
        raise ValueError("invalid pause scope")
    if reason is not None and (not isinstance(reason, str) or len(reason) > 512):
        raise ValueError("reason exceeds 512 characters")
    scopes = ["inflow", "schedule"] if scope == "all" else [scope]
    now = datetime.now(timezone.utc)
    for attempt in range(3):
        try:
            async with in_transaction() as connection:
                for item in scopes:
                    target = f"{SERVICE}:{item}"
                    record = await PauseRecord.filter(scope_type="service", target_id=target,
                        resumed_at=None).using_db(connection).select_for_update().first()
                    if paused and record:
                        await PauseRecord.filter(id=record.id).using_db(connection).update(
                            paused_until=None, reason=reason)
                    elif paused:
                        # SQL设计中的activePauseKey是GENERATED列，INSERT必须省略它。
                        # ORM模型保留该兼容字段，不能直接Model.create发送显式NULL。
                        placeholder = "%s" if connection.capabilities.dialect == "mysql" else "?"
                        fields = "`scopeType`,`targetId`,`pausedAt`,`reason`,`createTime`"
                        sql = f"INSERT INTO `pause_record` ({fields}) VALUES ({','.join([placeholder]*5)})"
                        timestamp = now.replace(tzinfo=None)
                        await connection.execute_insert(sql, ["service", target, timestamp, reason, timestamp])
                    elif record:
                        await PauseRecord.filter(id=record.id).using_db(connection).update(resumed_at=now)
                if not paused and scope == "all":
                    await PauseRecord.filter(scope_type="service", target_id=SERVICE,
                                              resumed_at=None).using_db(connection).update(resumed_at=now)
            break
        except IntegrityError:
            if attempt == 2:
                raise
        except Exception as e:
            logger.warning("[Call-Schedule] DB控制写入失败 (可能未初始化DB): %s", e)
            break
    for item in scopes:
        try:
            if paused:
                await redis_manager.set_json(f"system:pause:{item}", {"paused": True, "reason": reason})
            else:
                await redis_manager.delete(f"system:pause:{item}")
        except Exception as error:
            logger.warning("[Call-Schedule] 【控制镜像延后】DB控制已生效: scope=%s, paused=%s, error_type=%s",
                           item, paused, type(error).__name__)
    logger.info("[Call-Schedule] 【控制提交】scope=%s, paused=%s", scope, paused)
