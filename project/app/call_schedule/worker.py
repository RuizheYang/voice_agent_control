"""业务排期独立生命周期；不复用状态超时消费者。"""
import asyncio
import logging

from app.call_schedule.service import CallScheduleService
from app.config import call_schedule_settings as settings

logger = logging.getLogger(__name__)


async def call_dispatch_loop():
    """持续消费到期索引并派发 handler；执行状态及恢复规则交由 service。"""
    service = CallScheduleService()
    logger.info("[Call-Schedule] 【启动】到期派发worker已启动")
    while True:
        try:
            if not await service.run_once():
                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.error("[Call-Schedule] 【异常】到期worker稍后重试: error_type=%s", type(error).__name__)
            await asyncio.sleep(5)


async def call_reconcile_loop():
    """独立补回 Redis 索引并核对未知结果；这条循环不执行送号。"""
    service = CallScheduleService()
    logger.info("[Call-Schedule] 【启动】DB到Redis补偿worker已启动")
    while True:
        try:
            count = await service.reconcile()
            await service.reconcile_unknown()
            logger.debug("[Call-Schedule] 【补偿】本轮已检查任务: count=%s", count)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.error("[Call-Schedule] 【异常】补偿worker稍后重试: error_type=%s", type(error).__name__)
        await asyncio.sleep(settings.reconcile_seconds)
