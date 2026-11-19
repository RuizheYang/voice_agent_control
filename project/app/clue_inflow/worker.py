"""线索流入任务执行：文件导入与提交后入池恢复；不消费呼叫任务。"""
import asyncio
import logging
from datetime import timedelta, timezone

from app.clue_inflow import InflowError, repository
from app.dbmodels.models import ClueInfo, OutboundStrategy
from app.schemas.clue_create import NormalizedClue
from app.utils.decrypt import crypted_manager
from app.utils.time_utils import LOCAL_TZ
from app.utils.workers import is_inflow_paused

logger = logging.getLogger(__name__)


async def run_file_once(service):
    """领取并执行一个文件导入任务，保留分块断点及失败重试。"""
    task = await repository.claim_task()
    if task is None:
        return False
    try:
        if task.payload.get("inflowType") != "file":
            raise InflowError(40001, "该导入任务无法由文件worker处理")
        await service.process_file(task)
    except repository.LeaseLost:
        logger.info("[Clue-Inflow] 【停止】租约已失效，停止当前文件处理: task_id=%s, request_id=%r", task.id, task.request_id)
    except asyncio.CancelledError:
        # 保留 processing/租约，其他进程可接管；不把停机标记为业务失败。
        logger.info("[Clue-Inflow] 【停止】文件处理被中断，保留租约等待接管: task_id=%s, request_id=%r", task.id, task.request_id)
        raise
    except Exception as error:
        logger.warning("[Clue-Inflow] 【异常】文件处理未完成，准备记录重试或失败: task_id=%s, request_id=%r, error_type=%s",
                       task.id, task.request_id, type(error).__name__)
        try:
            await repository.fail_or_retry(task.id, task.lock_token, error)
        except repository.LeaseLost:
            logger.info("[Clue-Inflow] 【停止】异常处理时租约已失效，不覆盖其他执行者结果: task_id=%s", task.id)
    return True


async def file_import_loop():
    from app.clue_inflow.service import InflowService
    service = InflowService()
    logger.info("[Clue-Inflow] 【启动】文件导入worker已启动，仅处理导入任务")
    while True:
        try:
            if not await run_file_once(service):
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("[Clue-Inflow] 【停止】文件导入worker已停止")
            raise
        except Exception as error:
            logger.error("[Clue-Inflow] 【失败】文件导入worker暂不可用，5秒后重试: error_type=%s", type(error).__name__)
            await asyncio.sleep(5)


def as_utc(value):
    """新受理字段以UTC存储，兼容项目 use_tz=False 返回的 naive 值。"""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _safe_decrypt(value: str | None) -> str | None:
    if not value:
        return value
    if len(value) > 20:
        try:
            return crypted_manager.decrypt_field(value)
        except Exception:
            return value
    return value


def to_normalized(clue, request_id):
    return NormalizedClue(
        clueId=clue.clue_id, phoneNumber=_safe_decrypt(clue.phone_number) or "",
        customerName=_safe_decrypt(clue.customer_name) if clue.customer_name else None,
        workflowId=clue.workflow_id, agentId=clue.agent_id, strategyId=clue.strategy_id,
        externalTaskId=clue.external_task_id, requestId=request_id,
        accessReceiveTime=as_utc(clue.received_at).astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        agentData=clue.agent_data, overrides=clue.overrides or {},
    )


async def run_entry_once(service, task_id=None):
    """领取已受理线索的入池任务，提交后调用现有handler完成排期。"""
    if await is_inflow_paused():
        return False
    task = await repository.claim_task("new_clue", task_id=task_id)
    if task is None:
        return False
    try:
        clue = await ClueInfo.get(clue_id=task.clue_id)
        strategy = await OutboundStrategy.filter(strategy_id=clue.strategy_id).first()
        if strategy and strategy.status == "paused":
            async with repository.transaction() as connection:
                current = await repository.locked_task(connection, task.id, task.lock_token)
                current.status = "pending"
                current.run_at = repository.utc_now()+timedelta(seconds=5)
                current.lock_token = current.locked_until = None
                await current.save(using_db=connection)
            logger.info("[Clue-Inflow] 【暂停】策略已暂停，入池任务保留等待恢复: task_id=%s, clue_id=%r", task.id, clue.clue_id)
            return True
        if clue.status not in ("created", "scheduled"):
            entry_status = "skipped"
        elif clue.expires_at and as_utc(clue.expires_at) <= repository.utc_now():
            # 入池前过期不再拨号；状态结束与任务完成一起提交。
            entry_status = "expired"
        else:
            logger.info("[Clue-Inflow] 【入池开始】调用新线索handler: task_id=%s, clue_id=%r, request_id=%r",
                        task.id, clue.clue_id, task.request_id)
            normalized = to_normalized(clue, task.request_id)
            if service.new_clue_handler is not None:
                result = await service.new_clue_handler.handle(normalized)
            else:
                from app.clue_handler.factory import ClueHandlerFactory
                handler = await ClueHandlerFactory.get_handler_by_workflow(clue.workflow_id)
                # 复用处理锁及 handler，不重复调用旧 create_clue。
                result = await handler.on_clue_queue(normalized)
            latest = await ClueInfo.get(clue_id=clue.clue_id)
            if result is not True and latest.status != "rejected":
                raise RuntimeError("新线索handler尚未完成入池")
            entry_status = "rejected" if latest.status == "rejected" else "entered"
        async with repository.transaction() as connection:
            current = await repository.locked_task(connection, task.id, task.lock_token)
            if entry_status == "expired":
                await ClueInfo.filter(id=clue.id, status__in=["created", "scheduled"]).using_db(connection).update(
                    status="rejected", close_reason="expired_before_entry",
                )
            current.status = "succeeded"
            current.result = {"entryStatus": entry_status}
            current.finished_at = repository.utc_now()
            current.last_error = None
            current.lock_token = current.locked_until = None
            await current.save(using_db=connection)
        logger.info("[Clue-Inflow] 【入池完成】入池任务结果已提交: task_id=%s, clue_id=%r, entry_status=%s",
                    task.id, clue.clue_id, entry_status)
    except repository.LeaseLost:
        logger.info("[Clue-Inflow] 【停止】入池租约失效，停止当前处理: task_id=%s", task.id)
    except asyncio.CancelledError:
        logger.info("[Clue-Inflow] 【停止】入池处理中断，保留任务等待接管: task_id=%s", task.id)
        raise
    except Exception as error:
        logger.warning("[Clue-Inflow] 【入池失败】准备记录重试结果: task_id=%s, clue_id=%r, error_type=%s",
                       task.id, task.clue_id, type(error).__name__)
        try:
            await repository.fail_or_retry(task.id, task.lock_token, error)
        except repository.LeaseLost:
            logger.info("[Clue-Inflow] 【停止】入池失败处理时租约已失效: task_id=%s", task.id)
    return True


async def new_clue_entry_loop():
    from app.clue_inflow.service import InflowService
    service = InflowService()
    logger.info("[Clue-Inflow] 【启动】新线索入池恢复worker启动")
    while True:
        try:
            if not await run_entry_once(service):
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("[Clue-Inflow] 【停止】新线索入池恢复worker停止")
            raise
        except Exception as error:
            logger.error("[Clue-Inflow] 【失败】入池恢复worker暂不可用: error_type=%s", type(error).__name__)
            await asyncio.sleep(5)
