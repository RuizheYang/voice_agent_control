"""线索流入持久化：请求占位、两表同步、任务租约及文件断点。"""
import hashlib
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone

from tortoise.exceptions import IntegrityError
from tortoise.expressions import Q
from tortoise.transactions import in_transaction

from app.clue_inflow import InflowError
from app.dbmodels.models import Clue, ClueInfo, ExecutionTask, OutboundStrategy, WorkflowBinding
from app.utils.decrypt import crypted_manager

LEASE_SECONDS = 180
logger = logging.getLogger(__name__)


def utc_now():
    return datetime.now(timezone.utc)


def transaction():
    return in_transaction()


def request_key(app_id, request_id):
    data = json.dumps(["import", app_id, request_id], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


async def get_workflow(workflow_id):
    # 只读取 SQL 设计稿中真实存在的绑定字段，不依赖旧管理页面附加列。
    return await WorkflowBinding.filter(workflow_id=workflow_id).first().values(
        "workflow_id", "agent_id", "strategy_id", "binding_version",
    )


async def get_strategy(strategy_id):
    return await OutboundStrategy.filter(strategy_id=strategy_id).first()


async def ensure_new_request(app_id, request_id):
    key = request_key(app_id, request_id)
    if await ExecutionTask.filter(task_key=key).exists():
        raise await duplicate_request_error(key)


async def resource_allowed(field, value, workflows):
    query = WorkflowBinding.filter(**{field: value})
    if "*" not in workflows:
        query = query.filter(workflow_id__in=workflows)
    return await query.exists()


async def register_request(connection, app_id, request_id, workflow_id, payload):
    key = request_key(app_id, request_id)
    existing = await ExecutionTask.filter(task_key=key).using_db(connection).first()
    if existing:
        logger.info("[Clue-Inflow] 【幂等拦截】重复导入请求: task_id=%s, app_id=%r, request_id=%r, workflow_id=%r",
                    existing.id, app_id, request_id, workflow_id)
        raise InflowError(40005, "request_id 已受理，请查询原导入任务", 409,
                          {"import_task_id": str(existing.id)})
    try:
        return await ExecutionTask.create(
            using_db=connection, task_key=key, task_type="import", app_id=app_id,
            request_id=request_id, workflow_id=workflow_id, payload=payload,
            status="pending", run_at=utc_now(),
            result={"importStatus": "accepted", "acceptedCount": 0, "rejectedCount": 0,
                    "duplicateCount": 0, "lastRow": 0, "errorFiles": [], "errors": []},
        )
    except IntegrityError as error:
        logger.debug("[Clue-Inflow] 【并发冲突】导入请求唯一键冲突，等待事务回滚: app_id=%r, request_id=%r",
                     app_id, request_id)
        # 并发唯一键冲突由外层事务回滚后查询原任务，不在已失败的事务里继续工作。
        raise RequestConflict(key) from error


class RequestConflict(Exception):
    def __init__(self, key):
        self.key = key


async def duplicate_request_error(key):
    existing = await ExecutionTask.filter(task_key=key).first()
    if existing is None:
        raise RuntimeError("导入任务写入失败，未找到重复任务")
    logger.info("[Clue-Inflow] 【幂等拦截】返回原导入任务: task_id=%s, app_id=%r, request_id=%r, workflow_id=%r",
                existing.id, existing.app_id, existing.request_id, existing.workflow_id)
    return InflowError(40005, "request_id 已受理，请查询原导入任务", 409,
                       {"import_task_id": str(existing.id)})


async def sync_to_clue(clue_info: ClueInfo, request_id: str, *, using_db=None):
    """新增 Clue，返回 (记录, 是否新建)；可加入流入事务或独立调用。

    clue_info 必须已入库。锁住来源行，避免本应用并发同步同一线索时重复创建；
    已存在的 Clue 不覆盖。返回新记录不代表外层事务已经提交。
    """
    if using_db is None:
        async with in_transaction() as connection:
            return await sync_to_clue(clue_info, request_id, using_db=connection)

    source = await ClueInfo.filter(id=clue_info.id).using_db(using_db).select_for_update().get()
    existing = await Clue.filter(clueId=source.clue_id).using_db(using_db).order_by("-id").first()
    if existing:
        logger.debug("[Clue-Inflow] 【同步跳过】Clue已存在，保留原记录: clue_id=%r, request_id=%r, target_id=%s",
                     source.clue_id, request_id, existing.id)
        return existing, False

    # 兼容现有外呼 Agent：表级来源负责回调路由，agentData 中保留业务来源。
    agent_data = dict(source.agent_data or {})
    for key, value in (("customerName", source.customer_name), ("customerPhoneNum", source.phone_number)):
        if not agent_data.get(key) and value:
            agent_data[key] = crypted_manager.decrypt_field(value) if len(value) > 20 else value

    # 手机号/姓名列复用已有密文，避免再次加密。
    values = {
        "requestId": request_id,
        "clueId": source.clue_id,
        "clueSource": "commercial",
        "customerName": source.customer_name,
        "customerPhoneNum": source.phone_number,
        "strategyId": source.strategy_id,
        "agentId": source.agent_id,
        "summaryId": source.agent_id,
        "agentData": json.dumps(agent_data, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
    }
    # 旧表的部分可选字段更短：不截断标识或密文，也不扩充旧表。
    for name, value in list(values.items()):
        limit = getattr(Clue._meta.fields_map[name], "max_length", None)
        if value is not None and limit is not None and len(value) > limit:
            values.pop(name)
            logger.warning("[Clue-Inflow] 【字段跳过】字段超过Clue长度限制，完整值保留在clue_info: clue_id=%r, field=%s, value_length=%s, max_length=%s",
                           source.clue_id, name, len(value), limit)

    try:
        target = await Clue.create(using_db=using_db, **values)
    except Exception as error:
        logger.error("[Clue-Inflow] 【同步失败】Clue记录写入失败: clue_id=%r, request_id=%r, error_type=%s",
                     source.clue_id, request_id, type(error).__name__)
        raise
    logger.info("[Clue-Inflow] 【同步写入】Clue记录已写入，等待事务提交: clue_id=%r, request_id=%r, target_id=%s",
                source.clue_id, request_id, target.id)
    return target, True

async def insert_clue(connection, values, request_id=None):
    clue, created = await ClueInfo.get_or_create(
        using_db=connection, clue_id=values["clue_id"],
        defaults={key: value for key, value in values.items() if key != "clue_id"},
    )
    if created:
        await sync_to_clue(clue, request_id, using_db=connection)
        # 与两张线索表一起提交，保证进程在提交后退出时仍能恢复入池。
        key = hashlib.sha256(json.dumps(["new_clue", clue.workflow_instance_id]).encode()).hexdigest()
        await ExecutionTask.create(
            using_db=connection, task_key=key, task_type="new_clue", clue_id=clue.clue_id,
            app_id=clue.source_app_id, request_id=request_id, workflow_id=clue.workflow_id,
            strategy_id=clue.strategy_id, payload={"importTaskId": clue.import_task_id},
            result={"entryStatus": "pending"}, status="pending", run_at=utc_now(),
        )
    return clue, created


async def claim_task(task_type="import", task_id=None):
    now = utc_now()
    eligible = (Q(status__in=["pending", "retry"], run_at__lte=now)
                | Q(status="processing", locked_until__lte=now))
    async with transaction() as connection:
        query = ExecutionTask.filter(eligible, task_type=task_type)
        if task_id is not None:
            query = query.filter(id=task_id)
        task = await query.using_db(connection).order_by(
            "run_at", "id",
        ).select_for_update().first()
        if task is None:
            return None
        token = uuid.uuid4().hex
        updated = await ExecutionTask.filter(
            id=task.id, status=task.status, lock_token=task.lock_token,
        ).using_db(connection).update(
            status="processing", lock_owner="clue-import", lock_token=token,
            locked_until=now + timedelta(seconds=LEASE_SECONDS),
        )
        if not updated:
            return None
        claimed = await ExecutionTask.filter(id=task.id).using_db(connection).first()
    logger.info("[Clue-Inflow] 【%s】执行任务领取成功: task_type=%s, task_id=%s, app_id=%r, request_id=%r, last_row=%s, retry_count=%s",
                "接管" if task.status == "processing" else "领取", task_type, claimed.id, claimed.app_id,
                claimed.request_id, (claimed.result or {}).get("lastRow", 0), claimed.retry_count)
    return claimed


async def locked_task(connection, task_id, token):
    task = await ExecutionTask.filter(
        id=task_id, status="processing", lock_token=token, locked_until__gt=utc_now(),
    ).using_db(connection).select_for_update().first()
    if task is None:
        logger.info("[Clue-Inflow] 【租约失效】当前执行者停止写入: task_id=%s", task_id)
        raise LeaseLost()
    return task


class LeaseLost(Exception):
    """其他 worker 已接管，当前执行者必须停止提交。"""


async def save_checkpoint(connection, task, *, completed=False):
    task.locked_until = utc_now() + timedelta(seconds=LEASE_SECONDS)
    if completed:
        task.status = "succeeded"
        task.finished_at = utc_now()
        task.lock_token = None
        task.locked_until = None
        task.result = {**task.result, "importStatus": "completed"}
    await task.save(using_db=connection)


async def fail_or_retry(task_id, token, error):
    async with transaction() as connection:
        task = await locked_task(connection, task_id, token)
        permanent = isinstance(error, InflowError)
        if permanent or (task.max_retries is not None and task.retry_count >= task.max_retries):
            task.status = "failed"
            task.finished_at = utc_now()
            status_field = "entryStatus" if task.task_type == "new_clue" else "importStatus"
            task.result = {**task.result, status_field: "failed", "errorCode": error.code if permanent else 50000,
                           "partial": task.result.get("acceptedCount", 0) > 0}
        else:
            task.retry_count += 1
            task.status = "retry"
            task.run_at = utc_now() + timedelta(seconds=min(300, 2 ** task.retry_count))
        task.last_error = error.message if permanent else "任务处理失败，请检查重试结果"
        task.lock_token = None
        task.locked_until = None
        await task.save(using_db=connection)
    logger.log(logging.ERROR if task.status == "failed" else logging.WARNING,
               "[Clue-Inflow] 【%s】执行任务处理结果已提交: task_type=%s, task_id=%s, app_id=%r, request_id=%r, retry_count=%s, run_at=%s, last_row=%s, accepted_count=%s, rejected_count=%s, code=%s, error_type=%s",
               "失败" if task.status == "failed" else "重试", task.task_type, task.id, task.app_id, task.request_id,
               task.retry_count, task.run_at.isoformat(), task.result.get("lastRow", 0),
               task.result.get("acceptedCount", 0), task.result.get("rejectedCount", 0),
               error.code if permanent else 50000, type(error).__name__)
