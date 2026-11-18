"""呼叫排期、到期派发与执行状态管理；MySQL 为准，Redis 索引可重建。

主流程：schedule_call 创建轮次 → run_once 派发 handler；送号模块使用 prepare/finish 管理执行状态。
ExecutionTask.status 描述送号任务，CallRecord.status 描述真实通话，ClueInfo.status 描述线索。
任务 succeeded 只表示平台受理；通话开始/结束由 callbacks.py 处理。
"""
import copy
import hashlib
import logging
import uuid
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from tortoise.expressions import Q
from tortoise.transactions import in_transaction

from app.schemas.telephony import SubmitResult
from app.call_schedule.contracts import ScheduledCallRequest
from app.call_schedule.dispatch import dispatch_scheduled_call
from app.call_schedule.queue import get_call_queue
from app.config import call_schedule_settings as settings
from app.call_schedule.control import is_scope_paused
from app.dbmodels.models import CallRecord, ClueInfo, ExecutionTask, OutboundStrategy, PauseRecord

logger = logging.getLogger(__name__)
TERMINAL = {"accepted", "rejected", "completed", "cancelled", "failed"}  # 线索终态。
ACTIVE = ("pending", "retry", "processing", "unknown")  # 尚未确认结束的送号任务。


def utc(value):
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def utc_now():
    return datetime.now(timezone.utc)


def frozen_policy(record):
    snapshot = record.config_snapshot
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("config"), dict):
        raise ValueError("invalid call snapshot")
    config = snapshot["config"]
    followup = config.get("followup") or {}
    if not isinstance(followup, dict):
        raise ValueError("invalid followup config")
    for key, default in (("maxCalls", 10), ("maxCallPerDay", 3), ("maxNoAnswerCalls", 3)):
        value = followup.get(key, default)
        if type(value) is not int or not 1 <= value <= 100000:
            raise ValueError("invalid followup limit")
    return config, followup, ZoneInfo(config.get("timezone", "Asia/Shanghai"))


async def install_watchdog(clue_id):
    """送号成功后才启动等待开始回调的状态超时监控。"""
    from app.redis_client.redis_manager import redis_manager
    try:
        await redis_manager.get_delayed_queue().add_task(f"wait_for_calling:{clue_id}", 14400)
    except Exception as error:
        logger.warning("[Call-Schedule] 【监控延后】clue_id=%r, error_type=%s", clue_id, type(error).__name__)


class CallScheduleService:
    """公开入口按业务流程排列，内部检查和状态写入方法放在后半部分。

    跨表写入时统一先锁线索再锁任务；外部配置及网关请求不占用数据库事务。
    未知送号结果只能核对，不能重送。
    """
    def __init__(self, queue=None, *, dispatcher=None, clock=utc_now):
        self.queue = queue
        self.dispatcher = dispatch_scheduled_call if dispatcher is None else dispatcher
        self.clock = clock

    async def schedule_call(self, clue_id, run_at, priority=5, round_no=None):
        """创建或复用同一轮持久任务；重复调用不修改该轮时间和快照。"""
        if type(priority) is not int or not 0 <= priority <= 9999:
            raise ValueError("priority must be an integer from 0 to 9999")
        run_at = utc(run_at)
        async with in_transaction() as connection:
            clue = await ClueInfo.filter(clue_id=clue_id).using_db(connection).select_for_update().get()
            if round_no is not None and (type(round_no) is not int or round_no < 1):
                raise ValueError("round_no must be a positive integer")
            active = await ExecutionTask.filter(task_type="call", clue_id=clue_id,
                status__in=ACTIVE).using_db(connection).first()
            if active:
                if round_no is not None and active.payload["roundNo"] != round_no:
                    raise ValueError("a call round is already active")
                task = active
            else:
                last = await CallRecord.filter(clue_id=clue_id, call_task_id__gt=0).using_db(connection).order_by("-attempt_no").first()
                round_no = round_no or (last.attempt_no+1 if last else 1)
                key = hashlib.sha256(f"call:{clue.workflow_instance_id}:{round_no}".encode()).hexdigest()
                existing = await ExecutionTask.filter(task_key=key).using_db(connection).first()
                if existing:
                    return existing
                if clue.status in TERMINAL or clue.status in ("calling", "wait_for_calling", "paused"):
                    raise ValueError("clue cannot be scheduled in its current state")
                if last and last.status not in ("ended", "failed", "cancelled"):
                    raise ValueError("previous call has not ended")
                if (last and round_no != last.attempt_no+1) or (not last and round_no != 1):
                    raise ValueError("call rounds must be sequential")
                call_id = f"call_{uuid.uuid4().hex}"
                clue.status, clue.next_call_at, clue.last_call_id = "scheduled", run_at, call_id
                clue.state_version += 1
                await clue.save(using_db=connection, update_fields=["status", "next_call_at", "last_call_id", "state_version", "update_time"])
                task = await ExecutionTask.create(using_db=connection, task_key=key, task_type="call",
                    app_id=clue.source_app_id, workflow_id=clue.workflow_id, clue_id=clue_id,
                    call_id=call_id, strategy_id=clue.strategy_id, run_at=run_at,
                    payload={"priority": priority, "roundNo": round_no, "plannedAt": run_at.isoformat(),
                             "stateVersion": clue.state_version}, result={}, status="pending")
                await CallRecord.create(using_db=connection, call_id=call_id, clue_id=clue_id,
                    agent_id=clue.agent_id, strategy_id=clue.strategy_id, call_task_id=task.id,
                    attempt_no=round_no, scheduled_at=run_at,
                    config_snapshot={"config": copy.deepcopy(clue.clue_config),
                        "agentData": copy.deepcopy(clue.agent_data), "phoneNumber": clue.phone_number,
                        "customerName": clue.customer_name,
                        "expiresAt": utc(clue.expires_at).isoformat() if clue.expires_at else None})
        if task.status in ("pending", "retry"):
            await self.publish(task)
        logger.info("[Call-Schedule] 【排期】task_id=%s, clue_id=%r, call_id=%r, round=%s, run_at=%s, priority=%s",
                    task.id, clue_id, task.call_id, task.payload["roundNo"], utc(task.run_at).isoformat(), task.payload["priority"])
        return task

    async def run_once(self):
        """消费一个 Redis 索引；False 表示本次未领取任务，True 不代表送号成功。"""
        queue = await self.get_queue()
        token = uuid.uuid4().hex
        task_id = await queue.reserve(int(self.clock().timestamp()*1000), token,
                                      lease_ms=settings.lease_seconds*1000)
        if task_id is None:
            return False
        try:
            # Redis 只提供待执行索引；handler 决定线路并调用送号模块。
            if await self.claim(task_id, token):
                task = await ExecutionTask.get(id=task_id, task_type="call")
                request = ScheduledCallRequest(clueId=task.clue_id, taskId=task.id, executionToken=token)
                code = "scheduled_handler_did_not_submit"
                try:
                    await self.dispatcher(request)
                except Exception as error:
                    code = "scheduled_handler_exception"
                    logger.error("[Call-Schedule] 【派发异常】task_id=%s, error_type=%s", task_id, type(error).__name__)
                await self._settle_dispatch(task_id, token, code)
            # 先提交DB再删除Redis；崩溃后同一任务再次领取也会被DB状态拦截。
            if await queue.ack(task_id, token):
                task = await ExecutionTask.filter(id=task_id, task_type="call").first()
                if task and task.status in ("pending", "retry"):
                    await self.publish(task)
        except Exception as error:
            # 不 ack，让 Redis/DB 租约到期恢复；绝不吞掉任务。
            logger.error("[Call-Schedule] 【执行异常】等待租约恢复: task_id=%s, error_type=%s", task_id, type(error).__name__)
        return True

    async def _settle_dispatch(self, task_id, token, code):
        """handle 不返回结果也可执行；以送号模块保存的状态为准，未接入则延期。"""
        async with in_transaction() as connection:
            attempt = await self._prepare_attempt_for_submission(task_id, token, connection)
            if attempt is None:
                return
            task, clue, _ = attempt
            await self.defer(task, clue, connection, self.clock()+timedelta(seconds=30), code)

    async def prepare(self, task_id, token, *, clue_id=None, prepare_request=None):
        """轻量恢复检查 → 调用送号侧准备函数 → 一次完整检查 → 标记提交。"""
        async with in_transaction() as connection:
            attempt = await self._recover_or_load_attempt(task_id, token, connection, clue_id=clue_id)
            if attempt is None:
                return None
            _, _, prepared_record = attempt

        # 配置读取、请求组装属于送号模块；调度侧只接收延期原因。
        preparation_error = await prepare_request(prepared_record) if prepare_request is not None else None

        async with in_transaction() as connection:
            # 完整业务检查只做一次，以配置读取后的最新状态为准。
            attempt = await self._prepare_attempt_for_submission(task_id, token, connection)
            if attempt is None:
                return None
            task, clue, record = attempt
            if preparation_error:
                await self.defer(task, clue, connection, self.clock()+timedelta(seconds=30), preparation_error)
                return None
            record.config_snapshot = prepared_record.config_snapshot
            return await self._mark_submission_started(task, clue, record, token, connection)

    async def finish(self, task_id, token, outcome):
        """保存平台受理结果；不代替真实通话的开始/结束回调。"""
        now = self.clock()
        async with in_transaction() as connection:
            source = await ExecutionTask.get(id=task_id).using_db(connection)
            clue = await ClueInfo.filter(clue_id=source.clue_id).using_db(connection).select_for_update().get()
            task = await ExecutionTask.filter(id=task_id, status="processing", lock_token=token,
                locked_until__gt=now).using_db(connection).select_for_update().first()
            if task is None:
                logger.warning("[Call-Schedule] 【租约失效】结果交由接管者核对: task_id=%s", task_id)
                return
            record = await CallRecord.get(call_id=task.call_id).using_db(connection)
            if (outcome.status == "accepted" and outcome.provider_call_id
                    and await CallRecord.filter(clue_id=task.clue_id, provider_call_id=outcome.provider_call_id)
                        .exclude(call_id=task.call_id).using_db(connection).exists()):
                outcome = SubmitResult("unknown", "provider_call_id_conflict")
            submission_status = outcome.status
            task_status = submission_status
            owns_waiting_clue = clue.status == "wait_for_calling" and clue.last_call_id == task.call_id
            record.provider_result_code = outcome.code[:64]
            if submission_status == "accepted":
                if record.status == "submitting":
                    record.status = "submitted"
                record.provider_call_id = outcome.provider_call_id or record.provider_call_id
                task_status = "succeeded"
            elif submission_status == "deferred" or (
                submission_status == "retry"
                and task.retry_count < (task.max_retries if task.max_retries is not None else 5)
            ):
                # 配置未就绪的延期不消耗重试；明确未送达的技术重试保留原退避。
                deferred = submission_status == "deferred"
                if not deferred:
                    task.retry_count += 1
                delay = 30 if deferred else min(300, 2**task.retry_count)
                task.run_at = now+timedelta(seconds=delay)
                record.status, record.submitted_at = "created", None
                if owns_waiting_clue:
                    await self._restore_same_round_schedule(task, clue, connection, task.run_at)
                    task_status = "pending" if deferred else "retry"
                elif deferred:
                    record.status = "cancelled"
                    task_status = "cancelled"
            elif submission_status == "unknown":
                record.status = "unknown"
            else:
                task_status = "failed"
                record.status, record.submitted_at = "failed", None
                if owns_waiting_clue:
                    clue.status, clue.close_reason, clue.closed_at = "rejected", "call_submit_failed", now
                    clue.state_version += 1
                    await clue.save(using_db=connection)
            await record.save(using_db=connection)
            await self.save_task(task, connection, task_status, outcome.code or task_status)
        if task_status == "succeeded":
            await install_watchdog(task.clue_id)

    async def resolve_submission(self, task_id, *, accepted, evidence, provider_call_id=None):
        """仅供可信运维核对后调用；不提供无鉴权HTTP重送入口。"""
        if type(accepted) is not bool or not isinstance(evidence, str) or not evidence.strip() or len(evidence) > 512:
            raise ValueError("confirmed outcome and evidence (1..512 chars) are required")
        async with in_transaction() as connection:
            source = await ExecutionTask.get(id=task_id, task_type="call").using_db(connection)
            clue = await ClueInfo.filter(clue_id=source.clue_id).using_db(connection).select_for_update().get()
            task = await ExecutionTask.filter(id=task_id, status="unknown").using_db(connection).select_for_update().first()
            if task is None:
                raise ValueError("only unknown submissions can be resolved")
            record = await CallRecord.get(call_id=task.call_id).using_db(connection)
            if (provider_call_id and await CallRecord.filter(clue_id=task.clue_id, provider_call_id=provider_call_id)
                    .exclude(call_id=task.call_id).using_db(connection).exists()):
                raise ValueError("provider call id already belongs to another round")
            record.provider_call_id = provider_call_id or record.provider_call_id
            if accepted:
                record.status = "submitted"
                await self.save_task(task, connection, "succeeded", "confirmed_accepted")
            elif clue.status in TERMINAL or clue.last_call_id != task.call_id:
                record.status, record.submitted_at = "cancelled", None
                await self.save_task(task, connection, "cancelled", "confirmed_not_accepted_clue_closed")
            else:
                record.status, record.submitted_at = "created", None
                await self._restore_same_round_schedule(task, clue, connection, self.clock())
                await self.save_task(task, connection, "pending", "confirmed_not_accepted")
            # 只保存核对依据的引用/摘要，不写日志；调用方不得在证据中填写个人信息。
            task.result = {**task.result, "resolutionEvidence": evidence.strip()}
            await task.save(using_db=connection, update_fields=["result", "update_time"])
            await record.save(using_db=connection)
        if task.status == "pending":
            await self.publish(task)
        elif task.status == "succeeded":
            await install_watchdog(task.clue_id)
        return task

    async def delivery_allowed(self, task_id, token):
        """话务查询可能耗时，真正POST送号前再次复核租约、暂停、终态和有效期。"""
        task = await ExecutionTask.filter(id=task_id, status="processing", lock_token=token,
                                          locked_until__gt=self.clock()).first()
        if task is None:
            return False
        async with in_transaction() as connection:
            clue = await ClueInfo.filter(clue_id=task.clue_id).using_db(connection).select_for_update().get()
            if not await ExecutionTask.filter(id=task_id, status="processing", lock_token=token,
                                              locked_until__gt=self.clock()).using_db(connection).exists():
                return False
            zone = ZoneInfo((clue.clue_config or {}).get("timezone", "Asia/Shanghai"))
            if (clue.status != "wait_for_calling" or clue.last_call_id != task.call_id
                    or not 9 <= self.clock().astimezone(zone).hour < 21
                    or (clue.expires_at and utc(clue.expires_at) <= self.clock())):
                return False
            return not await self.paused(clue, task.payload["roundNo"], connection)

    async def reconcile(self, page_size=200):
        """游标分页，不能始终只取第一批；全量恢复未来任务与租约过期任务。"""
        cursor, count = 0, 0
        now = self.clock()
        eligible = Q(status__in=["pending", "retry"]) | Q(status="processing", locked_until__lte=now)
        while True:
            tasks = await ExecutionTask.filter(eligible, task_type="call", id__gt=cursor).order_by("id").limit(page_size)
            if not tasks:
                return count
            for task in tasks:
                if await self.publish(task):
                    count += 1
                else:
                    return count
                cursor = task.id

    async def reconcile_unknown(self, *, query_fn=None, page_size=200):
        """只查询未确认送号；找到本轮任务才能确认，空结果和查询异常均不重送。"""
        if query_fn is None:
            from app.apis.telephony import reconcile_call
            query_fn = reconcile_call
        cursor, confirmed = 0, 0
        while True:
            tasks = await ExecutionTask.filter(task_type="call", status="unknown", id__gt=cursor).order_by("id").limit(page_size)
            if not tasks:
                return confirmed
            for task in tasks:
                cursor = task.id
                record = await CallRecord.filter(call_id=task.call_id).first()
                if record is None:
                    continue
                try:
                    outcome = await query_fn(record)
                    if outcome.status == "accepted":
                        await self.resolve_submission(task.id, accepted=True, evidence="本轮batchId话务查询确认已受理",
                                                      provider_call_id=outcome.provider_call_id)
                        confirmed += 1
                except Exception as error:
                    logger.warning("[Call-Schedule] 【核对延后】task_id=%s, error_type=%s", task.id, type(error).__name__)

    async def get_queue(self):
        return self.queue if self.queue is not None else await get_call_queue()

    async def publish(self, task):
        try:
            queue = await self.get_queue()
            await queue.publish(task.id, int(utc(task.run_at).timestamp()*1000), task.payload["priority"])
            return True
        except Exception as error:
            logger.warning("[Call-Schedule] 【入队延后】持久任务待补偿: task_id=%s, error_type=%s",
                           task.id, type(error).__name__)
            return False

    async def claim(self, task_id, token):
        now = self.clock()
        eligible = Q(status__in=["pending", "retry"], run_at__lte=now) | Q(status="processing", locked_until__lte=now)
        return bool(await ExecutionTask.filter(eligible, id=task_id, task_type="call").update(
            status="processing", lock_owner="call-schedule", lock_token=token,
            locked_until=now+timedelta(seconds=settings.lease_seconds)))

    async def paused(self, clue, round_no, connection):
        queue = await self.get_queue()
        scope = "inflow" if round_no == 1 else "schedule"
        # 保持既有控制范围：inflow管首呼，schedule管次呼，all设置两个标志。
        if await is_scope_paused(scope, client=queue.client, connection=connection, now=self.clock()):
            return True
        if await OutboundStrategy.filter(strategy_id=clue.strategy_id).exclude(status="active").using_db(connection).exists():
            return True
        now = self.clock()
        return await PauseRecord.filter(Q(paused_until=None) | Q(paused_until__gt=now),
            scope_type="strategy", target_id=clue.strategy_id, resumed_at=None,
            paused_at__lte=now).using_db(connection).exists()

    async def _recover_or_load_attempt(self, task_id, token, connection, *, clue_id=None):
        """只核对租约和本轮送号状态；恢复已送出/结果不明任务，避免再次送号。"""
        # 统一先锁线索再锁任务，与生产者顺序一致。
        source = await ExecutionTask.filter(id=task_id).using_db(connection).get()
        if clue_id is not None and source.clue_id != clue_id:
            return None
        clue = await ClueInfo.filter(clue_id=source.clue_id).using_db(connection).select_for_update().get()
        now = self.clock()
        task = await ExecutionTask.filter(id=task_id, status="processing", lock_token=token,
            locked_until__gt=now).using_db(connection).select_for_update().first()
        if task is None:
            return None
        record = await CallRecord.filter(call_id=task.call_id).using_db(connection).get()
        # DB 可能已提交送号结果而 Redis ack 未完成；不能再调用网关。
        if record.status in ("submitted", "ringing", "connected", "ended"):
            await self.save_task(task, connection, "succeeded", "already_submitted")
            return None
        if record.status in ("submitting", "unknown"):
            record.status = "unknown"
            await record.save(using_db=connection, update_fields=["status", "update_time"])
            await self.save_task(task, connection, "unknown", "submission_requires_reconciliation")
            return None
        return task, clue, record

    async def _prepare_attempt_for_submission(self, task_id, token, connection):
        """配置读取后完成一次业务检查，落库取消/延期；None 表示不能送号。"""
        attempt = await self._recover_or_load_attempt(task_id, token, connection)
        if attempt is None:
            return None
        task, clue, record = attempt
        now = self.clock()
        # 只读检查给出原因；取消本轮及关闭线索的状态写入集中在这里。
        reason = await self._get_cancellation_reason(task, clue, record, connection, now)
        if reason:
            record.status = "failed" if reason in ("strategy_missing", "invalid_runtime_config") else "cancelled"
            await record.save(using_db=connection, update_fields=["status", "update_time"])
            await self.save_task(task, connection, record.status, reason)
            closes_clue = reason in (
                "clue_expired", "max_calls", "max_no_answer_calls",
                "strategy_missing", "invalid_runtime_config",
            )
            if closes_clue and clue.status not in TERMINAL:
                clue.status, clue.close_reason, clue.closed_at = "rejected", reason, now
                clue.next_call_at = None
                clue.state_version += 1
                await clue.save(using_db=connection)
            return None
        if clue.status == "paused" or await self.paused(clue, task.payload["roundNo"], connection):
            await self.defer(task, clue, connection, now+timedelta(seconds=settings.pause_delay_seconds), "paused")
            return None
        if clue.status not in ("scheduled", "wait_for_calling"):
            await self.save_task(task, connection, "cancelled", "clue_state_changed")
            record.status = "cancelled"
            await record.save(using_db=connection, update_fields=["status", "update_time"])
            return None
        next_run_at = await self._get_required_deferral(record, connection, now)
        if next_run_at is not None:
            await self.defer(task, clue, connection, next_run_at, "window_or_daily_limit")
            return None
        return task, clue, record

    async def _get_cancellation_reason(self, task, clue, record, connection, now):
        """只读检查：返回本轮应取消/失败的原因；None 表示通过。"""
        if clue.status in TERMINAL:
            return "clue_closed"
        if task.payload["stateVersion"] != clue.state_version and clue.status != "wait_for_calling":
            return "stale_schedule"
        if clue.expires_at and utc(clue.expires_at) <= now:
            return "clue_expired"
        if not await OutboundStrategy.filter(strategy_id=clue.strategy_id).using_db(connection).exists():
            return "strategy_missing"
        try:
            _, followup, _ = frozen_policy(record)
        except (ValueError, KeyError, TypeError):
            return "invalid_runtime_config"
        submitted_calls = CallRecord.filter(
            clue_id=clue.clue_id, submitted_at__not_isnull=True,
        ).exclude(call_id=record.call_id).using_db(connection)
        if max(clue.total_call_count, await submitted_calls.count()) >= followup.get("maxCalls", 10):
            return "max_calls"
        if clue.no_answer_count >= followup.get("maxNoAnswerCalls", 3):
            return "max_no_answer_calls"
        return None

    async def _get_required_deferral(self, record, connection, now):
        """只读检查：超出工作窗口/当日次数限制时返回延期时间，否则返回 None。"""
        _, followup, zone = frozen_policy(record)  # 已通过配置和总次数检查。
        local = now.astimezone(zone)
        opening = datetime.combine(local.date(), time(9), zone)
        next_opening = opening if local.hour < 9 else opening+timedelta(days=1)
        day_start = datetime.combine(local.date(), time.min, zone)
        today_calls = await CallRecord.filter(
            clue_id=record.clue_id,
            submitted_at__gte=day_start.astimezone(timezone.utc),
            submitted_at__lt=(day_start+timedelta(days=1)).astimezone(timezone.utc),
        ).exclude(call_id=record.call_id).using_db(connection).count()
        if not 9 <= local.hour < 21 or today_calls >= followup.get("maxCallPerDay", 3):
            return next_opening.astimezone(timezone.utc)
        # Global Outbox Pacer: 跨业务线同客户手机号 24h 总触达防骚扰检查
        max_touches_24h = followup.get("maxTouchesPer24h")
        if isinstance(max_touches_24h, int) and max_touches_24h > 0:
            clue_row = await ClueInfo.filter(clue_id=record.clue_id).using_db(connection).first()
            if clue_row and clue_row.phone_number and clue_row.phone_number != "13800000000":
                same_phone_clues = await ClueInfo.filter(phone_number=clue_row.phone_number).using_db(connection).values_list("clue_id", flat=True)
                if same_phone_clues:
                    since_24h = now - timedelta(hours=24)
                    touches = await CallRecord.filter(
                        clue_id__in=list(same_phone_clues),
                        submitted_at__gte=since_24h,
                    ).exclude(call_id=record.call_id).using_db(connection).count()
                    if touches >= max_touches_24h:
                        return next_opening.astimezone(timezone.utc)
        return None

    async def _mark_submission_started(self, task, clue, record, token, connection):
        """HTTP 前保存 submitting/wait_for_calling；后续中断按未知结果恢复。"""
        record.status, record.submitted_at = "submitting", self.clock()
        await record.save(using_db=connection, update_fields=["status", "submitted_at", "config_snapshot", "update_time"])
        clue.status, clue.last_call_id, clue.next_call_at = "wait_for_calling", task.call_id, None
        clue.state_version += 1
        await clue.save(using_db=connection, update_fields=["status", "last_call_id", "next_call_at", "state_version", "update_time"])
        task.payload = {**task.payload, "stateVersion": clue.state_version}
        await task.save(using_db=connection, update_fields=["payload", "update_time"])
        logger.info("[Call-Schedule] 【送号开始】task_id=%s, clue_id=%r, call_id=%r", task.id, task.clue_id, task.call_id)
        record.before_send = lambda: self.delivery_allowed(task.id, token)
        return record

    async def save_task(self, task, connection, status, code):
        """只保存送号任务结果并释放 DB 租约，不修改线索或通话状态。"""
        task.status = status
        task.result = {"code": code}
        task.lock_token, task.locked_until, task.lock_owner = None, None, None
        if status in ("succeeded", "cancelled", "failed", "unknown"):
            task.finished_at = self.clock()
        await task.save(using_db=connection)
        logger.log(logging.WARNING if status in ("failed", "unknown") else logging.INFO,
                   "[Call-Schedule] 【执行结果】task_id=%s, clue_id=%r, call_id=%r, status=%s, code=%s, retry_count=%s",
                   task.id, task.clue_id, task.call_id, status, code, task.retry_count)

    async def defer(self, task, clue, connection, run_at, code):
        """推迟尚未送出的任务，保留线索当前状态、版本和技术重试次数。"""
        task.run_at = run_at
        clue.next_call_at = run_at
        await clue.save(using_db=connection, update_fields=["next_call_at", "update_time"])
        await self.save_task(task, connection, "pending", code)

    async def _restore_same_round_schedule(self, task, clue, connection, run_at):
        """已确认未送达后恢复同一轮次，统一维护排期时间和状态版本。"""
        task.run_at, task.finished_at = run_at, None
        clue.status, clue.next_call_at = "scheduled", run_at
        clue.state_version += 1
        task.payload = {**task.payload, "stateVersion": clue.state_version}
        await clue.save(using_db=connection)


async def schedule_call(clue_id, run_at, priority=5, round_no=None):
    return await CallScheduleService().schedule_call(clue_id, run_at, priority, round_no)
