"""商业化线索受理：提交两表写入后，调用既有新线索handler排期入池。"""
import asyncio
import copy
import hashlib
import json
import logging
import uuid
from datetime import datetime, timedelta

from pydantic import ValidationError

from app.clue_inflow import InflowError, repository
from app.clue_inflow.files import FileRow, FileStore, next_chunk
from app.dbmodels.models import ExecutionTask
from app.schemas.clue_inflow import InflowRecord
from app.schemas.strategy_config import FollowupConfig
from app.utils.decrypt import IT_VERSION_LABEL, crypted_manager

logger = logging.getLogger(__name__)

FIELD_NAMES = {"summary_type": "summaryType", "summary_prompt": "summaryPrompt", "max_followup_hours": "maxFollowupHours",
               "max_calls": "maxCalls", "max_call_per_day": "maxCallPerDay",
               "max_no_answer_calls": "maxNoAnswerCalls", "accessor_id": "accessorId",
               "line_id": "lineId", "mobile_line_id": "mobileLineId"}


def normalize_config(value):
    if isinstance(value, str):
        value = json.loads(value)
    if isinstance(value, list):
        return [normalize_config(item) for item in value]
    if isinstance(value, dict):
        return {FIELD_NAMES.get(key, key): item for key, item in value.items()}
    return value


def response(data, message="success"):
    return {"code": 0, "msg": message, "data": data}


def row_error(number, clue_id, code, reason):
    return {"record_no": number, "clue_id": clue_id, "code": code, "reason": reason[:300]}


class InflowService:
    def __init__(self, storage=None, new_clue_handler=None):
        self.storage = storage or FileStore()
        self.new_clue_handler = new_clue_handler

    async def dispatch_followup(self, import_task):
        """提交后尝试入池；异常不撤销受理，持久任务由后台继续恢复。"""
        from app.clue_inflow.worker import run_entry_once
        try:
            ids = await ExecutionTask.filter(
                task_type="new_clue", app_id=import_task.app_id, request_id=import_task.request_id,
                status__in=["pending", "retry"], run_at__lte=repository.utc_now(),
            ).values_list("id", flat=True)
        except Exception as error:
            logger.error("[Clue-Inflow] 【入池延后】受理已提交，任务查询失败交由后台恢复: import_task_id=%s, error_type=%s",
                         import_task.id, type(error).__name__)
            return
        for task_id in ids:
            try:
                if not await run_entry_once(self, task_id=task_id):
                    break
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.error("[Clue-Inflow] 【入池延后】任务保留，交由后台恢复: task_id=%s, error_type=%s",
                             task_id, type(error).__name__)

    async def resolve_config(self, workflow_id, overrides, allowed_workflows):
        if "*" not in allowed_workflows and workflow_id not in allowed_workflows:
            raise InflowError(40003, "应用无权访问此workflow", 403)
        binding = await repository.get_workflow(workflow_id)
        if binding is None:
            raise InflowError(40004, "未找到对应workflow绑定")
        agent_id = overrides.get("voice_agent_id") or binding["agent_id"]
        strategy_id = overrides.get("followup_strategy_id") or binding["strategy_id"]
        if any(not isinstance(value, str) or not value.strip() for value in (agent_id, strategy_id)):
            raise InflowError(40001, "workflow必须绑定有效的Agent和跟进策略")
        for field, value, default in (("agent_id", agent_id, binding["agent_id"]),
                                      ("strategy_id", strategy_id, binding["strategy_id"])):
            if value != default and not await repository.resource_allowed(field, value, allowed_workflows):
                raise InflowError(40003, "覆盖的Agent或策略未获授权", 403)
        strategy = await repository.get_strategy(strategy_id)
        if not strategy:
            raise InflowError(40001, "绑定的跟进策略不存在")
        try:
            followup = normalize_config(strategy.followup)
            if not isinstance(followup, dict):
                raise ValueError("invalid followup")
            summary_config = FollowupConfig(
                summary_type=followup.get("summaryType"), summary_prompt=followup.get("summaryPrompt"),
            )
            followup["summaryType"] = summary_config.summary_type
            followup["summaryPrompt"] = summary_config.summary_prompt
            for key, default in (("maxFollowupHours", 72), ("maxCalls", 10), ("maxCallPerDay", 3), ("maxNoAnswerCalls", 3)):
                value = followup.setdefault(key, default)
                if type(value) is not int or not 1 <= value <= 100000:
                    raise ValueError("invalid limit")
            webhooks = normalize_config(strategy.webhook or [])
            if isinstance(webhooks, dict):
                webhooks = [webhooks]
            if not isinstance(webhooks, list) or any(
                not isinstance(item, dict) or not isinstance(item.get("url"), str) or not item["url"].strip()
                or not isinstance(item.get("events", []), list)
                or any(not isinstance(event, str) or not event.strip() for event in item.get("events", []))
                for item in webhooks
            ):
                raise ValueError("invalid webhook")
            sip = normalize_config(strategy.sip or {})
            if not isinstance(sip, dict):
                raise ValueError("invalid sip")
            if overrides.get("callback_url"):
                events = sorted({event for item in webhooks for event in item.get("events", [])})
                webhooks = [{"url": overrides["callback_url"], "events": events or [
                    "call_started", "call_ended", "call_scheduled", "clue_ended",
                ]}]
            logger.debug("[Clue-Inflow] 【配置解析】workflow_id=%r, agent_id=%r, strategy_id=%r, config_version=%s",
                         workflow_id, agent_id, strategy_id, strategy.config_version)
            return {"agentId": agent_id, "strategyId": strategy_id,
                    "bindingVersion": binding["binding_version"], "strategyConfigVersion": strategy.config_version,
                    "scheduleId": strategy.schedule_id, "scheduleVersion": strategy.schedule_version,
                    "decisionRuleVersion": strategy.decision_rule_version,
                    "summaryContractVersion": 2,
                    "followup": followup, "sip": sip, "webhook": webhooks,
                    "voiceOverrides": {"ttsId": overrides["tts_id"]} if overrides.get("tts_id") else {},
                    "agentConfigResolved": False, "timezone": "Asia/Shanghai"}
        except (TypeError, ValueError, KeyError) as error:
            raise InflowError(40001, "跟进策略配置不合法，请先修正配置") from error

    def payload(self, mode, raw, snapshot=None):
        encoded = json.dumps(raw, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        return {"schemaVersion": 1, "inflowType": mode,
                "requestHash": hashlib.sha256(encoded.encode()).hexdigest(),
                "receivedAt": repository.utc_now().isoformat(), "baseConfig": snapshot,
                "rawInput": crypted_manager.encrypt_field(encoded, IT_VERSION_LABEL)}

    async def context(self, app_id, request_id, workflow_id, mode, raw, overrides, allowed, evidence=None):
        logger.info("[Clue-Inflow] 【开始】受理线索: inflow_type=%s, app_id=%r, request_id=%r, workflow_id=%r",
                    mode, app_id, request_id, workflow_id)
        await repository.ensure_new_request(app_id, request_id)
        try:
            return await self.resolve_config(workflow_id, overrides, allowed)
        except InflowError as error:
            if error.code == 40004:
                async with repository.transaction() as connection:
                    content = {**self.payload(mode, raw), **(evidence or {})}
                    task = await repository.register_request(connection, app_id, request_id, workflow_id, content)
                    task.status = "quarantined"
                    task.result = {**task.result, "importStatus": "quarantined", "errorCode": 40004}
                    await task.save(using_db=connection)
                error.data = {"import_task_id": str(task.id)}
                logger.warning("[Clue-Inflow] 【隔离】workflow未配置，请求已保存: task_id=%s, app_id=%r, request_id=%r, workflow_id=%r",
                               task.id, app_id, request_id, workflow_id)
            else:
                logger.warning("[Clue-Inflow] 【拒绝】配置解析失败: app_id=%r, request_id=%r, workflow_id=%r, code=%s",
                               app_id, request_id, workflow_id, error.code)
            raise

    def clue_values(self, row, task, overrides=None):
        snapshot = copy.deepcopy(task.payload["baseConfig"])
        received_at = datetime.fromisoformat(task.payload["receivedAt"])
        name = row.agent_data.get("variables", {}).get("name")
        phone = crypted_manager.encrypt_field(row.phone_number, IT_VERSION_LABEL)
        name = crypted_manager.encrypt_field(str(name), IT_VERSION_LABEL) if name is not None else None
        if len(phone) > 512 or (name is not None and len(name) > 512):
            raise InflowError(40001, "加密后的客户信息超过字段长度")
        overrides = overrides or {}
        normalized = {"voice_agent_id": "voiceAgentId", "followup_strategy_id": "followupStrategyId",
                      "tts_id": "ttsId", "callback_url": "callbackUrl"}
        return dict(clue_id=row.clue_id, workflow_instance_id=f"wi_{uuid.uuid4().hex}",
                    source_app_id=task.app_id, external_task_id=row.external_task_id,
                    workflow_id=task.workflow_id, agent_id=snapshot["agentId"], strategy_id=snapshot["strategyId"],
                    import_task_id=task.id, phone_number=phone, customer_name=name, agent_data=row.agent_data,
                    overrides={normalized[key]: value for key, value in overrides.items()} or None,
                    clue_config=snapshot, status="created", received_at=received_at,
                    expires_at=received_at + timedelta(hours=snapshot["followup"]["maxFollowupHours"]))

    async def accept_single(self, app_id, payload, allowed_workflows):
        overrides = payload.overrides.model_dump(exclude_none=True) if payload.overrides else {}
        try:
            snapshot = await self.context(app_id, payload.request_id, payload.workflow_id, "single",
                                          payload.model_dump(), overrides, allowed_workflows)
            async with repository.transaction() as connection:
                task = await repository.register_request(connection, app_id, payload.request_id, payload.workflow_id,
                                                           self.payload("single", payload.model_dump(), snapshot))
                clue, created = await repository.insert_clue(
                    connection, self.clue_values(payload, task, overrides), request_id=task.request_id,
                )
                if not created:
                    logger.info("[Clue-Inflow] 【幂等拦截】线索已存在: app_id=%r, request_id=%r, clue_id=%r",
                                app_id, payload.request_id, payload.clue_id)
                    raise InflowError(40005, "clue_id 已存在，不会覆盖原线索", 409)
                data = {"inflow_type": "single", "workflow_instance_id": clue.workflow_instance_id,
                        "import_task_id": str(task.id), "status": "accepted"}
                task.result = {**task.result, "importStatus": "completed", "acceptedCount": 1,
                               "lastRow": 1, "response": data}
                task.strategy_id = snapshot["strategyId"]
                task.status = "succeeded"
                task.finished_at = repository.utc_now()
                await task.save(using_db=connection)
            logger.info("[Clue-Inflow] 【完成】单条线索受理已提交: task_id=%s, app_id=%r, request_id=%r, workflow_id=%r, clue_id=%r, status=created",
                        task.id, app_id, payload.request_id, payload.workflow_id, clue.clue_id)
            await self.dispatch_followup(task)
            return response(data)
        except repository.RequestConflict as error:
            raise await repository.duplicate_request_error(error.key) from error

    async def accept_batch(self, app_id, payload, allowed_workflows):
        try:
            snapshot = await self.context(app_id, payload.request_id, payload.workflow_id, "batch",
                                          payload.model_dump(), {}, allowed_workflows)
            async with repository.transaction() as connection:
                task = await repository.register_request(connection, app_id, payload.request_id, payload.workflow_id,
                                                           self.payload("batch", payload.model_dump(), snapshot))
                errors = await self.accept_rows(connection, task, [FileRow(i, row) for i, row in enumerate(payload.records, 1)])
                data = {"inflow_type": "batch", "import_task_id": str(task.id),
                        "accepted_count": task.result["acceptedCount"], "rejected_count": task.result["rejectedCount"]}
                task.result = {**task.result, "importStatus": "completed", "response": data, "errors": errors}
                task.strategy_id = snapshot["strategyId"]
                task.status = "succeeded"
                task.finished_at = repository.utc_now()
                await task.save(using_db=connection)
            logger.info("[Clue-Inflow] 【完成】批量线索受理已提交: task_id=%s, app_id=%r, request_id=%r, workflow_id=%r, accepted_count=%s, rejected_count=%s, duplicate_count=%s",
                        task.id, app_id, payload.request_id, payload.workflow_id, task.result["acceptedCount"],
                        task.result["rejectedCount"], task.result["duplicateCount"])
            await self.dispatch_followup(task)
            return response(data, "accepted")
        except repository.RequestConflict as error:
            raise await repository.duplicate_request_error(error.key) from error

    async def accept_rows(self, connection, task, rows):
        errors = []
        counts = dict(task.result)
        for source in rows:
            clue_id = source.value.get("clue_id") if isinstance(source.value, dict) else None
            clue_id = clue_id if isinstance(clue_id, str) and len(clue_id) <= 128 else None
            code, reason = 40001, source.error
            try:
                if source.error:
                    raise InflowError(40006, source.error)
                record = InflowRecord.model_validate(source.value)
                clue, created = await repository.insert_clue(
                    connection, self.clue_values(record, task), request_id=task.request_id,
                )
                if not created:
                    raise InflowError(40005, "clue_id重复，不会覆盖原线索")
                counts["acceptedCount"] += 1
            except ValidationError as error:
                reason = "; ".join(".".join(str(part) for part in item["loc"])+": "+item["msg"]
                                   for item in error.errors(include_input=False, include_url=False))
            except InflowError as error:
                code, reason = error.code, error.message
            else:
                reason = None
            if reason:
                counts["rejectedCount"] += 1
                if code == 40005:
                    counts["duplicateCount"] += 1
                errors.append(row_error(source.number, clue_id, code, reason))
                logger.debug("[Clue-Inflow] 【行拒绝】task_id=%s, record_no=%s, clue_id=%r, code=%s",
                             task.id, source.number, clue_id, code)
            counts["lastRow"] = source.number
        task.result = counts
        return errors

    async def accept_file(self, app_id, payload, allowed_workflows, upload_key=None):
        if bool(upload_key) == bool(payload.file_url):
            raise InflowError(40001, "file与file_url必须二选一")
        if payload.file_url:
            self.storage.validate_url(payload.file_url)
        file_info = {"type": payload.file_type, "objectKey": upload_key, "sha256": payload.file_sha256,
                     "urlCipher": crypted_manager.encrypt_field(payload.file_url, IT_VERSION_LABEL) if payload.file_url else None}
        try:
            snapshot = await self.context(app_id, payload.request_id, payload.workflow_id, "file",
                                          payload.model_dump(), {}, allowed_workflows, {"file": file_info})
            async with repository.transaction() as connection:
                content = self.payload("file", payload.model_dump(), snapshot)
                content["file"] = file_info
                task = await repository.register_request(connection, app_id, payload.request_id, payload.workflow_id, content)
                task.strategy_id = snapshot["strategyId"]
                await task.save(using_db=connection)
            logger.info("[Clue-Inflow] 【完成】文件导入请求已提交，等待异步处理: task_id=%s, app_id=%r, request_id=%r, workflow_id=%r, file_type=%s, source=%s, object_key=%s",
                        task.id, app_id, payload.request_id, payload.workflow_id, payload.file_type,
                        "url" if payload.file_url else "upload", upload_key)
            return response({"inflow_type": "file", "import_task_id": str(task.id), "status": "accepted",
                             "accepted_count": None, "rejected_count": None}, "accepted")
        except repository.RequestConflict as error:
            raise await repository.duplicate_request_error(error.key) from error

    async def own_task(self, app_id, task_id, allowed):
        if (not task_id.isascii() or not task_id.isdigit() or len(task_id) > 19
                or not 0 < int(task_id) <= 9223372036854775807):
            raise InflowError(40001, "导入任务标识不合法")
        task = await ExecutionTask.filter(id=int(task_id), app_id=app_id, task_type="import").first()
        if task is None:
            raise InflowError(404, "导入任务不存在", 404)
        if "*" not in allowed and task.workflow_id not in allowed:
            raise InflowError(40003, "应用无权访问此workflow", 403)
        logger.debug("[Clue-Inflow] 【查询】导入任务访问校验通过: task_id=%s, app_id=%r, workflow_id=%r",
                     task.id, app_id, task.workflow_id)
        return task

    async def get_task(self, app_id, task_id, allowed):
        task = await self.own_task(app_id, task_id, allowed)
        result = task.result or {}
        complete = result.get("importStatus") == "completed"
        file_unknown = task.payload.get("inflowType") == "file" and not complete and result.get("lastRow", 0) == 0
        logger.debug("[Clue-Inflow] 【查询】导入任务状态读取完成: task_id=%s, app_id=%r, import_status=%s, last_row=%s",
                     task.id, app_id, result.get("importStatus", "accepted"), result.get("lastRow", 0))
        return response({"import_task_id": str(task.id), "status": result.get("importStatus", "accepted"),
                         "accepted_count": None if file_unknown else result.get("acceptedCount", 0),
                         "rejected_count": None if file_unknown else result.get("rejectedCount", 0),
                         "duplicate_count": result.get("duplicateCount", 0), "partial": result.get("partial", False),
                         "total_count": result.get("acceptedCount", 0)+result.get("rejectedCount", 0) if complete else None,
                         "error_code": result.get("errorCode"), "msg": task.last_error})

    async def get_errors(self, app_id, task_id, allowed, cursor=0, limit=100):
        task = await self.own_task(app_id, task_id, allowed)
        items = []
        for item in (task.result or {}).get("errors", []):
            if item["record_no"] > cursor:
                items.append(item)
        for key in (task.result or {}).get("errorFiles", []):
            for item in await asyncio.to_thread(self.storage.read_errors, key):
                if item["record_no"] > cursor:
                    items.append(item)
            if len(items) > limit:
                break
        items.sort(key=lambda row: row["record_no"])
        logger.debug("[Clue-Inflow] 【查询】导入错误明细读取完成: task_id=%s, app_id=%r, cursor=%s, returned_count=%s",
                     task.id, app_id, cursor, min(len(items), limit))
        return response({"items": items[:limit], "next_cursor": items[limit-1]["record_no"] if len(items) > limit else None})

    async def process_file(self, claimed):
        file_info = dict(claimed.payload["file"])
        logger.info("[Clue-Inflow] 【开始】处理文件导入: task_id=%s, app_id=%r, request_id=%r, workflow_id=%r, file_type=%s, last_row=%s, retry_count=%s, object_key=%s",
                    claimed.id, claimed.app_id, claimed.request_id, claimed.workflow_id, file_info["type"],
                    (claimed.result or {}).get("lastRow", 0), claimed.retry_count, file_info.get("objectKey"))
        if not file_info.get("objectKey"):
            url = crypted_manager.decrypt_field(file_info["urlCipher"])
            key = await self.storage.fetch(url, file_info["sha256"])
            async with repository.transaction() as connection:
                task = await repository.locked_task(connection, claimed.id, claimed.lock_token)
                task.payload = {**task.payload, "file": {**file_info, "objectKey": key}}
                await repository.save_checkpoint(connection, task)
                claimed = task
            logger.info("[Clue-Inflow] 【文件关联】远程文件引用已提交: task_id=%s, request_id=%r, object_key=%s",
                        claimed.id, claimed.request_id, key)
            file_info["objectKey"] = key
        iterator = self.storage.iter_rows(file_info["objectKey"], file_info["type"], (claimed.result or {}).get("lastRow", 0))
        try:
            while True:
                rows = await asyncio.to_thread(next_chunk, iterator)
                async with repository.transaction() as connection:
                    task = await repository.locked_task(connection, claimed.id, claimed.lock_token)
                    if not rows:
                        if task.result["acceptedCount"]+task.result["rejectedCount"] == 0:
                            raise InflowError(40006, "文件不包含任何线索行")
                        await repository.save_checkpoint(connection, task, completed=True)
                    else:
                        errors = await self.accept_rows(connection, task, rows)
                        if errors:
                            key = await asyncio.to_thread(self.storage.write_errors, errors)
                            task.result = {**task.result, "errorFiles": [*task.result["errorFiles"], key]}
                        task.result = {**task.result, "importStatus": "processing"}
                        await repository.save_checkpoint(connection, task)
                # 提交事务后再记录进度，不能把待提交的计数写成已完成。
                logger.info("[Clue-Inflow] 【%s】文件导入事务已提交: task_id=%s, request_id=%r, last_row=%s, accepted_count=%s, rejected_count=%s, duplicate_count=%s",
                            "进度" if rows else "完成", task.id, task.request_id, task.result["lastRow"],
                            task.result["acceptedCount"], task.result["rejectedCount"], task.result["duplicateCount"])
                if rows:
                    await self.dispatch_followup(task)
                if not rows:
                    return
        finally:
            await asyncio.to_thread(iterator.close)
