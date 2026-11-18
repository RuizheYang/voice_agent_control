"""送号 API：接收本轮任务与座机/手机线路，组装请求、提交并保存结果。"""
import asyncio
import copy
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, time, timezone
from enum import Enum
from time import perf_counter
from typing import Any
from urllib.parse import unquote, urlsplit
from zoneinfo import ZoneInfo

import aiohttp

from app.call_schedule.contracts import ScheduledCallRequest
from app.schemas.telephony import SubmitResult, TelephonyAPI

logger = logging.getLogger(__name__)


class LineType(str, Enum):
    LANDLINE = "landline"
    MOBILE = "mobile"


async def send_call(request: ScheduledCallRequest, *, line_type: LineType,
                    scheduler=None, gateway=None, agent_config_loader=None) -> SubmitResult | None:
    """进程内顺序 await；只有持有本轮有效租约的请求才能送号。

    handler 只需传入原始请求和线路；其余关键字是测试依赖注入入口。
    None 表示取消、延期、租约失效或该轮已提交，禁止由 handler 自行重送。
    """
    from app.call_schedule.service import CallScheduleService
    from app.config import call_schedule_settings as settings

    request = ScheduledCallRequest.model_validate(request, from_attributes=True)
    line_type = LineType(line_type)
    scheduler = scheduler if scheduler is not None else CallScheduleService()
    loader = agent_config_loader or load_agent_config

    async def prepare_request(record):
        try:
            config = record.config_snapshot["config"]
            agent_config = record.config_snapshot.get("agentConfig") or config.get("agentConfig")
            if not record.config_snapshot.get("telephonyRequest") and agent_config is None:
                agent_config = await asyncio.wait_for(
                    loader(record.agent_id, copy.deepcopy(config.get("sip") or {})),
                    timeout=settings.http_timeout_seconds)
            freeze_telephony_request(record, agent_config, now=scheduler.clock(), line_type=line_type)
        except AgentConfigurationPending as error:
            return str(error)
        except Exception as error:
            logger.warning("[Telephony] 【参数准备延后】task_id=%s, error_type=%s",
                           request.taskId, type(error).__name__)
            return "agent_config_unavailable"
        return None

    record = await scheduler.prepare(request.taskId, request.executionToken, clue_id=request.clueId,
                                     prepare_request=prepare_request if gateway is None else None)
    if record is None:
        return None
    try:
        outcome = await (submit_call if gateway is None else gateway)(record)
    except Exception as error:
        logger.error("[Telephony] 【送号结果不明】task_id=%s, error_type=%s", request.taskId, type(error).__name__)
        outcome = SubmitResult("unknown", "gateway_exception")
    # ClueHandler.on_scheduled_task 当前不透传返回值，因此在这里通过调度服务持久化结果。
    await scheduler.finish(request.taskId, request.executionToken, outcome)

    # 触发内部事件跟踪：VoiceAgentCallRequestEvent (记录发往外呼平台的全部送号参数与受理状态)
    try:
        from app.observability.events_bus import event_bus, VoiceAgentCallRequestEvent
        from app.utils.decrypt import crypted_manager

        snapshot = getattr(record, "config_snapshot", {}) or {}
        tele_raw = crypted_manager.decrypt_field(snapshot.get("telephonyRequest", "")) if snapshot.get("telephonyRequest") else ""
        tele_dict = json.loads(tele_raw) if tele_raw else {}
        phone_item = (tele_dict.get("phoneList") or [{}])[0]
        raw_phone = phone_item.get("calledPhone", "")
        if not raw_phone and snapshot.get("phoneNumber"):
            try:
                raw_phone = crypted_manager.decrypt_field(snapshot["phoneNumber"])
            except Exception:
                raw_phone = ""
        masked_phone = f"{raw_phone[:3]}****{raw_phone[-4:]}" if len(raw_phone) >= 7 else "***"

        call_params = {}
        if phone_item.get("callParams"):
            try:
                call_params = json.loads(phone_item["callParams"])
            except Exception:
                pass

        tele_meta = snapshot.get("telephonyMetadata") or {}
        agent_ver = tele_meta.get("agentVersion") or (snapshot.get("agentConfig") or {}).get("version")

        await event_bus.emit(VoiceAgentCallRequestEvent(
            clue_id=record.clue_id,
            call_id=record.call_id,
            event_data={
                "task_id": request.taskId,
                "clue_id": record.clue_id,
                "call_id": record.call_id,
                "agent_id": record.agent_id,
                "agent_version": agent_ver,
                "line_type": line_type.value,
                "accessor_id": tele_dict.get("accessorId"),
                "strategy_id": tele_dict.get("strategyId"),
                "speechcraft_id": tele_dict.get("speechcraftId"),
                "trade_no": tele_dict.get("tradeNo") or tele_meta.get("tradeNo"),
                "batch_id": tele_dict.get("batchId") or tele_meta.get("batchId"),
                "timestamp": tele_dict.get("timestamp"),
                "exec_start_time": tele_dict.get("execStartTime"),
                "exec_end_time": tele_dict.get("execEndTime"),
                "exec_limit": tele_dict.get("execLimit"),
                "called_phone_masked": masked_phone,
                "call_params": call_params,
                "submit_status": outcome.status if outcome else None,
                "submit_code": outcome.code if outcome else None,
                "provider_call_id": outcome.provider_call_id if outcome else None,
            }
        ))
    except Exception as ev_err:
        logger.warning("[Telephony] 触发 VoiceAgentCallRequestEvent 异常 (已降级): %s", ev_err)

    return outcome


class AgentConfigurationPending(Exception):
    """Agent配置来源或必要的话务映射未准备好，任务应延期而不是结束线索。"""


def _validate_agent_config(agent_id, agent_config):
    """校验送号需要的Agent配置，返回明确配置的speechcraftId。"""
    if not isinstance(agent_config, dict) or agent_config.get("agent_id") != agent_id:
        raise AgentConfigurationPending("agent_config_missing")
    version = agent_config.get("version")
    if not isinstance(version, str) or not version.strip():
        raise AgentConfigurationPending("agent_version_missing")
    if agent_config.get("status", "active") != "active":
        raise AgentConfigurationPending("agent_inactive")
    if agent_config.get("call_type", "outbound") != "outbound":
        raise AgentConfigurationPending("agent_not_outbound")
    settings = agent_config.get("global_settings")
    tts = settings.get("tts") if isinstance(settings, dict) else None
    speechcraft_id = tts.get("provider_id") if isinstance(tts, dict) else None
    if not isinstance(speechcraft_id, str) or not speechcraft_id.strip():
        raise AgentConfigurationPending("agent_tts_provider_pending")
    return speechcraft_id


async def load_agent_config(agent_id, sip):
    """按ID读取VoiceAgent；sip保留加载器接口，线路仍由策略快照提供。"""
    from app.dbmodels.crud import get_voice_agent_config_by_id

    logger.debug("[Telephony] 【配置读取】按Agent ID查询: agent_id=%r", agent_id)
    agent = await get_voice_agent_config_by_id(agent_id)
    config = None if agent is None else {
        "agent_id": agent.agent_id, "agent_name": agent.agent_name,
        "version": agent.version, "description": agent.description, "status": agent.status,
        "global_settings": copy.deepcopy(agent.globalSettings),
        "agent_type": agent.agent_type, "open_message": agent.open_message, "call_type": agent.call_type,
    }
    try:
        _validate_agent_config(agent_id, config)
    except AgentConfigurationPending as error:
        logger.warning("[Telephony] 【配置等待】Agent配置暂不可用: agent_id=%r, code=%s", agent_id, str(error))
        raise
    logger.info("[Telephony] 【配置就绪】Agent配置已读取: agent_id=%r, agent_version=%r", agent_id, config["version"])
    return config


def batch_id_for_call(call_id):
    return hashlib.sha256(f"batch:{call_id}".encode()).hexdigest()[:32]


def as_utc(value):
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def build_telephony_params(record, agent_config, *, now, line_type: LineType):
    line_type = LineType(line_type)
    speechcraft_id = _validate_agent_config(record.agent_id, agent_config)
    snapshot = record.config_snapshot
    config = snapshot["config"]
    sip = config.get("sip") or {}
    data = copy.deepcopy(snapshot.get("agentData") or {})
    # 线路由 scheduled 显式指定；送号模块只负责映射平台线路ID。
    use_mobile = line_type == LineType.MOBILE
    requested_line = sip.get("mobileLineId") if use_mobile else sip.get("lineId")
    if use_mobile and not requested_line:
        raise AgentConfigurationPending("mobile_line_missing")
    if not isinstance(sip.get("accessorId"), str) or not sip["accessorId"].strip():
        raise AgentConfigurationPending("sip_accessor_missing")
    if not isinstance(requested_line, str) or not requested_line.strip():
        raise AgentConfigurationPending("sip_line_missing")
    from app.utils.decrypt import crypted_manager
    phone = crypted_manager.decrypt_field(snapshot["phoneNumber"])
    local = as_utc(now).astimezone(ZoneInfo(config.get("timezone", "Asia/Shanghai")))
    end = datetime.combine(local.date(), time(21), local.tzinfo)
    if snapshot.get("expiresAt"):
        end = min(end, as_utc(datetime.fromisoformat(snapshot["expiresAt"])).astimezone(local.tzinfo))
    if not 9 <= local.hour < 21 or end <= local:
        raise ValueError("outside call window")
    context = {"clueId": record.clue_id, "callId": record.call_id, "batchId": batch_id_for_call(record.call_id),
               "agentId": record.agent_id,
               "agentVersion": agent_config["version"], "agentData": data,
               "overrides": copy.deepcopy(config.get("voiceOverrides") or {})}
    return TelephonyAPI(
        accessorId=sip["accessorId"],
        strategyId=requested_line, speechcraftId=speechcraft_id,
        tradeNo=hashlib.sha256(f"trade:{record.call_id}".encode()).hexdigest()[:32],
        batchId=batch_id_for_call(record.call_id), timestamp=local.strftime("%Y%m%d%H%M%S%f")[:-3],
        execStartTime=local.strftime("%Y-%m-%d %H:%M:%S"),
        execEndTime=end.strftime("%Y-%m-%d %H:%M:%S"), execLimit=40,
        phoneList=[{"calledPhone": phone, "businessId": record.clue_id,
                    "callParams": json.dumps(context, ensure_ascii=False, separators=(",", ":"), allow_nan=False)}],
    )


def freeze_telephony_request(record, agent_config, *, now, line_type: LineType):
    """调用方与submitting状态在同一DB事务保存此快照，HTTP前不另开写事务。"""
    from app.utils.decrypt import crypted_manager, IT_VERSION_LABEL
    line_type = LineType(line_type)
    if record.config_snapshot.get("telephonyRequest"):
        previous = TelephonyAPI.model_validate(json.loads(crypted_manager.decrypt_field(record.config_snapshot["telephonyRequest"])))
        previous_type = record.config_snapshot.get("telephonyMetadata", {}).get("lineType")
        if previous_type is not None:
            if previous_type != line_type.value:
                raise AgentConfigurationPending("frozen_line_type_mismatch")
        else:
            # 兼容升级前快照，仍以已固化的平台线路为准，不能借重试切线。
            sip = record.config_snapshot["config"].get("sip") or {}
            requested_line = sip.get("mobileLineId" if line_type == LineType.MOBILE else "lineId")
            if previous.strategyId != requested_line:
                raise AgentConfigurationPending("frozen_line_type_mismatch")
        zone = ZoneInfo(record.config_snapshot["config"].get("timezone", "Asia/Shanghai"))
        end = datetime.strptime(previous.execEndTime, "%Y-%m-%d %H:%M:%S").replace(tzinfo=zone) if previous.execEndTime else None
        if end is None or end > as_utc(now):
            return
        if getattr(record, "status", "created") != "created":
            raise AgentConfigurationPending("previous_submission_unconfirmed")
        # 查询前失败或确认未送达后可跨日刷新执行窗，沿用已固化Agent和线路，绝不处理unknown。
        agent_config = record.config_snapshot.get("agentConfig") or agent_config
    params = build_telephony_params(record, agent_config, now=now, line_type=line_type)
    encoded = json.dumps(params.model_dump(exclude_none=True), ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    record.config_snapshot = {**record.config_snapshot,
        "agentConfig": copy.deepcopy(agent_config),
        "telephonyRequest": crypted_manager.encrypt_field(encoded, IT_VERSION_LABEL),
        "telephonyMetadata": {"batchId": params.batchId, "tradeNo": params.tradeNo,
                               "agentVersion": agent_config["version"], "lineType": line_type.value}}


@dataclass(frozen=True)
class CallTaskStatus:
    status: str  # found / absent / unknown
    provider_call_id: str | None = None


def classify_response(status, body):
    if status == 429:
        return SubmitResult("retry", "rate_limited")
    if status in (400, 401, 403, 404, 405, 422):
        return SubmitResult("failed", f"http_{status}")
    if not 200 <= status < 300 or not isinstance(body, dict) or type(body.get("code")) is not int:
        return SubmitResult("unknown", f"http_{status}")
    if body["code"] != 0:
        return SubmitResult("failed", f"provider_{body['code']}"[:64])
    data = body.get("data")
    if not isinstance(data, dict):
        return SubmitResult("accepted")
    ids = [data[key] for key in ("providerCallId", "callId") if key in data and data[key] is not None]
    if any(not isinstance(value, str) or not value.strip() or len(value) > 128 for value in ids):
        return SubmitResult("unknown", "invalid_provider_call_id")
    if len(set(ids)) > 1:
        return SubmitResult("unknown", "ambiguous_provider_call_id")
    return SubmitResult("accepted", provider_call_id=ids[0] if ids else None)


async def check_call_task_status(session, tele_params: TelephonyAPI, clueId: str, *, url=None):
    if url is None:
        from app.config import config
        url = config.call_request_config.send_phone_status_url
    if not url:
        return CallTaskStatus("unknown")
    phone = tele_params.phoneList[0]
    payload = {"accessorId": tele_params.accessorId, "businessId": phone.businessId,
               "batchId": tele_params.batchId, "calledPhone": phone.calledPhone}
    logger.info("[Telephony] 【查询开始】clue_id=%r, batch_id=%r", clueId, tele_params.batchId)
    try:
        async with session.post(url=url, json=payload, allow_redirects=False) as response:
            data = await response.json(content_type=None)
            if (not 200 <= response.status < 300 or not isinstance(data, dict)
                    or type(data.get("code")) is not int or data["code"] != 0
                    or not isinstance(data.get("data"), dict)):
                return CallTaskStatus("unknown")
            rows = data["data"].get("aiTaskList")
            if not isinstance(rows, list):
                return CallTaskStatus("unknown")
            if not rows:
                logger.info("[Telephony] 【查询完成】本轮暂无送号记录: clue_id=%r, batch_id=%r", clueId, tele_params.batchId)
                return CallTaskStatus("absent")
            if len(rows) != 1 or not isinstance(rows[0], dict) or not rows[0]:
                return CallTaskStatus("unknown")
            row = rows[0]
            if any(key in row and row[key] != value for key, value in payload.items()):
                return CallTaskStatus("unknown")
            provider_id = row.get("callId")
            if provider_id is not None and (not isinstance(provider_id, str) or not provider_id.strip() or len(provider_id) > 128):
                return CallTaskStatus("unknown")
            # 状态不为1也不能证明本轮未拨打：已完成任务同样禁止再次送号。
            logger.info("[Telephony] 【查询完成】本轮已存在话务任务: clue_id=%r, batch_id=%r", clueId, tele_params.batchId)
            return CallTaskStatus("found", provider_id)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        logger.warning("[Telephony] 【查询失败】保留未知状态: clue_id=%r, error_type=%s", clueId, type(error).__name__)
        return CallTaskStatus("unknown")


async def _make_call(session, url, tele_params, clue_id, status_url, before_send):
    query = await check_call_task_status(session, tele_params, clue_id, url=status_url)
    if query.status == "found":
        logger.info("[Telephony] 【送号跳过】平台已存在本轮任务: clue_id=%r, batch_id=%r, "
                    "status=accepted, code=already_exists, provider_call_id=%r",
                    clue_id, tele_params.batchId, query.provider_call_id)
        return SubmitResult("accepted", "already_exists", query.provider_call_id)
    if query.status != "absent":
        # 查询阶段尚未尝试送号，可以延期再查询，不应消耗重试或结束线索。
        logger.info("[Telephony] 【送号未发起】查询结果不明，等待下次核对: "
                    "clue_id=%r, batch_id=%r, status=deferred, code=status_query_unknown",
                    clue_id, tele_params.batchId)
        return SubmitResult("deferred", "status_query_unknown")
    if before_send is not None and not await before_send():
        logger.info("[Telephony] 【送号未发起】暂停或租约校验未通过: "
                    "clue_id=%r, batch_id=%r, status=deferred, code=submission_gate_closed",
                    clue_id, tele_params.batchId)
        return SubmitResult("deferred", "submission_gate_closed")
    payload = tele_params.model_dump(exclude_none=True)
    started_at = perf_counter()
    logger.info("[Telephony] 【送号请求】clue_id=%r, batch_id=%r, trade_no=%r, url=%s, request_body=%s",
                clue_id, tele_params.batchId, tele_params.tradeNo, url,
                json.dumps(payload, ensure_ascii=False))
    try:
        async with session.post(url=url, json=payload, allow_redirects=False) as response:
            raw_body = await response.text()
            logger.info("[Telephony] 【送号响应】clue_id=%r, batch_id=%r, url=%s, "
                        "http_status=%s, elapsed_ms=%.1f, response_body=%s",
                        clue_id, tele_params.batchId, url, response.status,
                        (perf_counter() - started_at) * 1000, raw_body)
            try:
                body = json.loads(raw_body)
            except ValueError:
                body = None
            result = classify_response(response.status, body)
    except aiohttp.ClientConnectorError as error:
        logger.warning("[Telephony] 【送号异常】连接未建立: clue_id=%r, batch_id=%r, url=%s, "
                       "error_type=%s, error=%s", clue_id, tele_params.batchId, url, type(error).__name__, error)
        result = SubmitResult("retry", "connection_failed")
    except (asyncio.TimeoutError, aiohttp.ClientError, UnicodeError) as error:
        logger.warning("[Telephony] 【送号异常】未取得完整响应，需查询确认: "
                       "clue_id=%r, batch_id=%r, url=%s, error_type=%s, error=%s",
                       clue_id, tele_params.batchId, url, type(error).__name__, error)
        result = SubmitResult("unknown", "transport_unknown")
    result_message = {"accepted": "平台已受理", "failed": "平台明确拒绝",
                      "retry": "允许后续重试", "unknown": "结果不明，需查询确认"}[result.status]
    logger.info("[Telephony] 【送号结果】clue_id=%r, batch_id=%r, trade_no=%r, status=%s, "
                "code=%s, provider_call_id=%r, elapsed_ms=%.1f, message=%s",
                clue_id, tele_params.batchId, tele_params.tradeNo, result.status, result.code,
                result.provider_call_id, (perf_counter() - started_at) * 1000, result_message)
    return result


async def make_call_api(url: str, tele_params: TelephonyAPI, clueId: str, *, status_url=None,
                        session=None, timeout_seconds=5, before_send=None):
    """返回结构化结果；不在HTTP封装内叠加重试、不发送旧业务转人工回调。"""
    if status_url is None:
        from app.config import config
        status_url = config.call_request_config.send_phone_status_url
    if not url or not status_url:
        logger.warning("[Telephony] 【送号未发起】缺少送号或查询地址: clue_id=%r, "
                       "status=deferred, code=telephony_endpoint_missing", clueId)
        return SubmitResult("deferred", "telephony_endpoint_missing")
    if session is not None:
        return await _make_call(session, url, tele_params, clueId, status_url, before_send)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_seconds)) as client:
        return await _make_call(client, url, tele_params, clueId, status_url, before_send)


def build_payload(record):
    from app.utils.decrypt import crypted_manager
    encrypted = record.config_snapshot.get('telephonyRequest')
    if not encrypted:
        raise ValueError('native telephony request not prepared')
    body = TelephonyAPI.model_validate(json.loads(crypted_manager.decrypt_field(encrypted))).model_dump(exclude_none=True)
    context = json.loads(body['phoneList'][0]['callParams'])
    if (context.get('callId') != record.call_id or context.get('agentId') != record.agent_id
            or context.get('clueId') != record.clue_id or body['phoneList'][0]['businessId'] != record.clue_id):
        raise ValueError('frozen request identity mismatch')
    return body


def get_sender_url():
    from app.config import config
    return os.getenv('TELEPHONY_SEND_URL', config.call_request_config.call_request_url).strip()


def get_status_url():
    from app.config import config
    return os.getenv('TELEPHONY_STATUS_URL', config.call_request_config.send_phone_status_url).strip()


def valid_endpoint(url: str, suffix: str = "") -> bool:
    try:
        parsed = urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            return False
        if suffix:
            return unquote(parsed.path).rstrip("/").endswith(suffix)
        return True
    except ValueError:
        return False


class BaseTelephonyGateway:
    """开放话务网关抽象协议：支持本地 Mock 沙箱或标准 HTTP Webhook 外呼平台。"""

    async def submit_call(self, record: Any) -> SubmitResult:
        raise NotImplementedError

    async def reconcile_call(self, record: Any) -> SubmitResult:
        raise NotImplementedError


class MockTelephonyGateway(BaseTelephonyGateway):
    """本地沙箱模拟网关：无需真实语音外呼线路即可端到端跑通调度闭环。"""

    async def submit_call(self, record: Any) -> SubmitResult:
        call_id = getattr(record, "call_id", "mock_call")
        clue_id = getattr(record, "clue_id", "unknown")
        provider_id = f"mock_{call_id}"
        logger.info(
            "[MockTelephonyGateway] 【模拟送号成功】clue_id=%r, call_id=%r, provider_call_id=%r",
            clue_id,
            call_id,
            provider_id,
        )
        return SubmitResult("accepted", "mock_accepted", provider_call_id=provider_id)

    async def reconcile_call(self, record: Any) -> SubmitResult:
        call_id = getattr(record, "call_id", "mock_call")
        return SubmitResult("accepted", "mock_confirmed", provider_call_id=f"mock_{call_id}")


class GenericWebhookGateway(BaseTelephonyGateway):
    """通用 HTTP Webhook 外呼网关：适配任意支持 JSON 回调的语音外呼平台。"""

    async def submit_call(self, record: Any) -> SubmitResult:
        from app.config import call_schedule_settings as settings

        url, status_url = get_sender_url(), get_status_url()
        if not url:
            logger.warning(
                "[Telephony] 【送号未发起】缺少送号地址: clue_id=%r, status=deferred, code=telephony_endpoint_missing",
                getattr(record, "clue_id", None),
            )
            return SubmitResult("deferred", "telephony_endpoint_missing")
        if not valid_endpoint(url) or (status_url and not valid_endpoint(status_url)):
            logger.warning(
                "[Telephony] 【送号未发起】无效的话务网关 URL: clue_id=%r, url=%s, status_url=%s",
                getattr(record, "clue_id", None),
                url,
                status_url,
            )
            return SubmitResult("deferred", "invalid_telephony_endpoint")
        try:
            params = TelephonyAPI.model_validate(build_payload(record))
        except Exception as error:
            logger.error(
                "[Telephony] 【送号未发起】运行参数无效: clue_id=%r, status=failed, code=invalid_runtime_config, error_type=%s",
                getattr(record, "clue_id", None),
                type(error).__name__,
            )
            return SubmitResult("failed", "invalid_runtime_config")

        if status_url:
            return await make_call_api(
                url,
                params,
                record.clue_id,
                status_url=status_url,
                timeout_seconds=settings.http_timeout_seconds,
                before_send=getattr(record, "before_send", None),
            )

        # 未配置查询端点时直接发送单次 HTTP POST 送号请求
        before_send = getattr(record, "before_send", None)
        if before_send is not None and not await before_send():
            return SubmitResult("deferred", "submission_gate_closed")
        payload = params.model_dump(exclude_none=True)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=settings.http_timeout_seconds)) as session:
            try:
                async with session.post(url=url, json=payload, allow_redirects=False) as response:
                    raw_body = await response.text()
                    try:
                        body = json.loads(raw_body)
                    except ValueError:
                        body = None
                    return classify_response(response.status, body)
            except aiohttp.ClientConnectorError:
                return SubmitResult("retry", "connection_failed")
            except (asyncio.TimeoutError, aiohttp.ClientError, UnicodeError):
                return SubmitResult("unknown", "transport_unknown")

    async def reconcile_call(self, record: Any) -> SubmitResult:
        from app.config import call_schedule_settings as settings

        status_url = get_status_url()
        if not valid_endpoint(status_url):
            return SubmitResult("unknown", "status_endpoint_missing")
        try:
            params = TelephonyAPI.model_validate(build_payload(record))
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=settings.http_timeout_seconds)) as session:
                result = await check_call_task_status(session, params, record.clue_id, url=status_url)
            if result.status == "found":
                return SubmitResult("accepted", "confirmed_by_query", result.provider_call_id)
        except Exception:
            pass
        return SubmitResult("unknown", "submission_unconfirmed")


def resolve_telephony_gateway() -> BaseTelephonyGateway:
    """根据配置解析生效的话务网关实现（支持 mock 与 webhook）"""
    from app.config import config

    mode = (os.getenv("TELEPHONY_GATEWAY_MODE") or config.call_request_config.gateway_mode or "mock").strip().lower()
    if mode == "mock" or (config.bypass_call and not get_sender_url()):
        return MockTelephonyGateway()
    return GenericWebhookGateway()


async def submit_call(record):
    return await resolve_telephony_gateway().submit_call(record)


async def reconcile_call(record):
    """对 unknown 仅查询核对，不重复送号。"""
    return await resolve_telephony_gateway().reconcile_call(record)
