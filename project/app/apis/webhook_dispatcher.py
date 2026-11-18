import asyncio
import logging
import time
import uuid
from typing import Any, Dict, Optional, List
import aiohttp

from app.config import config
from app.apis.api_calls import do_post
from app.dbmodels.models import ClueInfo, OutboundStrategy, WorkflowBinding
from app.observability.events_bus import event_bus, WebhookDeliveryAuditEvent

logger = logging.getLogger(__name__)

EVENT_VERSION = "v1.0"
WEBHOOK_TIMEOUT_SECONDS = 10.0
MAX_WEBHOOK_RETRIES = 3
WEBHOOK_RETRY_BACKOFF = [1.0, 2.0, 4.0]

# 用户指定7大标准可订阅事件
SUPPORTED_EVENTS = {
    "call_schedule",   # 外呼排期
    "call_start",      # 通话开始
    "call_end",        # 通话结束
    "action_wechat",   # 微信动作
    "action_sms",      # 短信动作
    "action_email",    # 邮件动作
    "clue_end",        # 线索结束
}

CANONICAL_EVENT_MAP = {
    "call_schedule": "call_schedule",
    "call_start": "call_start",
    "call_end": "call_end",
    "action_wechat": "action_wechat",
    "action_sms": "action_sms",
    "action_email": "action_email",
    "clue_end": "clue_end",

    # 别名映射兼容
    "call-schedule": "call_schedule",
    "call_scheduled": "call_schedule",
    "call-scheduled": "call_schedule",
    "call-start": "call_start",
    "call_started": "call_start",
    "call-started": "call_start",
    "call-end": "call_end",
    "call_ended": "call_end",
    "call-ended": "call_end",
    "clue-end": "clue_end",
    "clue_ended": "clue_end",
    "clue-ended": "clue_end",
    "wechat": "action_wechat",
    "wechat_requested": "action_wechat",
    "wechat_accepted": "action_wechat",
    "action-wechat": "action_wechat",
    "sms": "action_sms",
    "sms_sent": "action_sms",
    "action-sms": "action_sms",
    "email": "action_email",
    "action-email": "action_email",
}

async def resolve_webhook_target(clue_id: str) -> tuple[Optional[str], Optional[List[str]]]:
    """
    根据 clue_id 查找对应的回调地址与订阅事件列表。
    判定规则：
    1. 第一优先级（线索级动态覆盖）：线索流入时若带了 overrides.callback_url，优先使用该地址；
    2. 第二优先级（策略配置基准，最主要来源）：通过线索关联的 outboundStrategy.webhook.url 获取；
    3. 若两者均未配置，则判定业务方未启用 Webhook 回调，默认不回推 (返回 None, None)。
    注意：坚决不使用全局兜底 URL，以防将敏感通话事件错推至错误的目标。
    """
    try:
        clue = await ClueInfo.filter(clue_id=clue_id).first()
        target_url = None
        subscribed_events = None

        if clue:
            # 1. 第一优先级: 线索级 overrides.callback_url
            if clue.overrides and isinstance(clue.overrides, dict):
                override_url = clue.overrides.get("callback_url")
                if override_url and isinstance(override_url, str) and override_url.strip():
                    target_url = override_url.strip()
                if clue.overrides.get("events") and isinstance(clue.overrides.get("events"), list):
                    subscribed_events = clue.overrides.get("events")

            # 查询关联策略
            strategy_id = clue.strategy_id
            if not strategy_id and clue.workflow_id:
                wf = await WorkflowBinding.filter(workflow_id=clue.workflow_id).first()
                if wf and wf.strategy_id:
                    strategy_id = wf.strategy_id

            strategy_id = strategy_id or "default"
            strategy = await OutboundStrategy.filter(strategy_id=strategy_id).first()
            if strategy and strategy.webhook and isinstance(strategy.webhook, dict):
                # 2. 第二优先级: 策略配置中的 webhook.url
                if not target_url:
                    strat_url = strategy.webhook.get("url")
                    if strat_url and isinstance(strat_url, str) and strat_url.strip():
                        target_url = strat_url.strip()
                # 订阅事件白名单（若线索级未覆盖则取策略配置）
                if not subscribed_events:
                    subscribed_events = strategy.webhook.get("events")

        # 若 overrides 和 strategy 均未配置有效 url，则默认不回推 (返回 None, None)
        return target_url, subscribed_events
    except Exception as e:
        logger.error(f"[Webhook] 解析回调目标异常 clue_id={clue_id}: {e}")
        return None, None

async def _send_http_with_retry(
    callback_url: str,
    payload: Dict[str, Any],
    event: str,
    event_id: str,
    clue_id: str,
) -> bool:
    """
    执行 HTTP POST 回调，包含 10 秒超时及最多 3 次重试 (基于 Retail AI Webhook Spec)。
    底层统一复用 api_calls.do_post 连接池与重试机制。
    使用方需根据 event_id 实现消费幂等。
    """
    headers = {
        "Content-Type": "application/json",
        "X-Event-Id": event_id,
        "X-Event-Type": event,
    }

    # 业务重试判断：非 2xx (status >= 300) 判定需重试
    async def should_retry_webhook(resp: aiohttp.ClientResponse) -> bool:
        return resp.status >= 300

    try:
        status, text, _ = await do_post(
            url=callback_url,
            headers=headers,
            payload=payload,
            clue_id=clue_id,
            timeout_seconds=WEBHOOK_TIMEOUT_SECONDS,
            retry_times=1 + MAX_WEBHOOK_RETRIES,
            retry_interval_seconds=WEBHOOK_RETRY_BACKOFF,
            should_retry_fn=should_retry_webhook,
        )
        if status < 300:
            logger.info(
                f"[Webhook] 事件推送成功: {event} "
                f"(event_id={event_id}, status={status}) -> {callback_url}"
            )
            return True
        else:
            logger.warning(
                f"[Webhook] 事件推送返回非2xx且重试耗尽: "
                f"status={status}, clue_id={clue_id}, event={event}, body={text[:200] if text else ''}"
            )
            return False
    except Exception as e:
        logger.error(
            f"[Webhook] 事件推送失败且重试耗尽 (共尝试 {1 + MAX_WEBHOOK_RETRIES} 次): "
            f"event={event}, event_id={event_id}, clue_id={clue_id}, err={e}"
        )
        return False

async def dispatch_webhook_event(
    event: str,
    clue_id: str,
    data: Dict[str, Any],
    call_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    blocking: bool = False,
    workflow_id: Optional[str] = None,
) -> bool:
    """
    分发标准化事件通知给业务方 Webhook。
    按 Retail AI Webhook 规范：
    - 超时为 10 秒，未收到 2xx 时自动重试最多 3 次。
    - 按序触发，但不阻塞主业务流程（即使 call_started 失败也不影响后续 call_ended 的触发）。
    - 接收方须将 event_id 作为幂等去重键。
    """
    now_ms = int(time.time() * 1000)
    event_id = str(uuid.uuid4())

    canonical_event = CANONICAL_EVENT_MAP.get(event.strip().lower(), event)

    if not workflow_id and clue_id:
        try:
            clue = await ClueInfo.filter(clue_id=clue_id).first()
            if clue:
                workflow_id = clue.workflow_id
        except Exception:
            pass

    payload = {
        "event": canonical_event,
        "event_id": event_id,
        "event_version": EVENT_VERSION,
        "timestamp": now_ms,
        "clue_id": clue_id,
        "workflow_id": workflow_id,
        "call_id": call_id,
        "data": data,
        "metadata": metadata or {},
    }

    # 1. 上报内部事件总线 (用于审计与全链路监控)
    try:
        await event_bus.emit(WebhookDeliveryAuditEvent(
            clue_id=clue_id,
            workflow_id=workflow_id,
            event_id=event_id,
            call_id=call_id,
            event_type=f"webhook_{canonical_event}",
            event_data=payload
        ))
    except Exception as bus_err:
        logger.warning(f"[Webhook] 内部事件总线发射失败 (不阻断流程): {bus_err}")

    # 2. 解析业务方 Webhook 地址
    callback_url, subscribed_events = await resolve_webhook_target(clue_id)
    if not callback_url:
        logger.debug(f"[Webhook] 线索 {clue_id} 未配置 Webhook 回调地址，跳过推送")
        return False

    # 3. 检查事件订阅范围 (事件回推必须显式订阅)
    if not subscribed_events:
        logger.debug(f"[Webhook] 线索 {clue_id} 未配置任何事件订阅白名单，跳过推送")
        return False

    normalized_subs = {CANONICAL_EVENT_MAP.get(e.strip().lower(), e.strip().lower()) for e in subscribed_events}
    if canonical_event not in normalized_subs:
        logger.debug(f"[Webhook] 线索 {clue_id} 的策略未订阅事件 {canonical_event}，跳过推送")
        return False

    # 4. 执行 HTTP POST 回调 (默认非阻塞异步后台任务)
    logger.info(f"[Webhook] 触发事件推送: {canonical_event} -> {callback_url} (event_id={event_id}, clue_id={clue_id}, workflow_id={workflow_id})")
    
    if blocking:
        success = await _send_http_with_retry(
            callback_url=callback_url,
            payload=payload,
            event=canonical_event,
            event_id=event_id,
            clue_id=clue_id,
        )
        try:
            from app.dbmodels.models import ExecutionTask
            from app.utils.time_utils import get_now
            await ExecutionTask.filter(event_id=event_id).update(
                status="succeeded" if success else "failed",
                finished_at=get_now(),
                last_error=None if success else "Webhook delivery failed after retries"
            )
        except Exception:
            pass
        return success
    else:
        async def _do_send_and_update():
            success = await _send_http_with_retry(
                callback_url=callback_url,
                payload=payload,
                event=canonical_event,
                event_id=event_id,
                clue_id=clue_id,
            )
            try:
                from app.dbmodels.models import ExecutionTask
                from app.utils.time_utils import get_now
                await ExecutionTask.filter(event_id=event_id).update(
                    status="succeeded" if success else "failed",
                    finished_at=get_now(),
                    last_error=None if success else "Webhook delivery failed after retries"
                )
            except Exception:
                pass
            return success

        # 非阻塞触发，确保即便网络超时或重试也不会阻塞呼叫与流转
        asyncio.create_task(_do_send_and_update())
        return True
