import logging
import json
from typing import Any, Dict, Optional, List
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime
import asyncio
from app.config import config
from app.redis_client.redis_manager import redis_manager
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)
EVENT_QUEUE_NAME = config.event_queue_name

def autonow():
    """统一的时间格式化函数"""
    return get_now().strftime("%Y-%m-%d %H:%M:%S.%f")

# ==========================================
# 1. 基础事件模型体系 (Event Models)
# ==========================================
class BaseEvent(BaseModel):
    """事件基类，所有事件必须包含 clue_id 和发生时间"""
    clue_id: str
    event_time: str = Field(default_factory=autonow)

class AgentEvent(BaseEvent):
    """常规 Agent 业务流转事件抽象基类，禁止直接实例化，必须通过子类使用"""
    event_type: str
    workflow_id: Optional[str] = None
    event_data: Dict[str, Any] = Field(default_factory=dict)
    event_id: Optional[str] = None
    call_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def validate_not_base(self) -> "AgentEvent":
        if self.__class__ is AgentEvent:
            raise TypeError("AgentEvent 是抽象事件基类，禁止直接实例化！请继承并使用具体的子类事件。")
        return self

# ==========================================
# 2. 7 大标准可订阅 Webhook 事件模型
# ==========================================
class CallScheduleEvent(AgentEvent):
    """外呼排期事件: 下一次主动呼叫计划已生成"""
    event_type: str = "call_schedule"

class CallStartEvent(AgentEvent):
    """通话开始事件: 底层线路振铃接通"""
    event_type: str = "call_start"

class CallEndEvent(AgentEvent):
    """通话结束事件: 通话挂断及 AI 质检总结完成"""
    event_type: str = "call_end"

class ActionWechatEvent(AgentEvent):
    """微信动作事件: 触发添加企微动作或发送加微名片"""
    event_type: str = "action_wechat"

class ActionSmsEvent(AgentEvent):
    """短信动作事件: 触发跟进短信发送"""
    event_type: str = "action_sms"

class ActionEmailEvent(AgentEvent):
    """邮件动作事件: 触发跟进邮件发送"""
    event_type: str = "action_email"

class ClueEndEvent(AgentEvent):
    """线索结束事件: 线索达成转化终态或超过最大追呼限制彻底结案"""
    event_type: str = "clue_end"

# ==========================================
# 3. 内部平台事件模型 (仅限内部流转与审计落库)
# ==========================================
class ClueIntakeEvent(AgentEvent):
    """线索接收/流入事件"""
    event_type: str = "clue_intake"

class StateTransferEvent(AgentEvent):
    """内部状态流转审计事件"""
    event_type: str = "state_transfer"

class VoiceAgentCallRequestEvent(AgentEvent):
    """外呼平台呼叫请求事件"""
    event_type: str = "voice_agent_call_request"

class VoiceAgentCancelSuccessEvent(AgentEvent):
    """外呼平台取消呼叫成功事件"""
    event_type: str = "voice_agent_cancel_success"

class ClueTimeoutWarningEvent(AgentEvent):
    """线索超时告警事件"""
    event_type: str = "clue_timeout_warning"

class WebhookDeliveryAuditEvent(AgentEvent):
    """Webhook 推送投递审计记录事件"""
    event_type: str = "webhook_delivery_audit"

STANDARD_WEBHOOK_EVENT_TYPES = {
    "call_schedule",
    "call_start",
    "call_end",
    "action_wechat",
    "action_sms",
    "action_email",
    "clue_end",
}

STANDARD_WEBHOOK_EVENT_CLASSES = {
    "CallScheduleEvent",
    "CallStartEvent",
    "CallEndEvent",
    "ActionWechatEvent",
    "ActionSmsEvent",
    "ActionEmailEvent",
    "ClueEndEvent",
}

# ==========================================
# 4. 核心事件派发器 (Event Dispatcher)
# ==========================================
class EventBus:
    """集成了发布(Emit)与订阅/消费(Consume)的事件总线"""
    
    def __init__(self, redis, queue_name: str):
        self.redis = redis
        self.queue_name = queue_name
        
        # 【核心路由表】：定义每个事件消费时的去向
        # key: 事件类名, value: 消费渠道列表 ["mysql", "webhook"]
        self.routes: Dict[str, List[str]] = {
            # 7 大标准可订阅 Webhook 事件 (落库 MySQL + 异步 Webhook 推送)
            "CallScheduleEvent": ["mysql", "webhook"],
            "CallStartEvent": ["mysql", "webhook"],
            "CallEndEvent": ["mysql", "webhook"],
            "ActionWechatEvent": ["mysql", "webhook"],
            "ActionSmsEvent": ["mysql", "webhook"],
            "ActionEmailEvent": ["mysql", "webhook"],
            "ClueEndEvent": ["mysql", "webhook"],

            # 内部审计与系统流转事件 (仅落库 MySQL)
            "ClueIntakeEvent": ["mysql"],
            "StateTransferEvent": ["mysql"],
            "VoiceAgentCallRequestEvent": ["mysql"],
            "VoiceAgentCancelSuccessEvent": ["mysql"],
            "ClueTimeoutWarningEvent": ["mysql"],
            "WebhookDeliveryAuditEvent": ["mysql"],
        }

    # ------------------------------------------
    # 生产者逻辑 (Emit)
    # ------------------------------------------
    async def emit(self, event: BaseEvent):
        """将事件装入统一信封(Envelope)并推入 Redis 队列"""
        try:
            envelope = {
                "event_class": event.__class__.__name__,
                "payload": event.model_dump(mode='json')
            }
            # 存入 Redis，等待后台任务慢慢消费
            await self.redis.rpush_list(self.queue_name, json.dumps(envelope, ensure_ascii=False))
        except Exception as e:
            logger.info(f"线索：{event.clue_id} - 事件 {event.__class__.__name__} Push Redis 失败: {e}")

    # ------------------------------------------
    # 消费者逻辑 (Consume)
    # ------------------------------------------
    async def consume_loop(self):
        """常驻后台的健壮消费循环"""
        logger.info(f"EventBus Consumer 已启动，正在持续监听队列: {self.queue_name}")
        
        while True:
            try:
                # 1. 阻塞等待：底层使用 BLPOP，超时设为 5 秒，避免占用 CPU 空转
                raw_event = await self.redis.queue_pop(self.queue_name, timeout=5)
                
                if not raw_event:
                    continue # 超时未拿到数据，进入下一次循环

                # 2. 隔离业务逻辑：将单条数据的处理放入独立方法，确保其崩溃不影响主循环
                await self._safe_process_event(raw_event)

            except asyncio.CancelledError:
                # 3. 平滑退出：捕捉 FastAPI 停机时发出的 Task.cancel() 信号
                logger.info("收到应用停机信号，EventBus Consumer 正在平滑退出...")
                break # 退出 while 循环，安全结束任务

            except Exception as e:
                # 4. 终极兜底：捕捉 Redis 断连等一切不可预知的系统级错误
                logger.info(f"EventBus 消费主循环发生严重异常: {e}", exc_info=True)
                
                # 5. 退避策略(Backoff)：必须休眠！防止故障期间死循环疯狂报错打满 CPU
                await asyncio.sleep(5) 
                logger.info("EventBus Consumer 尝试恢复运行...")

    # ------------------------------------------
    # 具体的分发动作 (Dispatchers)
    # ------------------------------------------
    async def _dispatch_to_mysql(self, payload: dict, event_class: str = ""):
        try:
            # 延迟导入，避免 app.dbmodels.crud -> app.clue_handler -> app.observability 的模块级循环依赖
            from app.dbmodels import crud
            event_type = payload.get("event_type", "")
            
            # 区分任务类型: 标准 Webhook 事件标记为 webhook，系统及流转事件标记为 internal_event
            if event_class in STANDARD_WEBHOOK_EVENT_CLASSES or event_type in STANDARD_WEBHOOK_EVENT_TYPES:
                task_type = "webhook"
            else:
                task_type = "internal_event"

            # 将多余的业务字段打包进 eventData
            event_data = payload.get("event_data", payload) 
            
            await crud.save_clue_event(
                clueId=payload.get("clue_id", ""),
                eventType=event_type,
                eventTime=payload.get("event_time", ""),
                eventData=event_data,
                taskType=task_type,
                workflowId=payload.get("workflow_id"),
                eventId=payload.get("event_id"),
            )
        except Exception as e:
            logger.info(f"分发至 MySQL 落库失败: {e}")
            
    async def _dispatch_to_webhook(self, payload: dict, canonical_event: str):
        try:
            import time
            import uuid
            from app.apis.webhook_dispatcher import (
                resolve_webhook_target,
                _send_http_with_retry,
                CANONICAL_EVENT_MAP,
            )

            clue_id = payload.get("clue_id", "")
            if not clue_id:
                return

            canonical = CANONICAL_EVENT_MAP.get(canonical_event.lower(), canonical_event)
            callback_url, subscribed_events = await resolve_webhook_target(clue_id)
            if not callback_url:
                logger.debug(f"[EventBus Webhook] 线索 {clue_id} 未配置 Webhook 回调地址，跳过推送")
                return

            # 事件回推必须显式订阅
            if not subscribed_events:
                logger.debug(f"[EventBus Webhook] 线索 {clue_id} 未配置任何事件订阅白名单，跳过推送")
                return

            normalized_subs = {CANONICAL_EVENT_MAP.get(e.strip().lower(), e.strip().lower()) for e in subscribed_events}
            if canonical not in normalized_subs:
                logger.debug(f"[EventBus Webhook] 线索 {clue_id} 的策略未订阅事件 {canonical}，跳过推送")
                return

            now_ms = int(time.time() * 1000)
            event_id = payload.get("event_id") or str(uuid.uuid4())
            call_id = payload.get("call_id")
            workflow_id = payload.get("workflow_id")
            if not workflow_id:
                try:
                    from app.dbmodels.models import ClueInfo
                    clue = await ClueInfo.filter(clue_id=clue_id).first()
                    if clue:
                        workflow_id = clue.workflow_id
                except Exception:
                    pass

            metadata = payload.get("metadata") or {}
            event_data = payload.get("event_data", {})

            webhook_payload = {
                "event": canonical,
                "event_id": event_id,
                "event_version": "v1.0",
                "timestamp": now_ms,
                "clue_id": clue_id,
                "workflow_id": workflow_id,
                "call_id": call_id,
                "data": event_data,
                "metadata": metadata,
            }

            logger.info(f"[EventBus Webhook] 触发推送: {canonical} -> {callback_url} (event_id={event_id}, clue_id={clue_id}, workflow_id={workflow_id})")
            send_success = await _send_http_with_retry(
                callback_url=callback_url,
                payload=webhook_payload,
                event=canonical,
                event_id=event_id,
                clue_id=clue_id,
            )

            # 更新 ExecutionTask 状态
            try:
                from app.dbmodels.models import ExecutionTask
                await ExecutionTask.filter(
                    event_id=event_id,
                    task_type="webhook"
                ).update(
                    status="succeeded" if send_success else "failed",
                    finished_at=get_now(),
                    last_error=None if send_success else "Webhook delivery failed after retries"
                )
            except Exception as db_err:
                logger.debug(f"更新 Webhook executionTask 状态失败: {db_err}")

        except Exception as e:
            logger.error(f"[EventBus Webhook] 分发至 Webhook 失败: {e}", exc_info=True)

    async def _safe_process_event(self, raw_event):
        """单条事件的安全处理方法（异常内部消化）"""
        try:
            envelope = json.loads(raw_event) if isinstance(raw_event, str) else raw_event
            event_class = envelope.get("event_class", "Unknown")
            payload = envelope.get("payload", {})
            event_type = payload.get("event_type", "")

            target_routes = self.routes.get(event_class)
            if target_routes is None:
                if event_class in STANDARD_WEBHOOK_EVENT_CLASSES or event_type in STANDARD_WEBHOOK_EVENT_TYPES:
                    target_routes = ["mysql", "webhook"]
                else:
                    target_routes = ["mysql"]

            # 并发执行各个目标渠道的分发，提升吞吐量
            tasks = []
            if "mysql" in target_routes:
                tasks.append(self._dispatch_to_mysql(payload, event_class))
            if "webhook" in target_routes:
                canonical_event = event_type or event_class
                tasks.append(self._dispatch_to_webhook(payload, canonical_event))
            
            if tasks:
                # return_exceptions=True 确保某一个渠道失败，不会影响另一个渠道
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            # 单条数据处理失败，仅记录日志，不会向上抛出导致主循环崩溃
            logger.info(f"处理单条事件失败，放弃该事件. raw_data: {raw_event} | 错误: {e}", exc_info=True)
            # 可选：在这里将失败的 raw_event 推入一个死信队列 (Dead Letter Queue) 以便后续排查

# 实例化全局单例
event_bus = EventBus(redis_manager, EVENT_QUEUE_NAME)