import atexit
import json
import logging
import os
import queue
import threading
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

ALERT_WEBHOOK_ERROR = os.getenv("ALERT_WEBHOOK_ERROR", os.getenv("ALERT_WEBHOOK_URL", "")).strip()
ALERT_WEBHOOK_WARNING = os.getenv("ALERT_WEBHOOK_WARNING", os.getenv("ALERT_WEBHOOK_URL", "")).strip()
ENV = os.getenv("ENV", "dev").upper()


class AlertWebhookHandler(logging.Handler):
    """可选异步日志告警处理器：仅在配置了 ALERT_WEBHOOK_URL 时推送 WARNING/ERROR 告警"""

    def __init__(self, level: int = logging.WARNING):
        super().__init__()
        self.setLevel(level)
        self._queue: queue.Queue = queue.Queue()
        self._stop_event = threading.Event()
        self._worker_thread = threading.Thread(target=self._log_consumer, daemon=True)
        self._worker_thread.start()
        atexit.register(self.close)

    def _get_target_url(self, levelno: int) -> str:
        return ALERT_WEBHOOK_ERROR if levelno >= logging.ERROR else ALERT_WEBHOOK_WARNING

    def emit(self, record: logging.LogRecord):
        target_url = self._get_target_url(record.levelno)
        if not target_url:
            return
        try:
            self.format(record)
            message = record.getMessage()
            if record.levelno == logging.WARNING and "Sending username and password in the clear" in message:
                return
            self._queue.put_nowait(
                {
                    "url": target_url,
                    "level": record.levelname,
                    "title": f"[Voice Agent Control Alert - {ENV}]",
                    "asctime": getattr(record, "asctime", ""),
                    "message": message,
                }
            )
        except Exception:
            pass

    def _log_consumer(self):
        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                log_item = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                content = (
                    f"{log_item['title']}\n"
                    f"Level: {log_item['level']}\n"
                    f"Time: {log_item['asctime']}\n"
                    f"Message: {log_item['message']}"
                )
                payload = {"msgtype": "text", "text": {"content": content}}
                req = urllib.request.Request(
                    url=log_item["url"],
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    if resp.status >= 400:
                        print(f"[AlertWebhookHandler] webhook status={resp.status}")
            except Exception:
                pass
            finally:
                self._queue.task_done()

    def close(self):
        self._stop_event.set()
        if self._worker_thread.is_alive():
            self._worker_thread.join(timeout=2.0)
        super().close()


def setup_global_alert_logger() -> None:
    """挂载可选的外部告警 Webhook 日志处理器"""
    if not (ALERT_WEBHOOK_ERROR or ALERT_WEBHOOK_WARNING):
        logger.debug("[AlertWebhookHandler] 未配置 ALERT_WEBHOOK_URL，跳过外部日志告警推送")
        return

    root_logger = logging.getLogger()
    if any(isinstance(h, AlertWebhookHandler) for h in root_logger.handlers):
        return

    handler = AlertWebhookHandler(level=logging.WARNING)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    root_logger.addHandler(handler)
    logger.info("[AlertWebhookHandler] 告警 Webhook 日志处理器已挂载 (env=%s)", ENV)


# 兼容保留调用入口
setup_global_wechat_logger = setup_global_alert_logger
