import logging
import os
import queue
import threading
import json
import urllib.request
import urllib.error
import atexit

logger = logging.getLogger(__name__)

# ================= 配置常量 =================
TEST_ERROR_URL = os.getenv(
    "TEST_WECHAT_WEBHOOK_ERROR",
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=759aaed0-cba8-4584-802e-2792fdb88e3e"
)
TEST_WARNING_URL = os.getenv(
    "TEST_WECHAT_WEBHOOK_WARNING",
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=00fc29c4-9caf-47f5-add8-e3c6eace40a1"
)
PROD_ERROR_URL = os.getenv(
    "PROD_WECHAT_WEBHOOK_ERROR",
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=aa462180-a422-4100-bfc6-1a2909ab7b38"
)
PROD_WARNING_URL = os.getenv(
    "PROD_WECHAT_WEBHOOK_WARNING",
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=54661f71-6f22-462b-b0b2-a94d776e24db"
)
ENV = os.getenv("ENV", "test").upper()


class WeChatWarnErrorHandler(logging.Handler):

    def __init__(self, level=logging.WARNING):
        super().__init__()
        self.setLevel(level)
        self._queue = queue.Queue()
        self._stop_event = threading.Event()

        # 启动后台消费者线程
        self._worker_thread = threading.Thread(target=self._log_consumer, daemon=True)
        self._worker_thread.start()

        # 注册优雅退出钩子：确保主程序退出时，队列中积压的日志能够发送完毕
        atexit.register(self.close)

    def _get_target_url(self, levelno: int) -> str:
        """根据环境和日志级别路由对应的 Webhook URL"""
        is_prod = "PROD" in ENV
        is_error = levelno >= logging.ERROR

        if is_prod:
            return PROD_ERROR_URL if is_error else PROD_WARNING_URL
        else:
            return TEST_ERROR_URL if is_error else TEST_WARNING_URL

    def emit(self, record: logging.LogRecord):
        try:
            # 触发标准的 format 逻辑（确保 record.asctime 等时间属性被正确生成）
            self.format(record)
            message = record.getMessage()

            # 新增拦截逻辑：如果是 WARNING 级别，且包含特定明文传输警告，直接丢弃
            if record.levelno == logging.WARNING and "Sending username and password in the clear" in message:
                return

            # 动态获取当前日志应该发送的 URL
            target_url = self._get_target_url(record.levelno)

            # 提取核心结构体，压入队列（无锁非阻塞）
            log_item = {
                "url": target_url,
                "level": record.levelname,
                "title": f"【加微Agent异常 - {ENV}】",  # 在标题中增加环境标识
                "asctime": getattr(record, "asctime", ""),
                "message": message,
                "mentioned_list": getattr(record, "mentioned_list", None),
                "mentioned_mobile_list": getattr(record, "mentioned_mobile_list", None)
            }
            self._queue.put_nowait(log_item)

        except Exception as e:
            # 避免 Handler 崩溃影响主业务线
            print(f"[WeChatLogHandler] 解析日志失败: {e}")

    def _log_consumer(self):
        """后台常驻消费者：串行异步发送"""
        # 当 stop_event 被设置且队列为空时，退出循环
        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                # 阻塞式获取，并设置超时。超时机制是为了能让线程定期去检查 _stop_event 的状态
                log_item = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            try:
                # 动态组装模板
                content = (
                    f"{log_item['title']}\n"
                    f"告警级别: {log_item['level']}\n"
                    f"告警信息: {log_item['message']}\n"
                    f"报警时间: {log_item['asctime']}\n"
                )

                payload = {
                    "msgtype": "text",
                    "text": {"content": content}
                }
                
                if log_item.get("mentioned_list"):
                    payload["text"]["mentioned_list"] = log_item["mentioned_list"]
                if log_item.get("mentioned_mobile_list"):
                    payload["text"]["mentioned_mobile_list"] = log_item["mentioned_mobile_list"]

                # 使用 Python 原生 urllib 发起 HTTP 请求
                data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(
                    url=log_item["url"],
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST"
                )

                # urlopen 会在遇到 4xx 或 5xx 时抛出 HTTPError
                with urllib.request.urlopen(req, timeout=10) as response:
                    status_code = response.getcode()
                    if status_code != 200:
                        print(f"[WeChatLogHandler] 企微消息发送返回异常状态码: {status_code}")

            except urllib.error.URLError as e:
                # 捕获网络异常（如超时、连接拒绝）
                print(f"[WeChatLogHandler] 企微消息网络请求失败: {e}")
            except Exception as e:
                # Fallback 打印，防止异常导致消费者线程死掉
                print(f"[WeChatLogHandler] 企微消息发送处理失败: {e}")
            finally:
                # 无论发送成功或失败，都通知队列该任务处理完毕
                self._queue.task_done()

    def close(self):
        """覆盖父类 close 方法，实现优雅退出"""
        self._stop_event.set()
        if self._worker_thread.is_alive():
            # 给线程最多 3 秒的时间处理队列中剩余的日志
            self._worker_thread.join(timeout=3.0)
        super().close()


# ================= 全局生效配置 =================
def setup_global_wechat_logger():
    root_logger = logging.getLogger()

    # 优化：防止被多次调用时重复添加 Handler 导致日志发多份
    if not any(isinstance(h, WeChatWarnErrorHandler) for h in root_logger.handlers):
        wechat_handler = WeChatWarnErrorHandler()
        root_logger.addHandler(wechat_handler)