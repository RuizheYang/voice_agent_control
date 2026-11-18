"""业务事件接收服务：运行 python event_receiver.py，默认监听 8002 端口。"""

import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """兼容没有内置 ThreadingHTTPServer 的 Python 版本。"""

    daemon_threads = True


class EventHandler(BaseHTTPRequestHandler):
    def reply(self, status, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.reply(200, {"code": 0, "msg": "event receiver is running"})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0:
                raise ValueError("empty body")
            event = json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            self.reply(400, {"code": 1, "msg": "invalid JSON body"})
            return

        timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
        print(
            f"\n[{timestamp}] POST {self.path}\n"
            + json.dumps(event, ensure_ascii=False, indent=2),
            flush=True,
        )
        self.reply(200, {"code": 0, "msg": "success"})


if __name__ == "__main__":
    with ThreadingHTTPServer(("0.0.0.0", 8002), EventHandler) as server:
        print("业务事件接收服务已启动：http://0.0.0.0:8002/voice-agent/events", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
