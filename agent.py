"""节点部署入口：与外呼 Agent 相同，git pull 后执行现有 server.sh。

每台服务器配置 DEPLOY_AGENT_TOKEN 后运行：python agent.py
默认监听 19528，避免与外呼 Agent 的 19527 端口冲突。
"""
import http.server
import json
import logging
import os
from pathlib import Path
import secrets
import subprocess
import threading

ROOT = Path(__file__).resolve().parent
PORT = int(os.getenv("DEPLOY_AGENT_PORT", "19528"))
AUTH_TOKEN = os.getenv("DEPLOY_AGENT_TOKEN", "")
DEPLOY_LOCK = threading.Lock()
logger = logging.getLogger(__name__)


def run_deploy(env):
    if env not in ("test", "uat", "prod"):
        return 400, {"success": False, "msg": "env 仅支持 test、uat、prod"}
    if not DEPLOY_LOCK.acquire(blocking=False):
        return 409, {"success": False, "msg": "本机正在部署，请勿重复提交"}
    try:
        subprocess.run(["git", "pull", "--ff-only"], cwd=ROOT, check=True,
                       capture_output=True, timeout=30)
        subprocess.run(["bash", "./server.sh", "-e", env], cwd=ROOT, check=True,
                       capture_output=True, timeout=120)
        return 200, {"success": True, "msg": "部署脚本执行完成"}
    except subprocess.TimeoutExpired:
        logger.exception("部署超时")
        return 504, {"success": False, "msg": "部署超时，请检查本机容器状态"}
    except subprocess.CalledProcessError:
        logger.exception("Git 更新或启动脚本执行失败")
        return 500, {"success": False, "msg": "Git 更新或启动脚本执行失败，请检查节点日志"}
    except OSError:
        logger.exception("无法执行部署命令")
        return 500, {"success": False, "msg": "无法执行部署命令，请检查 Git、Bash 和工作目录"}
    finally:
        DEPLOY_LOCK.release()


class DeployHandler(http.server.BaseHTTPRequestHandler):
    def respond(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/trigger-deploy":
            self.respond(404, {"msg": "Not found"})
            return
        token = self.headers.get("X-Auth-Token", "")
        if not AUTH_TOKEN or not secrets.compare_digest(token, AUTH_TOKEN):
            self.respond(403, {"msg": "Invalid agent token"})
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 4096:
                raise ValueError("Invalid body size")
            payload = json.loads(self.rfile.read(size))
            if not isinstance(payload, dict):
                raise ValueError("Expected object")
        except (ValueError, UnicodeError):
            self.respond(400, {"msg": "请求体必须是 JSON 对象"})
            return
        status, result = run_deploy(payload.get("env", "prod"))
        self.respond(status, result)


if __name__ == "__main__":
    if not AUTH_TOKEN:
        raise SystemExit("请先设置 DEPLOY_AGENT_TOKEN，与部署中控保持一致")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), DeployHandler)
    logger.info("部署节点启动，端口 %s，工作目录 %s", PORT, ROOT)
    server.serve_forever()
