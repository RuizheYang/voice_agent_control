"""策略 Agent 一键部署中控，沿用外呼 Agent 的串行发布方式。

配置 DEPLOY_AGENT_TOKEN、DEPLOY_MANAGER_TOKEN 后运行 python deploy_manager_http.py。
POST /deploy，Header: X-Manager-Token，JSON: {"env":"prod"}。
可选 ip_list 指定节点；WECHAT_WEBHOOK_URL 用于可选的企微结果汇总。
"""
import asyncio
import logging
import os
from typing import Literal

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import secrets

DEFAULT_SERVER_IPS = ["10.0.57.15", "10.0.57.16", "10.0.57.17", "10.0.57.18"]
AGENT_PORT = int(os.getenv("DEPLOY_AGENT_PORT", "19528"))
AGENT_AUTH_TOKEN = os.getenv("DEPLOY_AGENT_TOKEN", "")
MANAGER_AUTH_TOKEN = os.getenv("DEPLOY_MANAGER_TOKEN", "")
WEBHOOK_URL = os.getenv("WECHAT_WEBHOOK_URL", "")
MONITOR_PORT = 8001
DEPLOY_LOCK = asyncio.Lock()
logger = logging.getLogger(__name__)
app = FastAPI(title="Voice Agent Hub Deployment Manager")


class DeployRequest(BaseModel):
    env: Literal["test", "uat", "prod"] = "prod"
    ip_list: list[str] | None = None


async def call_single_agent(client, ip, env):
    try:
        response = await client.post(
            f"http://{ip}:{AGENT_PORT}/trigger-deploy", json={"env": env},
            headers={"X-Auth-Token": AGENT_AUTH_TOKEN}, timeout=180)
        result = response.json()
        if response.status_code != 200 or not isinstance(result, dict) or result.get("success") is not True:
            return {"ip": ip, "success": False, "msg": "节点部署失败，请检查节点日志"}
    except (httpx.HTTPError, ValueError):
        return {"ip": ip, "success": False, "msg": "部署请求超时或异常，请检查节点状态"}

    await asyncio.sleep(10)
    for attempt in range(1, 4):
        try:
            response = await client.get(f"http://{ip}:{MONITOR_PORT}/wechat-agent/monitor", timeout=5)
            result = response.json()
            if response.status_code == 200 and isinstance(result, dict) and result.get("code") == 0:
                return {"ip": ip, "success": True, "msg": f"部署成功，心跳检测第 {attempt} 次通过"}
        except (httpx.HTTPError, ValueError):
            pass
        if attempt < 3:
            await asyncio.sleep(10)
    return {"ip": ip, "success": False, "msg": "部署脚本已执行，但心跳检测连续三次失败"}


async def deploy_nodes(client, env, targets):
    results = []
    failed = False
    for ip in targets:
        if failed:
            results.append({"ip": ip, "success": False, "msg": "因前置节点失败，已取消部署"})
            continue
        result = await call_single_agent(client, ip, env)
        results.append(result)
        failed = not result["success"]
    return results


async def send_wechat_summary(client, results, env):
    if not WEBHOOK_URL:
        return
    details = "\n".join(f"{item['ip']}: {item['msg']}" for item in results)
    try:
        response = await client.post(WEBHOOK_URL, timeout=10, json={
            "msgtype": "text", "text": {"content": f"【外呼策略 Agent 部署结果】\n环境: {env}\n{details}"}})
        response.raise_for_status()
        if response.json().get("errcode", 0) != 0:
            logger.warning("企微部署通知被拒绝")
    except (httpx.HTTPError, ValueError):
        logger.warning("企微部署通知发送失败")


@app.post("/deploy")
async def start_deploy(request: DeployRequest, x_manager_token: str | None = Header(None)):
    if not MANAGER_AUTH_TOKEN or not AGENT_AUTH_TOKEN:
        raise HTTPException(503, "请先配置 DEPLOY_MANAGER_TOKEN 和 DEPLOY_AGENT_TOKEN")
    if not x_manager_token or not secrets.compare_digest(x_manager_token, MANAGER_AUTH_TOKEN):
        raise HTTPException(403, "Invalid manager token")
    targets = request.ip_list if request.ip_list is not None else DEFAULT_SERVER_IPS
    if not targets or len(set(targets)) != len(targets) or any(ip not in DEFAULT_SERVER_IPS for ip in targets):
        raise HTTPException(400, "ip_list 必须是已配置生产节点的非空子集，且不能重复")
    if DEPLOY_LOCK.locked():
        raise HTTPException(409, "正在部署，请等待本次部署结束")
    async with DEPLOY_LOCK:
        async with httpx.AsyncClient(trust_env=False) as client:
            results = await deploy_nodes(client, request.env, targets)
            await send_wechat_summary(client, results, request.env)
    success = sum(item["success"] for item in results)
    skipped = sum(item["msg"] == "因前置节点失败，已取消部署" for item in results)
    return {"status": "completed" if success == len(targets) else "interrupted",
            "summary": {"total": len(targets), "success": success,
                        "failed": len(targets)-success-skipped, "skipped": skipped}, "details": results}


if __name__ == "__main__":
    if not MANAGER_AUTH_TOKEN or not AGENT_AUTH_TOKEN:
        raise SystemExit("请先设置 DEPLOY_MANAGER_TOKEN 和 DEPLOY_AGENT_TOKEN")
    import uvicorn
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=8081)
