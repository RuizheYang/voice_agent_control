import logging
import os
from pathlib import Path

from app.utils.wechatbot import setup_global_wechat_logger
from app.utils.workers import clue_scheduler_loop
from app.clue_inflow.worker import file_import_loop, new_clue_entry_loop
from app.call_schedule.worker import call_dispatch_loop, call_reconcile_loop
from app.agents.inbox_worker import inbox_wakeup_loop
from app.config import call_schedule_settings

logger = logging.getLogger(__name__)
import asyncio

from contextlib import asynccontextmanager
from app.config import config
from app.redis_client.redis_manager import redis_manager
from app.routers import clue_webhook
from app.routers import clue_inflow
from app.routers import config_admin
from app.routers import in_call_control
from app.routers import observability as observability_router
from app.routers import analytics as analytics_router
from app.observability.events_bus import event_bus
from fastapi import FastAPI, Request, applications
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse, HTMLResponse
from tortoise import Tortoise
from tortoise.contrib.fastapi import RegisterTortoise


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_path = Path(__file__).resolve().parent.parent / "logs"
    log_path.mkdir(parents=True, exist_ok=True)
    
    db_folder = Path(__file__).resolve().parent.parent / "dbfiles"
    db_folder.mkdir(parents=True, exist_ok=True)

    # 初始化全局配置
    setup_global_wechat_logger()
    
    logger.info("初始化 Tortoise ORM...")
    await Tortoise.init(
        db_url=config.mysql_url,
        modules={'models': ['app.dbmodels.models']},
        timezone="Asia/Shanghai",
        use_tz=False,
        _enable_global_fallback=True
    )

    current_env = os.getenv("ENV", "dev").lower()

    if current_env in ["dev", "test", "uat"]:
        logger.info(f"当前环境为 {current_env}，尝试自动同步数据库表结构...")
        await Tortoise.generate_schemas(safe=True)
    else:
        logger.info(f"当前环境为 {current_env}，跳过 ORM 自动建表阶段。")

    logger.info("初始化 Redis 连接...")
    try:
        await redis_manager.init_redis()
    except Exception as e:
        logger.error(f"初始化 Redis 失败: {e}")

    logger.info("启动阶段挂载监听器")

    logger.info("启动线索消费 worker...")
    workers = []
    # 受理后进入既有新线索handler；独立恢复worker接管未完成的入池任务。
    workers.append(asyncio.create_task(file_import_loop(), name="clue-file-import"))
    workers.append(asyncio.create_task(new_clue_entry_loop(), name="clue-entry-recovery"))
    workers.append(asyncio.create_task(call_reconcile_loop(), name="call-schedule-reconcile"))
    for index in range(call_schedule_settings.workers):
        workers.append(asyncio.create_task(call_dispatch_loop(), name=f"call-dispatch-{index}"))

    logger.info("启动线索状态定时轮询调度器...")
    scheduler_task = asyncio.create_task(clue_scheduler_loop())
    workers.append(scheduler_task)

    logger.info("启动事件溯源消费者 (Event Consumer)...")
    event_consumer_task = asyncio.create_task(event_bus.consume_loop())
    workers.append(event_consumer_task)

    logger.info("启动 Always-On Event Inbox & 认知唤醒 Worker...")
    for idx in range(2):
        workers.append(asyncio.create_task(inbox_wakeup_loop(), name=f"inbox-wakeup-{idx}"))

    try:
        yield
    finally:
        logger.info("关闭应用程序...")
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await redis_manager.close()
        await Tortoise.close_connections()
    
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(clue_inflow.router)
app.include_router(config_admin.router)
app.include_router(clue_webhook.router)
app.include_router(in_call_control.router)
app.include_router(observability_router.router)
app.include_router(analytics_router.router)

@app.get("/simulator", response_class=HTMLResponse, tags=["simulator"])
async def get_simulator():
    """提供交互式前端模拟页面"""
    html_file = Path(__file__).resolve().parent.parent / "static" / "simulator.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))

@app.get("/event-monitor", response_class=HTMLResponse, tags=["simulator"])
async def get_event_monitor():
    """提供单一线索事件溯源监控面板"""
    html_file = Path(__file__).resolve().parent.parent / "static" / "event_monitor.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))

@app.get("/analytics", response_class=HTMLResponse, tags=["simulator"])
async def get_analytics_dashboard():
    """提供数据分析大盘面板"""
    html_file = Path(__file__).resolve().parent.parent / "static" / "analytics.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))

@app.get("/voice-agent-hub/monitor")
async def monitor():
    return {"code": 0, "msg": "OK", "records": "Voice Agent Control service ready"}
