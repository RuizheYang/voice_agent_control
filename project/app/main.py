"""Clean V3 FastAPI Entrypoint for Customer Follow-up RSI Platform."""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from tortoise import Tortoise

from app.config import config
from app.redis_client.redis_manager import redis_manager
from app.routers import rsi_admin, rsi_events, rsi_feedback
from app.rsi.worker import rsi_worker_loop

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    base_dir = Path(__file__).resolve().parent.parent
    (base_dir / "logs").mkdir(parents=True, exist_ok=True)
    (base_dir / "dbfiles").mkdir(parents=True, exist_ok=True)

    logger.info("[RSI V3] Initializing Tortoise ORM...")
    await Tortoise.init(
        db_url=config.database_url,
        modules={"models": ["app.rsi.models"]},
        timezone="Asia/Shanghai",
        use_tz=False,
        _enable_global_fallback=True,
    )

    current_env = os.getenv("ENV", "dev").lower()
    if current_env in ("dev", "test", "uat"):
        await Tortoise.generate_schemas(safe=True)

    try:
        await redis_manager.init_redis()
    except Exception as err:
        logger.warning("[RSI V3] Optional Redis unavailable, continuing with DB/local fallback: %s", err)

    worker_task = asyncio.create_task(rsi_worker_loop(), name="rsi-inbox-worker")
    try:
        yield
    finally:
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)
        await redis_manager.close()
        await Tortoise.close_connections()


app = FastAPI(
    title="Customer Follow-up RSI V3",
    description="Event-driven Customer Long-Horizon Follow-up RSI Platform",
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(rsi_events.router)
app.include_router(rsi_admin.router)
app.include_router(rsi_feedback.router)


@app.get("/health", tags=["health"])
async def health() -> dict:
    return {"status": "ok", "version": "3.0.0", "mode": "rsi_v3"}


@app.get("/voice-agent-hub/monitor", tags=["health"])
async def monitor() -> dict:
    return {"code": 0, "msg": "OK", "records": "Customer Follow-up RSI V3 ready"}
