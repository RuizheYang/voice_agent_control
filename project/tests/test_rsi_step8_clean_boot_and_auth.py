from __future__ import annotations

import hashlib
import hmac
import importlib
import json
import time
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from fastapi import Depends, FastAPI, Request
from tortoise import Tortoise

from app.auth import clear_local_nonce_cache, verify_rsi_signature
from app.main import app
from app.redis_client.redis_manager import RedisManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest_asyncio.fixture(autouse=True)
async def setup_rsi_step8_db():
    clear_local_nonce_cache()
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["app.rsi.models"]},
    )
    await Tortoise.generate_schemas()
    yield
    await Tortoise.close_connections()
    clear_local_nonce_cache()


def test_legacy_v1_v2_directories_and_files_removed():
    """Verify all obsolete V1/V2 directories, routers, static pages, and YAML files are gone."""
    legacy_paths = [
        "app/clue_inflow",
        "app/clue_handler",
        "app/strategy",
        "app/agents",
        "app/call_schedule",
        "app/apis",
        "app/observability",
        "app/dbmodels",
        "app/schemas",
        "static",
        "app/routers/analytics.py",
        "app/routers/clue_inflow.py",
        "app/routers/clue_webhook.py",
        "app/routers/config_admin.py",
        "app/routers/in_call_control.py",
        "app/routers/observability.py",
        "app/domains/car_loan.yaml",
        "app/domains/new_car_sales.yaml",
    ]
    for rel in legacy_paths:
        assert not (PROJECT_ROOT / rel).exists(), f"Legacy path still exists: {rel}"

    clean_v3_paths = [
        "app/main.py",
        "app/config.py",
        "app/auth/signature.py",
        "app/redis_client/redis_manager.py",
        "app/routers/rsi_events.py",
        "app/routers/rsi_admin.py",
        "app/routers/rsi_feedback.py",
        "app/domains/auto_finance_rsi.yaml",
        "app/rsi/models.py",
        "app/rsi/steward.py",
        "app/rsi/call_planner.py",
        "app/rsi/feedback.py",
        "app/rsi/offline_optimizer.py",
    ]
    for rel in clean_v3_paths:
        assert (PROJECT_ROOT / rel).exists(), f"Clean V3 path missing: {rel}"


def test_all_remaining_python_modules_import_cleanly():
    """Every remaining Python module under project/app imports with zero dangling references."""
    app_dir = PROJECT_ROOT / "app"
    imported = []
    for py_file in sorted(app_dir.rglob("*.py")):
        if "__pycache__" in py_file.parts:
            continue
        rel_parts = py_file.relative_to(PROJECT_ROOT).with_suffix("").parts
        if rel_parts[-1] == "__init__":
            mod_name = ".".join(rel_parts[:-1])
        else:
            mod_name = ".".join(rel_parts)
        mod = importlib.import_module(mod_name)
        assert mod is not None
        imported.append(mod_name)

    assert "app.main" in imported
    assert "app.rsi" in imported
    assert "app.rsi.steward" in imported
    assert "app.rsi.offline_optimizer" in imported


@pytest.mark.asyncio
async def test_fastapi_app_mounts_only_clean_v3_routes_and_health():
    """App mounts only clean RSI V3 routes & health endpoints; legacy HTML/clue routes are gone."""
    route_paths = {getattr(r, "path", "") for r in app.routes}
    expected_v3 = {
        "/health",
        "/voice-agent-hub/monitor",
        "/v1/events",
        "/v1/events/batch",
        "/v1/policies",
        "/v1/approvals/pending",
        "/v1/auth/register",
        "/v1/rsi/feedback",
        "/v1/rsi/reports",
        "/v1/rsi/shadow-eval",
    }
    assert expected_v3.issubset(route_paths)

    forbidden_legacy = {
        "/simulator",
        "/event-monitor",
        "/analytics",
        "/clue-inflow",
        "/config-admin",
    }
    assert not (forbidden_legacy & route_paths)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        r_health = await client.get("/health")
        assert r_health.status_code == 200
        assert r_health.json()["version"] == "3.0.0"

        r_mon = await client.get("/voice-agent-hub/monitor")
        assert r_mon.status_code == 200
        assert r_mon.json()["code"] == 0


@pytest.mark.asyncio
async def test_rsi_hmac_signature_auth_and_replay_protection():
    """Register app via /v1/auth/register and enforce HMAC, policy scope & nonce anti-replay."""
    protected_app = FastAPI()

    @protected_app.post("/probe")
    async def probe_route(request: Request, policy_id: str = "af_v1"):
        raw = (await request.body()).decode("utf-8")
        app_id = await verify_rsi_signature(
            request,
            raw,
            policy_id=policy_id,
            tenant_id="t8",
            strict=True,
        )
        return {"ok": True, "app_id": app_id, "tenant_id": request.state.tenant_id}

    secret = "hmac_secret_step8_123"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as admin_client:
        reg_res = await admin_client.post(
            "/v1/auth/register",
            json={
                "tenant_id": "t8",
                "app_id": "crm_client_8",
                "app_secret": secret,
                "name": "CRM V3 Client",
                "allowed_policies": ["af_v1"],
            },
        )
        assert reg_res.status_code == 200

    body_str = json.dumps({"event_id": "e1"})

    def _sign_headers(app_id: str, app_secret: str, nonce: str, ts_ms: int | None = None):
        ts = str(ts_ms or int(time.time() * 1000))
        msg = f"{app_id}.{ts}.{nonce}.{body_str}"
        sig = hmac.new(app_secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()
        return {
            "X-App-Id": app_id,
            "X-Timestamp": ts,
            "X-Nonce": nonce,
            "X-Signature": sig,
            "Content-Type": "application/json",
        }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=protected_app), base_url="http://testserver"
    ) as auth_client:
        # 1) Valid signature + allowed policy passes
        ok_res = await auth_client.post(
            "/probe?policy_id=af_v1",
            content=body_str,
            headers=_sign_headers("crm_client_8", secret, "nonce_001"),
        )
        assert ok_res.status_code == 200
        assert ok_res.json() == {"ok": True, "app_id": "crm_client_8", "tenant_id": "t8"}

        # 2) Nonce replay rejected (401)
        replay_res = await auth_client.post(
            "/probe?policy_id=af_v1",
            content=body_str,
            headers=_sign_headers("crm_client_8", secret, "nonce_001"),
        )
        assert replay_res.status_code == 401

        # 3) Disallowed policy_id rejected (403)
        forbid_res = await auth_client.post(
            "/probe?policy_id=other_pol",
            content=body_str,
            headers=_sign_headers("crm_client_8", secret, "nonce_002"),
        )
        assert forbid_res.status_code == 403

        # 4) Wrong signature rejected (401)
        bad_sig_res = await auth_client.post(
            "/probe?policy_id=af_v1",
            content=body_str,
            headers=_sign_headers("crm_client_8", "wrong_secret", "nonce_003"),
        )
        assert bad_sig_res.status_code == 401

        # 5) Unknown app rejected (401)
        unknown_res = await auth_client.post(
            "/probe?policy_id=af_v1",
            content=body_str,
            headers=_sign_headers("unknown_app", secret, "nonce_004"),
        )
        assert unknown_res.status_code == 401


def test_redis_manager_dead_queue_methods_removed():
    """Verify legacy queue/delayed_queue methods are removed while core Redis methods remain."""
    mgr = RedisManager()
    assert not hasattr(mgr, "rpush_list")
    assert not hasattr(mgr, "queue_pop")
    assert not hasattr(mgr, "get_delayed_queue")
    for method in ("init_redis", "close", "set_json", "get_json", "exists", "delete", "get_lock"):
        assert callable(getattr(mgr, method))
