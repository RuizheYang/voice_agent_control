"""RSI V3 control-plane admin API: policies, L2 approvals, kill switches, and tenant auth."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.rsi.guardrails.approvals import (
    list_pending_outbox_approvals,
    resolve_outbox_approval,
)
from app.rsi.guardrails.kill_switch import list_kill_switches, set_kill_switch
from app.rsi.models import FollowupPolicy, RsiTenantAuth
from app.rsi.schemas import (
    ApprovalResolveRequest,
    ApprovalResolveResponse,
    KillSwitchRequest,
    PolicyUpsertRequest,
    TenantAuthRegisterRequest,
)

router = APIRouter(prefix="/v1", tags=["RSI Admin & Guardrails"])


@router.post("/policies", summary="创建或更新租户跟进策略包 (Policy-as-Data)")
async def upsert_policy(payload: PolicyUpsertRequest) -> dict:
    existing = await FollowupPolicy.filter(
        tenant_id=payload.tenant_id, policy_id=payload.policy_id
    ).first()
    if existing is not None:
        existing.name = payload.name or existing.name
        existing.version = payload.version
        existing.config = payload.config
        existing.is_active = payload.is_active
        await existing.save()
        rec = existing
    else:
        rec = await FollowupPolicy.create(
            tenant_id=payload.tenant_id,
            policy_id=payload.policy_id,
            name=payload.name,
            version=payload.version,
            config=payload.config,
            is_active=payload.is_active,
        )
    return {
        "tenant_id": rec.tenant_id,
        "policy_id": rec.policy_id,
        "name": rec.name,
        "version": rec.version,
        "config": rec.config or {},
        "is_active": rec.is_active,
    }


@router.get("/policies/{policy_id}", summary="查询租户跟进策略包")
async def get_policy(
    policy_id: str,
    tenant_id: str = Query(default="default", min_length=1, max_length=64),
) -> dict:
    rec = await FollowupPolicy.filter(
        tenant_id=tenant_id, policy_id=policy_id
    ).first()
    if rec is None:
        raise HTTPException(status_code=404, detail=f"Policy '{policy_id}' not found")
    return {
        "tenant_id": rec.tenant_id,
        "policy_id": rec.policy_id,
        "name": rec.name,
        "version": rec.version,
        "config": rec.config or {},
        "is_active": rec.is_active,
    }


@router.get("/approvals/pending", summary="查询待人工审批的 L2 Outbox 动作")
async def get_pending_approvals(
    tenant_id: Optional[str] = Query(default=None, max_length=64),
    entity_id: Optional[str] = Query(default=None, max_length=128),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict:
    items = await list_pending_outbox_approvals(
        tenant_id=tenant_id, entity_id=entity_id, limit=limit
    )
    return {"total": len(items), "items": items}


@router.post("/approvals/{outbox_id}/resolve", response_model=ApprovalResolveResponse, summary="审批并恢复执行挂起的 L2 Outbox 动作")
async def post_resolve_approval(
    outbox_id: int,
    payload: ApprovalResolveRequest,
) -> ApprovalResolveResponse:
    try:
        return await resolve_outbox_approval(
            outbox_id,
            approved=payload.approved,
            reviewer=payload.reviewer,
            comment=payload.comment,
        )
    except ValueError as err:
        raise HTTPException(status_code=404, detail=str(err)) from err


@router.post("/control/pause", summary="启用全局/租户/通道/策略级熔断暂停")
async def post_control_pause(payload: KillSwitchRequest) -> dict:
    sw = await set_kill_switch(
        payload.tenant_id,
        payload.scope_type,
        payload.target_id,
        True,
        reason=payload.reason,
        paused_until=payload.paused_until,
    )
    return {
        "tenant_id": sw.tenant_id,
        "scope_type": sw.scope_type,
        "target_id": sw.target_id,
        "paused": sw.paused,
        "reason": sw.reason,
    }


@router.post("/control/resume", summary="解除熔断暂停恢复执行")
async def post_control_resume(payload: KillSwitchRequest) -> dict:
    sw = await set_kill_switch(
        payload.tenant_id,
        payload.scope_type,
        payload.target_id,
        False,
    )
    return {
        "tenant_id": sw.tenant_id,
        "scope_type": sw.scope_type,
        "target_id": sw.target_id,
        "paused": sw.paused,
    }


@router.get("/control/status", summary="查询当前生效中的熔断开关")
async def get_control_status(
    tenant_id: Optional[str] = Query(default=None, max_length=64),
) -> dict:
    switches = await list_kill_switches(tenant_id=tenant_id)
    return {"total": len(switches), "active_switches": switches}


@router.post("/auth/register", summary="注册或更新租户应用凭证")
async def post_register_auth(payload: TenantAuthRegisterRequest) -> dict:
    existing = await RsiTenantAuth.filter(
        tenant_id=payload.tenant_id, app_id=payload.app_id
    ).first()
    if existing is not None:
        existing.app_secret = payload.app_secret
        existing.name = payload.name or existing.name
        existing.allowed_policies = payload.allowed_policies
        existing.is_active = True
        await existing.save()
        rec = existing
    else:
        rec = await RsiTenantAuth.create(
            tenant_id=payload.tenant_id,
            app_id=payload.app_id,
            app_secret=payload.app_secret,
            name=payload.name,
            allowed_policies=payload.allowed_policies,
            is_active=True,
        )
    return {
        "tenant_id": rec.tenant_id,
        "app_id": rec.app_id,
        "name": rec.name,
        "allowed_policies": rec.allowed_policies or [],
        "is_active": rec.is_active,
    }
