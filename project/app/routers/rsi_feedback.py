"""Offline RSI feedback, optimization report, and Shadow Mode API router."""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Query

from app.rsi.feedback import (
    FeedbackRecordRequest,
    FeedbackRecordResponse,
    list_feedback_records,
    record_feedback,
    sync_guardrail_blocks_to_feedback,
)
from app.rsi.offline_optimizer import (
    OfflineReviewReport,
    ShadowEvalRequest,
    ShadowEvalResponse,
    generate_offline_review_report,
    run_shadow_evaluation,
)

router = APIRouter(prefix="/v1/rsi", tags=["RSI Offline Optimization & Feedback"])


@router.post(
    "/feedback",
    response_model=FeedbackRecordResponse,
    summary="上报转化归因与人工改稿反馈（自动计算人类改稿 Diff）",
)
async def post_rsi_feedback(payload: FeedbackRecordRequest) -> FeedbackRecordResponse:
    return await record_feedback(payload)


@router.get(
    "/feedback",
    response_model=List[FeedbackRecordResponse],
    summary="查询已沉淀的反馈与改稿记录",
)
async def get_rsi_feedback(
    tenant_id: str = Query(default="default", min_length=1, max_length=64),
    entity_id: Optional[str] = Query(default=None, max_length=128),
    policy_id: Optional[str] = Query(default=None, max_length=128),
    feedback_type: Optional[str] = Query(default=None, max_length=64),
    limit: int = Query(default=100, ge=1, le=500),
) -> List[FeedbackRecordResponse]:
    return await list_feedback_records(
        tenant_id=tenant_id,
        entity_id=entity_id,
        policy_id=policy_id,
        feedback_type=feedback_type,
        limit=limit,
    )


@router.get(
    "/reports",
    response_model=OfflineReviewReport,
    summary="获取离线复盘聚合报告、改稿聚类与调优建议",
)
async def get_rsi_reports(
    tenant_id: str = Query(default="default", min_length=1, max_length=64),
    policy_id: Optional[str] = Query(default=None, max_length=128),
    window_days: int = Query(default=30, ge=1, le=365),
    sync_blocks: bool = Query(default=False),
) -> OfflineReviewReport:
    if sync_blocks:
        await sync_guardrail_blocks_to_feedback(tenant_id=tenant_id)
    return await generate_offline_review_report(
        tenant_id=tenant_id,
        policy_id=policy_id,
        window_days=window_days,
    )


@router.post(
    "/shadow-eval",
    response_model=ShadowEvalResponse,
    summary="执行候选策略零外发 Shadow Mode 离线对比评估",
)
async def post_rsi_shadow_eval(payload: ShadowEvalRequest) -> ShadowEvalResponse:
    return await run_shadow_evaluation(payload)
