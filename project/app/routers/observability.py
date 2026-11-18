import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field

from app.dbmodels.crud import get_clue_events
from app.dbmodels.models import CallRecord, ClueInfo
from app.strategy.contracts import ClueContext
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/observability",
    tags=["observability"],
    responses={404: {"description": "Not found"}},
)


@router.get("/clue-trace", summary="获取线索全链路状态、画像、多智能体记忆、通联与事件全貌")
async def fetch_clue_trace(clueId: str = Query(..., description="线索唯一标识")):
    """单接口全链路排障聚合接口"""
    try:
        from app.utils.decrypt import crypted_manager

        clue = await ClueInfo.filter(clue_id=clueId).first()
        if not clue:
            return {
                "clueId": clueId,
                "state": "unknown",
                "detail": None,
                "events": [],
            }

        phone = clue.phone_number
        if phone and len(phone) > 20:
            try:
                phone = crypted_manager.decrypt_field(phone)
            except Exception:
                pass

        name = clue.customer_name
        if name and len(name) > 20:
            try:
                name = crypted_manager.decrypt_field(name)
            except Exception:
                pass

        calls = await CallRecord.filter(clue_id=clueId).order_by("-create_time").limit(10)
        call_records_data = []
        for c in calls:
            decision_res = c.decision_result if isinstance(c.decision_result, dict) else {}
            call_records_data.append(
                {
                    "call_id": c.call_id,
                    "status": c.status,
                    "attempt_no": c.attempt_no,
                    "answered": c.answered,
                    "duration_seconds": c.duration_seconds,
                    "recording_url": c.recording_url,
                    "summary_status": c.summary_status,
                    "decision": c.decision,
                    "decision_result": decision_res,
                    "common_call_events": c.common_call_events if isinstance(c.common_call_events, dict) else {},
                    "create_time": c.create_time.strftime("%Y-%m-%d %H:%M:%S") if c.create_time else None,
                }
            )

        events = await get_clue_events(clueId)
        events_data = [e.model_dump() for e in events]

        agent_data = clue.agent_data if isinstance(clue.agent_data, dict) else {}
        detail = {
            "clue_id": clue.clue_id,
            "status": clue.status,
            "state_version": clue.state_version,
            "workflow_id": clue.workflow_id,
            "agent_id": clue.agent_id,
            "strategy_id": clue.strategy_id,
            "customer_name": name,
            "customer_city": agent_data.get("customerCity"),
            "phone_number": phone,
            "total_call_count": clue.total_call_count,
            "next_call_at": clue.next_call_at.strftime("%Y-%m-%d %H:%M:%S") if clue.next_call_at else None,
            "close_reason": clue.close_reason,
            "end_code": clue.end_code,
            "handoff_history": agent_data.get("handoff_history", []),
            "agent_directive": agent_data.get("agent_directive"),
            "memory": agent_data.get("memory"),
            "create_time": clue.create_time.strftime("%Y-%m-%d %H:%M:%S") if clue.create_time else None,
            "update_time": clue.update_time.strftime("%Y-%m-%d %H:%M:%S") if clue.update_time else None,
            "call_records": call_records_data,
        }

        return {
            "clueId": clueId,
            "state": clue.status,
            "detail": detail,
            "events": events_data,
        }
    except Exception as exc:
        logger.error("获取线索追踪全貌异常: clueId=%s, error=%s", clueId, exc, exc_info=True)
        return {
            "clueId": clueId,
            "state": "unknown",
            "detail": None,
            "events": [],
        }


@router.post("/clue-events", summary="查询线索事件溯源记录 (POST)")
@router.get("/clue-events", summary="查询线索事件溯源记录 (GET)")
async def get_clue_events_endpoint(
    payload: Optional[Dict[str, Any]] = Body(None),
    clueId: Optional[str] = Query(None, description="线索ID"),
    clue_id: Optional[str] = Query(None, description="线索ID别名"),
):
    target_id = None
    if payload and isinstance(payload, dict):
        target_id = payload.get("clueId") or payload.get("clue_id")
    if not target_id:
        target_id = clueId or clue_id
    if not target_id:
        raise HTTPException(status_code=422, detail="clueId is required")

    events = await get_clue_events(target_id)
    events_data = [e.model_dump() for e in events]
    return {"message": "Clue events retrieved successfully", "code": 0, "data": events_data}


class QueryContextRequest(BaseModel):
    clueId: Optional[str] = Field(None, description="线索ID")
    clue_id: Optional[str] = Field(None, description="线索ID别名")


@router.post("/clue-context", summary="查询线索排期上下文")
@router.get("/clue-context", summary="查询线索排期上下文")
async def query_context(
    payload: Optional[QueryContextRequest] = Body(None),
    clueId: Optional[str] = Query(None, description="线索ID"),
    clue_id: Optional[str] = Query(None, description="线索ID别名"),
):
    target_clue_id = (payload.clueId or payload.clue_id) if payload else None
    if not target_clue_id:
        target_clue_id = clueId or clue_id
    if not target_clue_id:
        raise HTTPException(status_code=422, detail="clueId is required")
    context = await ClueContext.from_db(target_clue_id)
    return context.model_dump()


@router.get("/sample-clues", summary="按小时抽样线索ID")
async def sample_clues_by_hour(
    hour: int = Query(..., ge=0, le=23, description="抽样小时 (0-23)"),
    date_str: Optional[str] = Query(None, description="日期 YYYY-MM-DD，默认当天"),
    size: int = Query(20, ge=1, le=1000, description="抽样条数"),
):
    target_date = date_str or get_now().strftime("%Y-%m-%d")
    hour_str = f"{hour:02d}"
    start_time = f"{target_date} {hour_str}:00:00"
    end_time = f"{target_date} {hour_str}:59:59"

    clues = await ClueInfo.filter(
        received_at__gte=start_time,
        received_at__lte=end_time,
    ).limit(size).values_list("clue_id", flat=True)
    clue_ids = list(clues)
    return {"code": 0, "msg": "success", "total": len(clue_ids), "data": clue_ids}
