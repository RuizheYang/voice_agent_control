import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Query

from app.dbmodels.models import CallRecord, ClueInfo, ExecutionTask

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/analytics",
    tags=["analytics"],
)


@router.get("/dashboard-stats", summary="获取调度与外呼大盘统计指标")
async def get_dashboard_stats(
    start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
):
    """基于 Tortoise ORM 聚合指定时间范围内的线索状态、外呼接通与任务执行统计（兼容 SQLite 与 MySQL）。"""
    try:
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        start_dt = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return {"code": 3, "msg": "日期格式错误，请使用 YYYY-MM-DD", "data": None}

    try:
        clues = await ClueInfo.filter(create_time__gte=start_dt, create_time__lte=end_dt).all()
        calls = await CallRecord.filter(create_time__gte=start_dt, create_time__lte=end_dt).all()
        tasks = await ExecutionTask.filter(create_time__gte=start_dt, create_time__lte=end_dt).all()

        status_counts: dict[str, int] = defaultdict(int)
        workflow_counts: dict[str, int] = defaultdict(int)
        daily_bucket: dict[str, dict[str, int]] = defaultdict(
            lambda: {"new_clues": 0, "calls": 0, "answered_calls": 0, "accepted": 0, "rejected": 0}
        )

        for clue in clues:
            status_counts[clue.status] += 1
            workflow_counts[clue.workflow_id or "default"] += 1
            day = clue.create_time.strftime("%Y-%m-%d") if clue.create_time else start_date
            daily_bucket[day]["new_clues"] += 1
            if clue.status in ("accepted", "completed"):
                daily_bucket[day]["accepted"] += 1
            elif clue.status in ("rejected", "cancelled", "failed"):
                daily_bucket[day]["rejected"] += 1

        answered_count = 0
        for call in calls:
            day = call.create_time.strftime("%Y-%m-%d") if call.create_time else start_date
            daily_bucket[day]["calls"] += 1
            if call.answered == 1:
                answered_count += 1
                daily_bucket[day]["answered_calls"] += 1

        inbox_pending = sum(1 for t in tasks if t.task_type == "call_end_inbox" and t.status in ("pending", "retry"))
        wakeup_pending = sum(1 for t in tasks if t.task_type == "agent_wakeup" and t.status in ("pending", "retry"))

        daily_trend = [
            {"date": day, **metrics}
            for day, metrics in sorted(daily_bucket.items(), key=lambda item: item[0])
        ]

        return {
            "code": 0,
            "msg": "success",
            "data": {
                "date_range": f"{start_date} ~ {end_date}",
                "total_clues": len(clues),
                "total_calls": len(calls),
                "answered_calls": answered_count,
                "status_breakdown": dict(status_counts),
                "workflow_breakdown": dict(workflow_counts),
                "inbox_pending": inbox_pending,
                "wakeup_pending": wakeup_pending,
                "daily_trend": daily_trend,
            },
        }
    except Exception as exc:
        logger.error("获取大盘统计指标失败: %s", exc, exc_info=True)
        return {"code": 3, "msg": str(exc), "data": None}
