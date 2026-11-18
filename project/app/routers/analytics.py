# app/routers/analytics.py
from fastapi import APIRouter, Query
from tortoise import Tortoise
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/analytics",
    tags=["analytics"]
)

@router.get("/dashboard-stats")
async def get_dashboard_stats(
    start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD")
):
    """获取指定时间范围内的数据分析统计及每日趋势"""
    
    # 1. 时间参数处理
    try:
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return {"code": 0, "msg": "日期格式错误，请使用 YYYY-MM-DD", "data": None}

    start_time_str = f"{start_date} 00:00:00"
    end_time_str = f"{end_date} 23:59:59"
    time_filter = f" AND create_time >= '{start_time_str}' AND create_time <= '{end_time_str}'"
    
    conn = Tortoise.get_connection("default")
    
    # 🌟 核心：子查询补丁 - 判断同一个 clueId 下，是否存在极限挽回成功的 Event 记录
    late_accept_exists_sql = """
        clueId in (
            SELECT clueId FROM clueevent e2 
            WHERE e2.eventType = 'state_transfer' 
              AND e2.eventData LIKE '%"from":%"rejected"%' 
              AND e2.eventData LIKE '%"to":%"accepted"%' 
              AND e2.eventData LIKE '%"reason":%"invalid_transition"%' 
              AND e2.eventData LIKE '%"success":%false%'
              union 
              SELECT clueId FROM clueevent e2 
            WHERE e2.eventType = 'state_transfer' 
              AND e2.eventData LIKE '%"from":%"wait_for_cancel"%' 
              AND e2.eventData LIKE '%"to":%"accepted"%' 
              AND e2.eventData LIKE '%"reason":%"invalid_transition"%' 
              AND e2.eventData LIKE '%"success":%false%'
        )
    """

    try:
        # ==================== 指标查询区 ====================
        
        # 1a. Cancel 抽单总数
        res_cancel = await conn.execute_query_dict(
            f"SELECT count(*) as cnt FROM clueevent WHERE eventType = 'cancel_clue'{time_filter};"
        )

        # 1b. 抽单导致加微流失 (同一个 clueId，既有 cancel=true，又有挽回成功)
        res_cancel_loss = await conn.execute_query_dict(
            f"""
            SELECT count(DISTINCT clueId) as cnt FROM clueevent 
            WHERE eventType = 'sync_clue_status' 
              AND (eventData LIKE '% "close_reason": "2"%' or eventData LIKE '% "close_reason": "3"%')
              AND {late_accept_exists_sql}
              {time_filter};
            """
        )

        # 2a. Pending timeout 总数量
        res_pending = await conn.execute_query_dict(
            f"SELECT count(*) as cnt FROM clueevent WHERE eventType = 'pending_timeout'{time_filter};"
        )

        # 2b. Pending timeout 过短导致加微流失 (同一个 clueId，既有 pending_timeout，又有挽回成功)
        res_pending_success = await conn.execute_query_dict(
            f"""
            SELECT count(DISTINCT clueId) as cnt FROM clueevent 
            WHERE eventType = 'pending_timeout' 
              AND {late_accept_exists_sql}
              {time_filter};
            """
        )

        # 3. 因排次呼失败导致的线索结束
        res_schedule_fail = await conn.execute_query_dict(
            f"SELECT count(*) as cnt FROM clueevent WHERE eventData LIKE '%\"response_code\": 500%'{time_filter};"
        )

        # 4. 因发加微请求报错导致的线索结束
        res_wechat_error = await conn.execute_query_dict(
            f"""
            SELECT count(*) as cnt FROM clueevent 
            WHERE eventType = 'send_wechat_add_request' 
              AND JSON_EXTRACT(eventData, '$.response_code') != 0
              AND JSON_EXTRACT(eventData, '$.response_code') IS NOT NULL{time_filter};
            """
        )

        # ==================== 每日趋势查询 ====================
        res_trend = await conn.execute_query_dict(
            f"""
            SELECT 
                DATE(create_time) as date_str,
                SUM(CASE WHEN eventType = 'cancel_clue' THEN 1 ELSE 0 END) as cancel_cnt,
                SUM(CASE WHEN eventType = 'cancel_clue' AND eventData LIKE '%"success":%true%' AND {late_accept_exists_sql} THEN 1 ELSE 0 END) as cancel_loss_cnt,
                SUM(CASE WHEN eventType = 'pending_timeout' THEN 1 ELSE 0 END) as pending_cnt,
                SUM(CASE WHEN eventType = 'pending_timeout' AND {late_accept_exists_sql} THEN 1 ELSE 0 END) as pending_success_cnt,
                SUM(CASE WHEN eventData LIKE '%"response_code": 500%' THEN 1 ELSE 0 END) as schedule_fail_cnt,
                SUM(CASE WHEN eventType = 'send_wechat_add_request' AND JSON_EXTRACT(eventData, '$.response_code') != 0 THEN 1 ELSE 0 END) as wechat_error_cnt
            FROM clueevent 
            WHERE create_time >= '{start_time_str}' AND create_time <= '{end_time_str}'
            GROUP BY DATE(create_time)
            ORDER BY date_str ASC;
            """
        )
        
        daily_trend = [
            {
                "date": str(row['date_str']),
                "cancel": int(row['cancel_cnt'] or 0),
                "cancel_loss": int(row['cancel_loss_cnt'] or 0),
                "pending": int(row['pending_cnt'] or 0),
                "pending_success": int(row['pending_success_cnt'] or 0),
                "schedule_fail": int(row['schedule_fail_cnt'] or 0),
                "wechat_error": int(row['wechat_error_cnt'] or 0)
            } for row in res_trend
        ]

        return {
            "code": 1,
            "msg": "success",
            "data": {
                "date_range": f"{start_date} 至 {end_date}",
                "cancel_count": res_cancel[0]['cnt'],
                "cancel_loss_count": res_cancel_loss[0]['cnt'],
                "pending_timeout_count": res_pending[0]['cnt'],
                "pending_success_count": res_pending_success[0]['cnt'],
                "schedule_fail_count": res_schedule_fail[0]['cnt'],
                "wechat_error_count": res_wechat_error[0]['cnt'],
                "daily_trend": daily_trend
            }
        }
        
    except Exception as e:
        logger.error(f"获取分析数据失败: {e}", exc_info=True)
        return {"code": 0, "msg": str(e), "data": None}