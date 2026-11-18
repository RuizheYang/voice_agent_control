from __future__ import annotations

import logging
from datetime import datetime, time, timedelta
from typing import Optional, Dict, Any, List

from app.utils.time_utils import ensure_tz_aware, get_now

logger = logging.getLogger(__name__)


def parse_time_str(time_str: str) -> time:
    """解析 HH:MM:SS 或 HH:MM 为 time 对象"""
    parts = [int(p) for p in time_str.strip().split(":")]
    if len(parts) == 2:
        return time(parts[0], parts[1], 0)
    return time(parts[0], parts[1], parts[2])


def is_within_working_window(
    dt: datetime,
    work_start: str = "09:00:00",
    work_end: str = "21:00:00"
) -> bool:
    """判断给定时间是否落在法定工时窗口内"""
    aware_dt = ensure_tz_aware(dt)
    target_time = aware_dt.time()
    start_t = parse_time_str(work_start)
    end_t = parse_time_str(work_end)
    return start_t <= target_time < end_t


def snap_to_working_window(
    dt: datetime,
    work_start: str = "09:00:00",
    work_end: str = "21:00:00",
    morning_buffer_minutes: int = 30,
) -> datetime:
    """
    确定性安全吸附投影器 (Safety Snapper):
    将超出法定工时 (09:00 - 21:00) 的时间平滑投影至合法开工时间：
    - 早于 09:00: 投影至当日 09:30 (带30分钟安全缓冲)
    - 晚于 21:00: 投影至次日 09:30
    """
    aware_dt = ensure_tz_aware(dt)
    target_time = aware_dt.time()
    start_t = parse_time_str(work_start)
    end_t = parse_time_str(work_end)

    if start_t <= target_time < end_t:
        return aware_dt

    if target_time < start_t:
        # 早晨未开工，吸附到当日开工加缓冲
        snapped = aware_dt.replace(
            hour=start_t.hour,
            minute=morning_buffer_minutes,
            second=0,
            microsecond=0,
        )
        logger.info(f"[SafetySnapper] 时间 {aware_dt} 早于营业开始，吸附至当日: {snapped}")
        return snapped
    else:
        # 晚间已歇业，吸附到次日开工加缓冲
        next_day = aware_dt + timedelta(days=1)
        snapped = next_day.replace(
            hour=start_t.hour,
            minute=morning_buffer_minutes,
            second=0,
            microsecond=0,
        )
        logger.info(f"[SafetySnapper] 时间 {aware_dt} 晚于营业截止，吸附至次日: {snapped}")
        return snapped


def calculate_relative_time(
    base_time: Optional[datetime] = None,
    hours: float = 0,
    days: int = 0,
    minutes: int = 0,
    target_hour: Optional[int] = None,
    target_minute: Optional[int] = None,
) -> datetime:
    """
    确定性相对时间计算工具 (供 Agent 或 Harness 计算绝对时间)
    """
    ref = ensure_tz_aware(base_time or get_now())
    shifted = ref + timedelta(days=days, hours=hours, minutes=minutes)

    if target_hour is not None:
        minute_val = target_minute if target_minute is not None else 0
        shifted = shifted.replace(hour=target_hour, minute=minute_val, second=0, microsecond=0)

    return shifted


def format_conversation_for_prompt(messages: List[Dict[str, Any]] | List[Any]) -> str:
    """将对话历史转换为格式化文本供 Prompt 输入"""
    if not messages:
        return "[无通话记录 / 电话未接通]"

    lines = []
    for idx, msg in enumerate(messages, 1):
        if isinstance(msg, dict):
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
        else:
            role = getattr(msg, "role", "unknown")
            content = getattr(msg, "content", "")
        
        role_label = "AI销售" if role in ("assistant", "agent") else ("客户" if role in ("user", "customer") else role)
        lines.append(f"第 {idx} 轮 [{role_label}]: {content}")

    return "\n".join(lines)
