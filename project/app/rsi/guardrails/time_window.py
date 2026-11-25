"""Pure timezone-aware compliance working-window helpers for Global Outbox guardrails."""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo


def to_utc(dt: datetime) -> datetime:
    """Normalize naive or aware datetime cleanly to UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_time_str(time_str: str) -> time:
    """Parse HH:MM or HH:MM:SS string into datetime.time."""
    parts = [int(p) for p in str(time_str).strip().split(":")]
    if len(parts) == 2:
        return time(parts[0], parts[1], 0)
    return time(parts[0], parts[1], parts[2])


def resolve_working_window(
    policy_config: Optional[Dict[str, Any]] = None,
    *,
    default_start: str = "09:00:00",
    default_end: str = "21:00:00",
    default_tz: str = "Asia/Shanghai",
) -> Tuple[str, str, ZoneInfo]:
    """Extract (work_start, work_end, ZoneInfo) safely from policy config."""
    cfg = policy_config if isinstance(policy_config, dict) else {}
    window_cfg = cfg.get("working_window") if isinstance(cfg.get("working_window"), dict) else cfg

    start_val = str(
        window_cfg.get("work_start_time")
        or window_cfg.get("workStartTime")
        or cfg.get("work_start_time")
        or default_start
    )
    end_val = str(
        window_cfg.get("work_end_time")
        or window_cfg.get("workEndTime")
        or cfg.get("work_end_time")
        or default_end
    )
    tz_name = str(
        window_cfg.get("timezone")
        or cfg.get("timezone")
        or default_tz
    )

    try:
        parse_time_str(start_val)
        parse_time_str(end_val)
    except Exception:
        start_val, end_val = default_start, default_end

    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo(default_tz)

    return start_val, end_val, tz


def is_within_working_window(
    dt: datetime,
    work_start: str = "09:00:00",
    work_end: str = "21:00:00",
    tz: Optional[ZoneInfo] = None,
) -> bool:
    """Return True iff dt in target local timezone falls inside [work_start, work_end)."""
    zone = tz or ZoneInfo("Asia/Shanghai")
    local_dt = to_utc(dt).astimezone(zone)
    start_t = parse_time_str(work_start)
    end_t = parse_time_str(work_end)
    return start_t <= local_dt.time() < end_t


def next_working_window_opening(
    dt: datetime,
    work_start: str = "09:00:00",
    work_end: str = "21:00:00",
    tz: Optional[ZoneInfo] = None,
    *,
    force_future: bool = False,
) -> datetime:
    """Return UTC datetime of next working-window opening (optionally strictly future day if inside window)."""
    zone = tz or ZoneInfo("Asia/Shanghai")
    utc_dt = to_utc(dt)
    local_dt = utc_dt.astimezone(zone)
    start_t = parse_time_str(work_start)
    end_t = parse_time_str(work_end)

    if local_dt.time() < start_t:
        opening = datetime.combine(local_dt.date(), start_t, zone)
        return opening.astimezone(timezone.utc)
    if local_dt.time() >= end_t or force_future:
        opening = datetime.combine(local_dt.date() + timedelta(days=1), start_t, zone)
        return opening.astimezone(timezone.utc)
    return utc_dt


def snap_to_working_window(
    dt: datetime,
    work_start: str = "09:00:00",
    work_end: str = "21:00:00",
    tz: Optional[ZoneInfo] = None,
    morning_buffer_minutes: int = 30,
) -> datetime:
    """Snap outside-window datetime to next valid opening + safe morning buffer (returns UTC)."""
    zone = tz or ZoneInfo("Asia/Shanghai")
    utc_dt = to_utc(dt)
    local_dt = utc_dt.astimezone(zone)
    start_t = parse_time_str(work_start)
    end_t = parse_time_str(work_end)

    if start_t <= local_dt.time() < end_t:
        return utc_dt

    buffer = timedelta(minutes=max(0, int(morning_buffer_minutes)))
    target_date = local_dt.date() if local_dt.time() < start_t else (local_dt.date() + timedelta(days=1))
    opening = datetime.combine(target_date, start_t, zone)
    window_close = datetime.combine(target_date, end_t, zone)
    snapped = opening + buffer
    if snapped >= window_close:
        snapped = opening
    return snapped.astimezone(timezone.utc)
