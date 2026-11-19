from datetime import datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo
from typing import Optional

# 定义你们业务的默认时区（北京时间）
LOCAL_TZ = ZoneInfo("Asia/Shanghai")

def ensure_tz_aware(dt: Optional[datetime], default_tz=LOCAL_TZ) -> Optional[datetime]:
    """
    时区安全对齐核心工具：
    将无时区的时间赋予默认时区，有时间的转化为目标时区。
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        # 如果没有时区（naive），假定它是本地时间
        return dt.replace(tzinfo=default_tz)
    # 如果有时区（aware），将其安全转换到目标时区
    return dt.astimezone(default_tz)

def get_now() -> datetime:
    """
    【系统中央时钟】
    获取当前系统的标准时间，统一返回带 Asia/Shanghai 时区的 Aware Datetime。
    """
    utc_now = datetime.now(ZoneInfo("UTC"))
    return utc_now.astimezone(LOCAL_TZ)

