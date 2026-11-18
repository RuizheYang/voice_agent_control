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
    获取当前系统的标准时间。永远返回 Naive Datetime (不带时区，但数值是北京时间)。
    """
    # 1. 获取绝对准确的当前 UTC 时间
    utc_now = datetime.now(ZoneInfo("UTC"))
    
    # 2. 转换为东八区时间
    cst_now = utc_now.astimezone(LOCAL_TZ)
    
    return cst_now

