from __future__ import annotations

from enum import Enum
from typing import Any, Optional


class CallOutcome(str, Enum):
    """标准化通话意图枚举 (专注排期分支判定)"""
    INITIAL = "initial"                                      # 初始状态（首呼前，无历史记录）
    UNANSWERED = "unanswered"                                # 未接通 (500/508, 500/510)
    UNANSWERED_NEED_WORK_PHONE = "unanswered_need_work_phone"# 未接通，需要切换为工作手机 (500/501)
    CONNECTED = "connected"                                  # 已接通但是需要核对 (500/507, 以及 200/600/700 等常规接通)
    BUSY_OR_REJECTED = "busy_or_rejected"                    # 拒接 (500/509)


class InflowSlot(str, Enum):
    """线索进件时段分类"""
    WORKING = "working"      # 工作时段 (09:00–21:00)
    EVENING = "evening"      # 晚间非工作时段 (21:00–23:59)
    MIDNIGHT = "midnight"    # 凌晨时段 (00:00–08:59)


class CallOutcomeClassifier:
    """
    轻量通话结果解析器：
    标准业务码值映射：
    1. 500/509：拒接 (BUSY_OR_REJECTED)
    2. 500/508、500/510：未接通 (UNANSWERED)
    3. 500/501：未接通，需要切换为工作手机 (UNANSWERED_NEED_WORK_PHONE)
    4. 500/507：已接通但是需要核对 (CONNECTED)
    兼容规则：
    - callAnswered is True 或 一级码为 200/600/700 等：归为 CONNECTED
    - callAnswered is False：归为 UNANSWERED
    """

    @staticmethod
    def classify(last_record: Any) -> CallOutcome:
        if not last_record:
            return CallOutcome.INITIAL

        if isinstance(last_record, dict):
            one_code = last_record.get("oneLevelCode")
            two_code = last_record.get("twoLevelCode")
            call_answered = last_record.get("callAnswered")
        else:
            one_code = getattr(last_record, "oneLevelCode", None)
            two_code = getattr(last_record, "twoLevelCode", None)
            call_answered = getattr(last_record, "callAnswered", None)

        two_code_str = str(two_code).strip() if two_code is not None else ""
        one_code_str = str(one_code).strip() if one_code is not None else ""

        # 1. 拒接：500/509
        if two_code_str in {"509"}:
            return CallOutcome.BUSY_OR_REJECTED

        # 2. 未接通，需要切换为工作手机：500/501
        if two_code_str == "501":
            return CallOutcome.UNANSWERED_NEED_WORK_PHONE

        # 3. 未接通：500/508、500/510
        if two_code_str in {"508", "510"}:
            return CallOutcome.UNANSWERED

        # 4. 已接通但是需要核对：500/507，以及明确接通(callAnswered is True)或常规接通码值(200/600/700)
        if two_code_str == "507" or call_answered is True or one_code_str in {"200", "600", "700"}:
            return CallOutcome.CONNECTED

        if call_answered is False:
            return CallOutcome.UNANSWERED

        return CallOutcome.UNANSWERED

    @staticmethod
    def is_rejected(record: Any) -> bool:
        """判断是否为拒接 (500/509)"""
        return CallOutcomeClassifier.classify(record) == CallOutcome.BUSY_OR_REJECTED

    @staticmethod
    def is_unanswered(record: Any) -> bool:
        """判断是否为未接通 (包括 500/508, 500/510 以及 500/501)"""
        outcome = CallOutcomeClassifier.classify(record)
        return outcome in {CallOutcome.UNANSWERED, CallOutcome.UNANSWERED_NEED_WORK_PHONE}

    @staticmethod
    def is_need_work_phone(record: Any) -> bool:
        """判断是否需要切换工作手机 (500/501)"""
        return CallOutcomeClassifier.classify(record) == CallOutcome.UNANSWERED_NEED_WORK_PHONE

    @staticmethod
    def is_connected_need_verify(record: Any) -> bool:
        """判断是否为已接通但是需要核对 (500/507)"""
        if not record:
            return False
        two_code = record.get("twoLevelCode") if isinstance(record, dict) else getattr(record, "twoLevelCode", None)
        return str(two_code).strip() == "507"

    @staticmethod
    def is_connected(record: Any) -> bool:
        """判断是否为已接通 (500/507 或常规接通)"""
        return CallOutcomeClassifier.classify(record) == CallOutcome.CONNECTED
