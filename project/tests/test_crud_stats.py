import unittest
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, patch, MagicMock
from app.utils.time_utils import get_now, ensure_tz_aware
from app.dbmodels.crud import get_clue_history_stats
from app.dbmodels.models import ClueInfo, CallRecord


class TestCrudStats(unittest.IsolatedAsyncioTestCase):

    @patch.object(ClueInfo, "filter")
    @patch.object(CallRecord, "filter")
    async def test_get_clue_history_stats_clue_not_found(self, mock_record_filter, mock_clue_filter):
        """测试线索不存在的情况，应抛出 ValueError"""
        mock_record_filter.return_value.order_by = AsyncMock(return_value=[])
        mock_clue_filter.return_value.first = AsyncMock(return_value=None)
        
        with self.assertRaises(ValueError):
            await get_clue_history_stats("fake_id")

    @patch.object(ClueInfo, "filter")
    @patch.object(CallRecord, "filter")
    async def test_get_clue_history_stats_zero_calls(self, mock_record_filter, mock_clue_filter):
        """测试线索存在，但没有任何历史通话记录的情况"""
        mock_record_filter.return_value.order_by = AsyncMock(return_value=[])
        
        clue_time = ensure_tz_aware(datetime(2026, 6, 5, 10, 0, 0))
        mock_clue = MagicMock()
        mock_clue.received_at = clue_time
        mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)
        
        stats = await get_clue_history_stats("test_id")
        
        self.assertEqual(stats["total_call_count"], 0)
        self.assertEqual(stats["continuous_no_answer_count"], 0)
        self.assertEqual(stats["continuous_no_answer_hours"], 0)
        self.assertEqual(stats["clue_in_time"], clue_time)

    @patch.object(ClueInfo, "filter")
    @patch.object(CallRecord, "filter")
    async def test_get_clue_history_stats_mixed_calls(self, mock_record_filter, mock_clue_filter):
        """测试复杂的连续未接通跳出逻辑"""
        call_end_time = datetime(2026, 6, 5, 12, 0, 0)
        in_time = call_end_time - timedelta(hours=10)
        
        call1 = MagicMock()
        call1.answered = 0
        call1.create_time = call_end_time - timedelta(hours=1)

        call2 = MagicMock()
        call2.answered = 0
        call2.create_time = call_end_time - timedelta(hours=2)

        call3 = MagicMock()
        call3.answered = 1
        call3.create_time = call_end_time - timedelta(hours=4)

        call4 = MagicMock()
        call4.answered = 0
        call4.create_time = call_end_time - timedelta(hours=5)

        mock_record_filter.return_value.order_by = AsyncMock(return_value=[call1, call2, call3, call4])
        
        mock_clue = MagicMock()
        mock_clue.received_at = in_time
        mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)
        
        stats = await get_clue_history_stats("test_id", call_end_time=call_end_time)
        
        self.assertEqual(stats["total_call_count"], 4)
        self.assertEqual(stats["continuous_no_answer_count"], 2)
        self.assertEqual(stats["continuous_no_answer_hours"], 4)

    @patch.object(ClueInfo, "filter")
    @patch.object(CallRecord, "filter")
    async def test_get_clue_history_stats_all_no_answers(self, mock_record_filter, mock_clue_filter):
        """测试全是未接通电话的情况, 计算起跳点应使用线索入库时间"""
        base_time = datetime(2026, 6, 5, 12, 0, 0)
        in_time = base_time - timedelta(hours=8)
        
        call1 = MagicMock()
        call1.answered = 0
        call1.create_time = base_time - timedelta(hours=1)

        call2 = MagicMock()
        call2.answered = 0
        call2.create_time = base_time - timedelta(hours=2)

        mock_record_filter.return_value.order_by = AsyncMock(return_value=[call1, call2])
        
        mock_clue = MagicMock()
        mock_clue.received_at = in_time
        mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)
        
        stats = await get_clue_history_stats("test_id", call_end_time=base_time)
        
        self.assertEqual(stats["total_call_count"], 2)
        self.assertEqual(stats["continuous_no_answer_count"], 2)
        self.assertEqual(stats["continuous_no_answer_hours"], 8)

    @patch.object(ClueInfo, "filter")
    @patch.object(CallRecord, "filter")
    async def test_get_clue_history_stats_base_time_none(self, mock_record_filter, mock_clue_filter):
        """测试 base_time 为 None 的默认情况"""
        mock_record_filter.return_value.order_by = AsyncMock(return_value=[])
        
        mock_clue = MagicMock()
        mock_clue.received_at = datetime.now(ZoneInfo("Asia/Shanghai")) - timedelta(hours=2)
        mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)
        
        stats = await get_clue_history_stats("test_id", call_end_time=None)
        
        self.assertEqual(stats["total_call_count"], 0)
        self.assertGreaterEqual(stats["continuous_no_answer_hours"], 0)

    @patch.object(ClueInfo, "filter")
    @patch.object(CallRecord, "filter")
    async def test_get_clue_history_stats_timezone_mixed(self, mock_record_filter, mock_clue_filter):
        """测试时区混用的情况，避免 offset-naive 和 offset-aware 之间的相减报错"""
        base_time_aware = datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc)
        in_time_naive = datetime(2026, 6, 5, 8, 0, 0)
        
        call1 = MagicMock()
        call1.answered = 0
        call1.create_time = in_time_naive + timedelta(hours=1)

        mock_record_filter.return_value.order_by = AsyncMock(return_value=[call1])
        
        mock_clue = MagicMock()
        mock_clue.received_at = in_time_naive
        mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)
        
        stats = await get_clue_history_stats("test_id", call_end_time=base_time_aware)
        self.assertEqual(stats["continuous_no_answer_hours"], 4)

    @patch.object(ClueInfo, "filter")
    @patch.object(CallRecord, "filter")
    async def test_get_clue_history_stats_all_connected(self, mock_record_filter, mock_clue_filter):
        """测试通话全是接通状态"""
        base_time = datetime(2026, 6, 5, 12, 0, 0)
        in_time = base_time - timedelta(hours=10)
        
        call1 = MagicMock()
        call1.answered = 1
        call1.create_time = base_time - timedelta(hours=2)

        call2 = MagicMock()
        call2.answered = 1
        call2.create_time = base_time - timedelta(hours=4)

        mock_record_filter.return_value.order_by = AsyncMock(return_value=[call1, call2])
        
        mock_clue = MagicMock()
        mock_clue.received_at = in_time
        mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)
        
        stats = await get_clue_history_stats("test_id", call_end_time=base_time)
        
        self.assertEqual(stats["total_call_count"], 2)
        self.assertEqual(stats["continuous_no_answer_count"], 0)
        self.assertEqual(stats["continuous_no_answer_hours"], 2)

    @patch.object(ClueInfo, "filter")
    @patch.object(CallRecord, "filter")
    async def test_get_clue_history_stats_negative_delta(self, mock_record_filter, mock_clue_filter):
        """测试边界值：差值为负时的安全保护"""
        base_time = datetime(2026, 6, 5, 12, 0, 0)
        
        call1 = MagicMock()
        call1.answered = 1
        call1.create_time = base_time + timedelta(hours=1)

        mock_record_filter.return_value.order_by = AsyncMock(return_value=[call1])
        
        mock_clue = MagicMock()
        mock_clue.received_at = base_time - timedelta(hours=10)
        mock_clue_filter.return_value.first = AsyncMock(return_value=mock_clue)
        
        stats = await get_clue_history_stats("test_id", call_end_time=base_time)
        self.assertEqual(stats["continuous_no_answer_hours"], 0)


if __name__ == "__main__":
    unittest.main()
