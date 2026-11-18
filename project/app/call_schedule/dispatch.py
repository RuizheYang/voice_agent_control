"""定时任务只进入线索处理入口，不直接调用送号。"""
from app.call_schedule.contracts import ScheduledCallRequest


async def dispatch_scheduled_call(request: ScheduledCallRequest):
    # 延迟导入，避免 ClueHandler -> ScheduledTaskHandler -> service 循环依赖。
    from app.clue_handler.factory import ClueHandlerFactory

    handler = await ClueHandlerFactory.get_handler_by_clue(request.clueId)
    await handler.on_scheduled_task(request)
