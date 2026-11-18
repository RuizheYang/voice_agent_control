import logging
import asyncio
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional, Any, Dict
from fastapi import APIRouter, Body, BackgroundTasks, Query, HTTPException, Request
from pydantic import BaseModel, Field

from app.clue_handler.state import ClueState as ClueStateValue
from app.dbmodels.crud import (
    update_state_no_lock, get_call_history, get_clue_state, iter_clue_state_batches,
    get_clue_stats, get_clue_events, get_clue_state_record, list_clue_events_timeline,
    create_call_end_record_mysql, get_clue_handler_name
)
from app.dbmodels.models import ClueInfo, CallRecord, ClueState, EndCallRecord
from app.dbmodels.crud import (
    create_call_end_record, create_clue, prepare_call_params, get_clue_intake_time,
    get_clue_meta, create_call_record
)
from app.redis_client.redis_manager import redis_manager
from app.schemas import CallEndRequest, CallEndResponse, ClueCreateRequest, ClueCreateResponse
from app.schemas.batch_process import BatchInitRequest, BatchCancelRequest, BatchResetRequest
from app.strategy.contracts import ClueContext
from app.utils.time_utils import get_now 
from app.clue_handler.factory import ClueHandlerFactory

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/database",
    tags=["database"],
    responses={404: {"description": "Not found"}},
)



@router.post("/clue", response_model=ClueCreateResponse)
async def save_clue(request: ClueCreateRequest) -> ClueCreateResponse:
    """接入系统推入了一条新线索, 推入到redis慢慢处理"""
    logger.info(f"接收到新线索: {request.clueId}")
    normalized = request.to_normalized()
    await create_clue(normalized)
    return ClueCreateResponse(message="Clue created successfully", code=1)

@router.post("/call_end", response_model=CallEndResponse)
async def save_call_end_info(request: CallEndRequest) -> CallEndResponse:
    """记录外呼结束信息"""
    logger.info(f"接收到外呼结束信息")
    await create_call_end_record(request)
    await create_call_end_record_mysql(request)
    return CallEndResponse(message="Call end info recorded successfully", code = 1)
    
@router.post("/call_params")
async def get_call_params(clueId:str):
    """根据线索ID准备外呼参数"""
    logger.info(f"准备外呼参数, clueId: {clueId}")
    params = await prepare_call_params(clueId)
    return params

@router.post("/update_state")
async def update_clue_state_endpoint(clueId:str, state:str):
    """更新线索状态（不带分布式锁，不检测状态机合法性，用于紧急更新状态，谨慎使用）"""
    logger.info(f"更新线索状态, clueId: {clueId}, state: {state}")
    success = await update_state_no_lock(clueId, state)
    if success:
        return {"message": "Clue state updated successfully", "code": 1}
    else:
        return {"message": "Failed to update clue state", "code": 0}


@router.get("/clue_state", summary="获取线索当前状态、画像、通联与事件全貌")
@router.get("/clue-trace", summary="[别名] 获取线索全链路状态与事件追踪全貌")
async def fetch_clue_state(clueId: str):
    """
    单接口全链路排障聚合接口：
    一次性返回线索当前状态、基础画像、历次通话记录与全量流转事件，
    避免客户端多次并发请求增加数据库连接压力。
    """
    try:
        from app.utils.decrypt import crypted_manager

        clue = await ClueInfo.filter(clue_id=clueId).first()
        if not clue:
            return {
                "clueId": clueId,
                "state": "未知 (未创建/暂无状态)",
                "detail": None,
                "events": [],
            }

        state = clue.status

        # 1. 手机号与姓名解密处理 (安全兜底)
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

        # 2. 查询最近 10 通通话记录
        calls = await CallRecord.filter(clue_id=clueId).order_by("-create_time").limit(10)
        call_records_data = []
        for c in calls:
            decision_res = c.decision_result if isinstance(c.decision_result, dict) else {}
            call_records_data.append({
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
            })

        # 3. 查询全量事件流
        events = await get_clue_events(clueId)
        events_data = [e.model_dump() for e in events]

        city = None
        if isinstance(clue.agent_data, dict):
            city = clue.agent_data.get("customerCity")

        detail = {
            "clue_id": clue.clue_id,
            "status": clue.status,
            "state_version": clue.state_version,
            "workflow_id": clue.workflow_id,
            "agent_id": clue.agent_id,
            "strategy_id": clue.strategy_id,
            "customer_name": name,
            "customer_city": city,
            "phone_number": phone,
            "total_call_count": clue.total_call_count,
            "next_call_at": clue.next_call_at.strftime("%Y-%m-%d %H:%M:%S") if clue.next_call_at else None,
            "close_reason": clue.close_reason,
            "end_code": clue.end_code,
            "create_time": clue.create_time.strftime("%Y-%m-%d %H:%M:%S") if clue.create_time else None,
            "update_time": clue.update_time.strftime("%Y-%m-%d %H:%M:%S") if clue.update_time else None,
            "call_records": call_records_data,
        }

        return {
            "clueId": clueId,
            "state": state,
            "detail": detail,
            "events": events_data,
        }
    except Exception as e:
        logger.error(f"获取线索状态画像异常: clueId={clueId}, error={e}", exc_info=True)
        return {
            "clueId": clueId,
            "state": "未知 (未创建/暂无状态)",
            "detail": None,
            "events": [],
        }
        

@router.post("/admin/force-unlock")
async def force_unlock_clue(clueId: str):
    """
    【高危管理接口】强制破拆线索的分布式锁。
    用途：用于解决极端情况下残留的死锁问题。
    原理：直接删除 Redis 里的分布式锁键（包含状态流转锁与处理锁）。
    """
    lock_key = f"lock:clue:{clueId}"
    process_lock_key = f"lock:clue:process:{clueId}"
    
    # 强制删除 Redis 里的底层 key
    r1 = await redis_manager.delete(lock_key)
    r2 = await redis_manager.delete(process_lock_key)
    
    if r1 or r2:
        logger.warning(f"🚨【高危操作】人工强行破拆了线索锁: {lock_key}, {process_lock_key}")
        return {"message": f"成功破拆锁 {lock_key} / {process_lock_key}。", "code": 1}
    else:
        return {"message": f"操作完成，锁 {lock_key} 本身不存在或已自动释放。", "code": 1}


@router.post("/admin/reset_state")
async def force_reset_clue_state(clueId: str, state: str = "rejected"):
    """
    【综合救火接口】一键破拆锁并重置状态。
    这是日常运维恢复兜底手段。先强行清理 Redis 锁，再更新数据库状态，保证逻辑不被锁阻挡。
    """
    logger.warning(f"🚑【火线救援】发起线索联动重置: {clueId} 目标状态: {state}")
    
    # 步骤 1: 拔出物理锁（状态锁与处理锁）
    lock_key = f"lock:clue:{clueId}"
    process_lock_key = f"lock:clue:process:{clueId}"
    u1 = await redis_manager.delete(lock_key)
    u2 = await redis_manager.delete(process_lock_key)
    unlocked = u1 or u2
    unlock_msg = "已清空悬挂锁" if unlocked else "本就无锁牵绊"
    
    # 步骤 2: 无视状态机的合法性限制，暴力拨回数据库状态标记
    success = await update_state_no_lock(clueId, state)
    
    if success:
        logger.info(f"✅【救援成功】线索 {clueId} 已恢复为 {state} ({unlock_msg})")
        return {
            "message": f"救援组合拳已生效。锁清理状态: {unlock_msg}。状态已被强改为: {state}。", 
            "code": 1
        }
    else:
        logger.error(f"❌【救援失败】线索 {clueId} 的状态强改失败，可能此线索在库中压根不存在")
        return {"message": f"锁清理状态: {unlock_msg}，但状态强制更新失败 (极可能查不到该线索)", "code": 0}


@router.post("/admin/batch_reset_state")
async def batch_force_reset_clue_state(request: BatchResetRequest):
    """
    【批量救火接口】批量一键破拆锁并重置状态。
    应对大面积状态流转异常，运维可传入线索列表进行批量强制阻断与重置。
    """
    logger.warning(f"🚑【火线救援】发起大批量线索联动重置，目标状态: {request.state}，共 {len(request.clueIds)} 条")
    
    results = {"success": [], "failed": []}
    for clue_id in request.clueIds:
        try:
            # 步骤 1: 拔出物理锁（状态锁与处理锁）
            lock_key = f"lock:clue:{clue_id}"
            process_lock_key = f"lock:clue:process:{clue_id}"
            await redis_manager.delete(lock_key)
            await redis_manager.delete(process_lock_key)
            
            # 步骤 2: 无视状态机的合法性限制，暴力拨回数据库状态标记
            success = await update_state_no_lock(clue_id, request.state)
            
            if success:
                results["success"].append(clue_id)
            else:
                results["failed"].append(clue_id)
        except Exception as e:
            logger.error(f"批量救援处理线索 {clue_id} 时发生异常: {e}")
            results["failed"].append(clue_id)
            
    logger.info(f"✅【批量救援结束】成功: {len(results['success'])}，失败: {len(results['failed'])}")
    
    return {
        "message": f"批量救援完成。成功: {len(results['success'])} 条, 失败: {len(results['failed'])} 条。",
        "data": results,
        "code": 1 if len(results['failed']) == 0 else 2 # 2代表部分成功
    }


@router.post("/call_history")
async def call_history(clueId:str):
    """查询线索的通话记录"""
    history = await get_call_history(clueId)
    history = [c.model_dump() for c in history]
    logger.info(f"查询线索通话记录, clueId: {clueId}, records found: {len(history)}")
    return {"message": "Call history retrieved successfully", "code": 1, "data": history}


@router.post("/clue_intake_time")
async def clue_intake_time(clueId:str):
    """查询线索的流入时间"""
    intake_time = await get_clue_intake_time(clueId)
    logger.info(f"查询线索流入时间, clueId: {clueId}, intakeTime: {intake_time}")
    return {"message": "Clue intake time retrieved successfully", "code": 1, "data": {"intakeTime": intake_time}}


@router.post("/clue_context")
async def clue_context(clueId:str):
    """查询线索的上下文信息（流入时间 + 通话记录）"""
    context = await ClueContext.from_db(clueId)
    logger.info(f"查询线索上下文信息, clueId: {clueId}, intakeTime: {context.clue_intake_time}, callRecords: {len(context.call_records)}")
    return {"message": "Clue context retrieved successfully", "code": 1, "data": context.model_dump()}


@router.post("/admin/batch_calling_to_error")
async def batch_calling_to_error():
    """
    【超时状态清理接口】将当天卡在 calling 状态超过 15 分钟的线索，批量转为 error 状态。
    """
    now = get_now()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    upper_threshold = now - timedelta(minutes=15)

    logger.warning("🚑【超时清理】开始扫描并清理 calling 状态超时的线索...")

    clue_stats = await get_clue_stats(ClueStateValue.CALLING.value)

    if not clue_stats:
        logger.info("✅【超时清理】扫描完成，未发现符合条件的超时线索。")
        return {
            "message": "未发现需要处理的线索。",
            "data": {"success": [], "failed": []},
            "code": 1
        }

    clue_ids = [clue.clueId for clue in clue_stats]
    logger.warning(f"🚑【超时清理】发现 {len(clue_ids)} 条超时线索，准备转为 error 状态。")

    results = {"success": [], "failed": []}

    for clue_id in clue_ids:
        try:
            # 步骤 1: 拔出物理锁（状态锁与处理锁）
            lock_key = f"lock:clue:{clue_id}"
            process_lock_key = f"lock:clue:process:{clue_id}"
            await redis_manager.delete(lock_key)
            await redis_manager.delete(process_lock_key)

            # 步骤 2: 无视状态机的合法性限制，暴力拨回数据库状态标记，目标状态固定为 "error"
            success = await update_state_no_lock(clue_id, ClueStateValue.ERROR.value)

            if success:
                results["success"].append(clue_id)
            else:
                results["failed"].append(clue_id)
        except Exception as e:
            logger.error(f"处理超时线索 {clue_id} 时发生异常: {e}")
            results["failed"].append(clue_id)

    logger.info(f"✅【超时清理结束】成功: {len(results['success'])}，失败: {len(results['failed'])}")

    return {
        "message": f"超时线索清理完成。成功: {len(results['success'])} 条, 失败: {len(results['failed'])} 条。",
        "data": results,
        "code": 1 if len(results['failed']) == 0 else 2  # 2代表部分成功或全部失败
    }
    
@router.post("/clue-context")
async def get_clue_context(clueId:str):
    clue_context = await ClueContext.from_db(clueId)
    return {"message": "Clue context retrieved successfully", "code": 1, "data": clue_context.model_dump()}


@router.post('/clue-events')
@router.get('/clue-events')
async def get_clue_events_endpoint(
    payload: Optional[Dict[str, Any]] = Body(None),
    clueId: Optional[str] = Query(None, description="线索ID"),
    clue_id: Optional[str] = Query(None, description="线索ID别名"),
):
    """查询线索事件溯源记录 (兼容 POST 与 GET)"""
    target_id = None
    if payload and isinstance(payload, dict):
        target_id = payload.get("clueId") or payload.get("clue_id")
    if not target_id:
        target_id = clueId or clue_id
    if not target_id:
        raise HTTPException(status_code=422, detail="clueId is required")

    events = await get_clue_events(target_id)
    events_data = [e.model_dump() for e in events]
    logger.info(f"查询线索事件溯源记录, clueId: {target_id}, events found: {len(events_data)}")
    return {"message": "Clue events retrieved successfully", "code": 1, "data": events_data}


class QueryContextRequest(BaseModel):
    clueId: Optional[str] = Field(None, description="线索ID", json_schema_extra={"example": "clue_123456"})
    clue_id: Optional[str] = Field(None, description="线索ID别名", json_schema_extra={"example": "clue_123456"})


@router.post("/query/context")
@router.get("/query/context")
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
    logger.info(f"查询线索上下文信息, clueId: {target_clue_id}")
    return context.model_dump()

@router.post("/admin/batch_init_clues")
async def batch_init_clues(payload: BatchInitRequest):
    """
    【预热初始化接口】批量导入线索数据。
    自动过滤多余字段，并在 MySQL 和 Mongo 中建立模拟的未接通与排期状态。
    """
    logger.info(f"🚀 开始批量初始化线索，共收到 {len(payload.data)} 条数据...")
    
    clues_to_insert = []
    calls_to_insert = []
    
    success_count = 0

    for item in payload.data:
        try:
            # 1. 利用现有的 Request 模型进行数据清洗，自动丢弃无用字段
            clean_data = ClueCreateRequest.model_validate(item.detail)

            # 2. 转换为内部统一的 NormalizedClue，与线上写入路径保持一致
            normalized = clean_data.to_normalized()

            # 准备 MySQL ClueInfo 数据
            clues_to_insert.append(
                ClueInfo(
                    clue_id=normalized.clueId,
                    workflow_instance_id=f"wi_init_{normalized.clueId}",
                    workflow_id=normalized.workflowId,
                    agent_id=normalized.agentId,
                    strategy_id=normalized.strategyId,
                    phone_number=normalized.phoneNumber,
                    customer_name=normalized.customerName,
                    agent_data=normalized.agentData or {},
                    status="scheduled",
                    total_call_count=1,
                )
            )
            
            # 准备 MySQL CallRecord 数据 (模拟 500/509 未接通)
            calls_to_insert.append(
                CallRecord(
                    call_id=f"call_init_{normalized.clueId}",
                    clue_id=normalized.clueId,
                    agent_id=normalized.agentId,
                    strategy_id=normalized.strategyId,
                    attempt_no=1,
                    status="ended",
                    answered=0,
                    decision_result={"oneLevelCode": 500, "twoLevelCode": 509},
                )
            )
            success_count += 1
            
        except Exception as e:
            logger.error(f"解析线索 {item.clueId} 失败，已跳过: {e}")
            continue

    # 执行大批量落库
    try:
        if clues_to_insert:
            await ClueInfo.bulk_create(clues_to_insert, ignore_conflicts=True)
            
        if calls_to_insert:
            await CallRecord.bulk_create(calls_to_insert, ignore_conflicts=True)
            
        logger.info(f"✅ 批量初始化完成！成功落库 {success_count} 条线索上下文。")
        return {
            "code": 1, 
            "message": f"初始化成功，共处理 {success_count} 条线索", 
            "data": {"processed": success_count}
        }
        
    except Exception as e:
        logger.error(f"❌ 批量落库阶段发生严重错误: {e}", exc_info=True)
        return {"code": 0, "message": f"数据库批量写入失败: {str(e)}"}

@router.post("/admin/batch_cancel")
async def batch_cancel_clues(request: BatchCancelRequest, background_tasks: BackgroundTasks):
    """
    【后台异步接口】批量发起取消线索请求。
    接收到任务后立即返回，真实取消动作在后台排队执行，防止批量操作导致接口超时。
    """
    async def _process_batch_cancel(clue_ids: List[str]):
        """
        实际执行批量取消的后台协程函数。
        遍历调用系统中标准的单条 cancel 逻辑。
        """
        logger.warning(f"🚑【后台任务】开始执行批量取消线索，共计 {len(clue_ids)} 条。")
        
        results = {"success": [], "failed": []}
        
        for clue_id in clue_ids:
            try:
                # 步骤 1: 物理破拆该线索可能残留的分布式锁（避免残留死锁阻碍 on_cancel 或状态流转）
                lock_key = f"lock:clue:{clue_id}"
                process_lock_key = f"lock:clue:process:{clue_id}"
                await redis_manager.delete(lock_key)
                await redis_manager.delete(process_lock_key)

                # 步骤 2: 强制将状态先转为 pending，绕过 calling/wait_for_calling 不允许 cancel 的限制
                success = await update_state_no_lock(clue_id, ClueStateValue.PENDING.value)
                if not success:
                    raise Exception(f"强制前置状态转换为 pending 失败")
                # 步骤 3: 调用系统中标准的单条撤单处理流程（包含状态校验、同步上游等）
                handler = await ClueHandlerFactory.get_handler_by_clue(clue_id)
                result = await handler.on_cancel(clue_id)
                
                if result.success:
                    results["success"].append(clue_id)
                    logger.info(f"【后台批量取消】线索 {clue_id} 取消成功。")
                else:
                    results["failed"].append(clue_id)
                    logger.warning(f"【后台批量取消】线索 {clue_id} 取消失败（可能状态不满足）：{result.reason}")
                    
            except Exception as e:
                logger.error(f"【后台批量取消】处理线索 {clue_id} 发生异常: {e}", exc_info=True)
                results["failed"].append(clue_id)
            
            await asyncio.sleep(0.1)
        logger.info(f"✅【后台批量取消结束】成功: {len(results['success'])} 条，失败: {len(results['failed'])} 条。")
    
    if not request.clueIds:
        return {"message": "传入的 clueIds 列表不能为空", "code": 0}
        
    # 🌟 核心：将后台处理函数和参数交由 FastAPI 调度
    background_tasks.add_task(_process_batch_cancel, request.clueIds)
    
    return {
        "message": f"已成功接收 {len(request.clueIds)} 条线索的取消请求，系统正在后台安全处理中...",
        "code": 1
    }

# @router.get("/admin/query/mongo/clue", summary="[排障] 查询 Mongo 原始线索")
# async def admin_query_mongo_clue(clueId: str):
#     try:
#         data = await get_raw_clue_from_mongo(clueId)
#         if not data:
#             return {"code": 0, "message": f"MongoDB 未找到线索: {clueId}", "data": None}
#         return {"code": 1, "message": "success", "data": data}
#     except Exception as e:
#         logger.error(f"查询 Mongo Clue 异常: {e}")
#         return {"code": -1, "message": f"查询异常: {str(e)}"}
    
# @router.get("/admin/query/mongo/end_call_info", summary="[排障] 查询大模型外呼详情")
# async def admin_query_mongo_end_call_info(clueId: str):
#     try:
#         data = await list_end_call_infos_from_mongo(clueId)
#         return {"code": 1, "message": "success", "total": len(data), "data": data}
#     except Exception as e:
#         logger.error(f"查询 Mongo EndCallInfo 异常: {e}")
#         return {"code": -1, "message": f"查询异常: {str(e)}"}

# @router.get("/admin/query/mysql/clue_state", summary="[排障] 查询 MySQL 状态机")
# async def admin_query_mysql_clue_state(clueId: str):
#     try:
#         data = await get_clue_state_record(clueId)
#         if not data:
#             return {"code": 0, "message": f"MySQL 未找到线索状态: {clueId}", "data": None}
#         return {"code": 1, "message": "success", "data": data}
#     except Exception as e:
#         logger.error(f"查询 MySQL ClueState 异常: {e}")
#         return {"code": -1, "message": f"查询异常: {str(e)}"}

# @router.get("/admin/query/mysql/clue_event", summary="[排障] 查询 MySQL 事件溯源时间轴")
# async def admin_query_mysql_clue_event(clueId: str):
#     try:
#         data = await list_clue_events_timeline(clueId)
#         return {"code": 1, "message": "success", "total": len(data), "data": data}
#     except Exception as e:
#         logger.error(f"查询 MySQL ClueEvent 异常: {e}")
#         return {"code": -1, "message": f"查询异常: {str(e)}"}
    
@router.get("/admin/tools/sample_clues", summary="[排障工具] 按小时抽样线索ID")
async def admin_sample_clues_by_hour(
    hour: int = Query(..., ge=0, le=23, description="抽样的小时 (如 9 代表 09:00 - 09:59)"),
    date_str: Optional[str] = Query(None, description="日期 (格式 YYYY-MM-DD)。不传则默认为今天"),
    size: int = Query(20, ge=1, le=1000, description="抽样条数")
):
    """
    根据给定的时间段（精确到小时）随机抽样 Mongo 库中的线索。
    - 若该小时内数据超过 size(20) 条，随机返回 20 条 clueId。
    - 若不足 20 条，返回全部。
    - 若完全没有数据，返回空列表。
    """
    async def sample_clue_ids_by_hour(hour: int, date_str: str = None, sample_size: int = 20) -> list[str]:
        if date_str is None:
            date_str = get_now().strftime("%Y-%m-%d")
            
        hour_str = f"{hour:02d}"
        start_time = f"{date_str} {hour_str}:00:00"
        end_time = f"{date_str} {hour_str}:59:59"

        clues = await ClueInfo.filter(
            received_at__gte=start_time,
            received_at__lte=end_time
        ).limit(sample_size).values_list("clue_id", flat=True)

        return list(clues)
    
    try:
        clue_ids = await sample_clue_ids_by_hour(hour=hour, date_str=date_str, sample_size=size)
        
        return {
            "code": 1,
            "message": "success",
            "total": len(clue_ids),
            "data": clue_ids
        }
        
    except Exception as e:
        logger.error(f"按小时抽样线索异常: {e}", exc_info=True)
        # 异常兜底，按照你的需求返回空列表
        return {"code": -1, "message": f"抽样查询异常: {str(e)}", "data": []}
    
    
@router.post("/close_reason")
async def get_close_reason(clueId:str):
    from app.dbmodels import crud
    return await crud.get_close_reason(clueId)