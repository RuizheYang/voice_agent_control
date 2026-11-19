"""商业化线索接入：可靠受理后，交给业务线专属 Handler (ClueHandlerFactory) 排期入池。"""
import json
import logging
import time
import hashlib
from functools import wraps
from time import perf_counter
from typing import Optional, List

from fastapi import APIRouter, File, Form, Query, Request, UploadFile, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import ValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.formparsers import MultiPartException

from app.auth.signature import verify_inflow_signature, check_and_lock_request_id
from app.schemas.clue_create import (
    ClueCreateRequest,
    ClueCreateResponse,
    BatchClueCreateRequest,
)
from app.dbmodels.crud import (
    get_workflow_config,
    get_voice_agent_config_by_id,
    get_voice_agent_config,
    get_strategy_config,
)
from app.clue_inflow import InflowError
from app.clue_inflow.service import InflowService
from app.schemas.clue_inflow import BatchInflowRequest, FileInflowRequest, SingleInflowRequest

logger = logging.getLogger(__name__)
MAX_JSON_BYTES = 5 * 1024 * 1024
MAX_MULTIPART_BYTES = 201 * 1024 * 1024


def log_request(request, level, stage, route_path, started_at, code=None, error_type=None):
    """只记录有界标识；不记录请求体、签名、URL 查询参数或异常原文。"""
    logger.log(
        level,
        "[Clue-Inflow] 【%s】接口请求: method=%s, path=%r, app_id=%r, request_id=%r, workflow_id=%r, task_id=%r, code=%s, error_type=%s, elapsed_ms=%.1f",
        stage,
        request.method,
        route_path,
        request.headers.get("X-App-Id", "")[:128],
        getattr(getattr(request, "state", None), "inflow_request_id", None),
        getattr(getattr(request, "state", None), "inflow_workflow_id", None),
        request.path_params.get("task_id", "")[:128],
        code,
        error_type,
        (perf_counter() - started_at) * 1000,
    )


class InflowRoute(APIRoute):
    """统一业务错误格式与请求耗时审计日志。"""
    def get_route_handler(self):
        original = super().get_route_handler()

        @wraps(original)
        async def handle(request):
            started_at = perf_counter()
            log_level = logging.DEBUG if request.method == "GET" else logging.INFO
            log_request(request, log_level, "开始", self.path, started_at)
            try:
                if request.method == "POST" and request.url.path.endswith("/clue/create/file"):
                    length = request.headers.get("content-length")
                    if length and int(length) > MAX_MULTIPART_BYTES:
                        raise InflowError(40001, "文件请求超过大小上限", 413)
                    receive = request._receive
                    received = 0

                    async def bounded_receive():
                        nonlocal received
                        message = await receive()
                        received += len(message.get("body", b""))
                        if received > MAX_MULTIPART_BYTES:
                            if hasattr(request, "state"):
                                request.state.inflow_too_large = True
                            raise MultiPartException("文件请求超过大小上限")
                        return message

                    request._receive = bounded_receive

                result = await original(request)
                log_request(request, log_level, "完成", self.path, started_at, code=0)
                return result
            except InflowError as error:
                log_request(
                    request,
                    logging.INFO if error.code == 40005 else logging.WARNING,
                    "幂等拦截" if error.code == 40005 else "拒绝",
                    self.path,
                    started_at,
                    code=error.code,
                )
                return JSONResponse(error.response(), status_code=error.status)
            except (ValidationError, RequestValidationError, UnicodeError, ValueError) as error:
                log_request(
                    request,
                    logging.WARNING,
                    "参数校验失败",
                    self.path,
                    started_at,
                    code=40001,
                    error_type=type(error).__name__,
                )
                return JSONResponse({"code": 40001, "msg": "参数、JSON格式或编码不合法", "data": None}, status_code=400)
            except StarletteHTTPException as error:
                if getattr(getattr(request, "state", None), "inflow_too_large", False):
                    log_request(request, logging.WARNING, "请求过大", self.path, started_at, code=40001)
                    return JSONResponse({"code": 40001, "msg": "文件请求超过大小上限", "data": None}, status_code=413)
                detail = error.detail if isinstance(error.detail, dict) else {"code": 40001, "msg": str(error.detail)}
                log_request(
                    request,
                    logging.ERROR if error.status_code >= 500 else logging.WARNING,
                    "拒绝",
                    self.path,
                    started_at,
                    code=detail.get("code"),
                )
                return JSONResponse({**detail, "data": detail.get("data")}, status_code=error.status_code)
            except Exception as error:
                log_request(
                    request,
                    logging.ERROR,
                    "失败",
                    self.path,
                    started_at,
                    code=50000,
                    error_type=type(error).__name__,
                )
                return JSONResponse({"code": 50000, "msg": "线索受理服务暂不可用，请稍后重试", "data": None}, status_code=503)

        return handle


router = APIRouter(
    prefix="/clue",
    tags=["clue_inflow"],
    route_class=InflowRoute,
)


def get_inflow_service():
    return InflowService()


async def read_json(request: Request):
    """兼容直接读取 body 与流式 stream 的 JSON 解析器，具备 5MiB 安全防爆保护。"""
    content_type = request.headers.get("content-type", "").split(";")[0].lower()
    if content_type and content_type != "application/json":
        raise InflowError(40001, "该入口要求application/json")

    try:
        body_bytes = await request.body()
    except Exception:
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
        body_bytes = bytes(body)

    if len(body_bytes) > MAX_JSON_BYTES:
        raise InflowError(40001, "请求JSON超过5MiB上限", 413)

    text = body_bytes.decode("utf-8", errors="replace")

    def invalid_constant(value):
        raise ValueError("JSON不接受非有限数值")

    return text, json.loads(text, parse_constant=invalid_constant)


async def authorize(request: Request, signed_body: str, workflow_id: Optional[str] = None):
    if hasattr(request, "state"):
        request.state.inflow_workflow_id = workflow_id
    app_id = await verify_inflow_signature(request, raw_body=signed_body, workflow_id=workflow_id)
    allowed = getattr(getattr(request, "state", None), "allowed_workflows", ["*"]) or ["*"]
    return app_id, allowed


@router.post("/create", summary="单条线索流入")
async def create_single_clue(request: Request):
    """
    单条线索流入接口：
    1. 根据 workflow_id 查询 WorkflowConfig 表，未查询到或无配套则返回 code: 40004 ("未查询到对应业务线配置")。
    2. 校验 HMAC-SHA256 签名与 X-App-Id 权限。
    3. 校验 request_id 幂等防重。
    4. 规范化并推入系统消费队列 (绑定 workflow_id 作用域专属 ClueHandler)。
    """
    raw_body_str, body_json = await read_json(request)

    try:
        clue_req = ClueCreateRequest.model_validate(body_json)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail={"code": 40001, "msg": f"参数校验不合法: {e}"}
        )

    if hasattr(request, "state"):
        request.state.inflow_request_id = clue_req.request_id

    # 0.5 若 workflow_id == "auto"，先由 ClueSupervisorAgent 进行流入自动分诊路由
    if (clue_req.workflow_id or "").strip().lower() == "auto":
        try:
            from app.agents.supervisor import clue_supervisor_agent
            routed_wf = await clue_supervisor_agent.triage_inflow_workflow(
                agent_data=clue_req.agent_data if isinstance(clue_req.agent_data, dict) else {},
                fallback_workflow_id="default",
            )
            if routed_wf and routed_wf != "auto":
                clue_req.workflow_id = routed_wf
                logger.info(f"[Clue-Inflow] 【自动分诊】workflow_id='auto' -> '{routed_wf}'")
        except Exception as triage_err:
            logger.warning(f"[Clue-Inflow] 自动分诊降级保留 default: {triage_err}")
            clue_req.workflow_id = "default"

    # 1. 查询 Workflow 配置表 (核心前置检验)
    workflow_config = await get_workflow_config(clue_req.workflow_id)
    if not workflow_config or not workflow_config.is_active:
        logger.warning(f"线索流入拦截: 未查询到 workflow_id '{clue_req.workflow_id}' 对应的业务线配置")
        return {
            "code": 40004,
            "msg": f"未查询到 workflow_id '{clue_req.workflow_id}' 对应的业务线配置",
            "data": None
        }

    # 校验绑定的 Agent 与 Strategy 是否在底表配套存在
    try:
        saved_agent = await get_voice_agent_config_by_id(workflow_config.agent_id)
        if not saved_agent:
            saved_agent = await get_voice_agent_config(workflow_config.agent_id)
        saved_strategy = await get_strategy_config(workflow_config.strategy_id)
        if not saved_agent or not saved_strategy:
            logger.warning(f"线索流入拦截: workflow_id '{clue_req.workflow_id}' 业务线没有对应的agent可以配套")
            return {
                "code": 40004,
                "msg": "此workflow_id业务线没有对应的agent可以配套",
                "data": None
            }
    except Exception as e:
        logger.debug(f"线索流入配套校验跳过/异常: {e}")

    if hasattr(request, "state"):
        request.state.inflow_workflow_id = clue_req.workflow_id

    # 2. 签名与权限验证
    app_id = await verify_inflow_signature(
        request=request,
        raw_body=raw_body_str,
        workflow_id=clue_req.workflow_id
    )
    allowed_workflows = getattr(getattr(request, "state", None), "allowed_workflows", ["*"]) or ["*"]

    # 3. 幂等性校验
    await check_and_lock_request_id(clue_req.request_id)

    # 4. 统一收敛到 InflowService: 单事务落库 ExecutionTask(import) + ClueInfo + ExecutionTask(new_clue)
    return await get_inflow_service().accept_clue_create(
        app_id=app_id,
        clue_req=clue_req,
        allowed_workflows=allowed_workflows,
    )


@router.post("/create/batch", summary="批量线索流入")
async def create_batch_clues(request: Request):
    """
    批量线索流入接口：
    1. 根据 workflow_id 校验业务线配置，未找到返回 code: 40004。
    2. 校验签名与 request_id 幂等。
    3. 通过批次的 workflow_id 获取外呼和跟进策略（批次统一，不支持单条参数覆写）。
    """
    raw_body_str, body_json = await read_json(request)

    try:
        batch_req = BatchClueCreateRequest.model_validate(body_json)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail={"code": 40001, "msg": f"参数校验不合法: {e}"}
        )

    if hasattr(request, "state"):
        request.state.inflow_request_id = batch_req.request_id

    # 1. 查询 Workflow 配置
    workflow_config = await get_workflow_config(batch_req.workflow_id)
    if not workflow_config or not workflow_config.is_active:
        logger.warning(f"批量线索流入拦截: 未查询到 workflow_id '{batch_req.workflow_id}' 对应的业务线配置")
        return {
            "code": 40004,
            "msg": f"未查询到 workflow_id '{batch_req.workflow_id}' 对应的业务线配置",
            "data": None
        }

    # 校验绑定的 Agent 与 Strategy 是否在底表配套存在
    try:
        saved_agent = await get_voice_agent_config_by_id(workflow_config.agent_id)
        if not saved_agent:
            saved_agent = await get_voice_agent_config(workflow_config.agent_id)
        saved_strategy = await get_strategy_config(workflow_config.strategy_id)
        if not saved_agent or not saved_strategy:
            logger.warning(f"批量线索流入拦截: workflow_id '{batch_req.workflow_id}' 业务线没有对应的agent可以配套")
            return {
                "code": 40004,
                "msg": "此workflow_id业务线没有对应的agent可以配套",
                "data": None
            }
    except Exception as e:
        logger.debug(f"批量线索流入配套校验跳过/异常: {e}")

    if hasattr(request, "state"):
        request.state.inflow_workflow_id = batch_req.workflow_id

    # 2. 签名与权限验证
    app_id = await verify_inflow_signature(
        request=request,
        raw_body=raw_body_str,
        workflow_id=batch_req.workflow_id
    )
    allowed_workflows = getattr(getattr(request, "state", None), "allowed_workflows", ["*"]) or ["*"]

    # 3. 幂等性校验
    await check_and_lock_request_id(batch_req.request_id)

    # 4. 统一收敛到 InflowService: 单事务落库 ExecutionTask(import) + ClueInfo + ExecutionTask(new_clue)
    return await get_inflow_service().accept_batch_create(
        app_id=app_id,
        batch_req=batch_req,
        allowed_workflows=allowed_workflows,
    )


@router.post("/create/file", summary="文件线索异步流入", status_code=202)
async def create_file_clues(
    request: Request,
    request_id: str = Form(...),
    workflow_id: str = Form(...),
    file_type: str = Form(...),
    file: Optional[UploadFile] = File(None),
    file_url: Optional[str] = Form(None),
    file_sha256: Optional[str] = Form(None),
):
    """
    文件线索异步流入接口：
    1. 根据 workflow_id 校验业务线配置，未找到返回 code: 40004。
    2. 支持单文件上传或远程拉取，异步创建任务并支持后台持久化推入。
    """
    workflow_config = await get_workflow_config(workflow_id)
    if not workflow_config or not workflow_config.is_active:
        logger.warning(f"文件线索流入拦截: 未查询到 workflow_id '{workflow_id}' 对应的业务线配置")
        return {
            "code": 40004,
            "msg": f"未查询到 workflow_id '{workflow_id}' 对应的业务线配置",
            "data": None
        }

    service = get_inflow_service()
    upload_key = None
    registered = False
    try:
        payload = FileInflowRequest(
            request_id=request_id,
            workflow_id=workflow_id,
            file_type=file_type,
            file_url=file_url,
            file_sha256=file_sha256,
        )
        if hasattr(request, "state"):
            request.state.inflow_request_id = payload.request_id
            request.state.inflow_workflow_id = payload.workflow_id
        if bool(file) == bool(file_url):
            raise InflowError(40001, "file与file_url必须二选一")
        version = request.headers.get("X-Signature-Version", "1")
        if version not in ("1", "2"):
            raise InflowError(40001, "不支持该文件签名版本")
        if file_url and (version != "2" or not file_sha256):
            raise InflowError(40001, "file_url要求签名v2和file_sha256，受理后异步校验文件")
        if version == "2":
            if not file_sha256:
                raise InflowError(40001, "签名v2需要file_sha256")
            signed = json.dumps(
                ["file", payload.request_id, payload.workflow_id, payload.file_type,
                 payload.file_sha256.lower(), payload.file_url or ""],
                ensure_ascii=False,
                separators=(",", ":"),
            )
            app_id, allowed = await authorize(request, signed, payload.workflow_id)
        if file:
            upload_key, actual_hash = await service.storage.save_upload(file)
            if file_sha256 and actual_hash != file_sha256.lower():
                raise InflowError(40002, "文件SHA256校验失败", 401)
            payload.file_sha256 = actual_hash
            if version == "1":
                app_id, allowed = await authorize(request, actual_hash, payload.workflow_id)
        result = await service.accept_file(app_id, payload, allowed, upload_key)
        registered = True
        return JSONResponse(result, status_code=202)
    except InflowError as error:
        if error.code == 40004 and error.data and error.data.get("import_task_id"):
            registered = True
        raise
    finally:
        if file:
            await file.close()
        if upload_key and not registered:
            service.storage.remove(upload_key)
            logger.debug(
                "[Clue-Inflow] 【清理】未受理的上传文件已删除: object_key=%s, request_id=%r",
                upload_key,
                getattr(getattr(request, "state", None), "inflow_request_id", None),
            )


@router.get("/import-tasks/{task_id}", summary="查询导入任务结果")
async def get_import_task(task_id: str, request: Request):
    app_id, allowed = await authorize(request, "")
    return await get_inflow_service().get_task(app_id, task_id, allowed)


@router.get("/import-tasks/{task_id}/errors", summary="查询导入逐行错误")
async def get_import_errors(
    task_id: str,
    request: Request,
    cursor: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=200),
):
    app_id, allowed = await authorize(request, "")
    return await get_inflow_service().get_errors(app_id, task_id, allowed, cursor, limit)
