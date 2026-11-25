"""Pluggable Multimodal Vehicle License Precheck SubFlow & Auto-Finance Domain Loader.

Demonstrates zero-kernel-change plugin extensibility:
- Connects Qwen3-VL-32B / OCR (via abstract BaseVisionOcrClient / Qwen3VlOcrClient)
- Validates 17-character ISO 3779 VIN codes (rejecting I/O/Q and invalid lengths)
- Calculates vehicle age from registration date against admission limits
- Issues deterministic precheck codes (`AFPC-...`)
- Locks step progress until valid VIN & registration date are verified
- Plugs cleanly into `SubFlowRegistry` alongside YAML subflows via `load_auto_finance_plugin`.
"""

from __future__ import annotations

import hashlib
import json
import re
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import httpx
import yaml
from pydantic import BaseModel, Field

from app.config import config
from app.rsi.models import FollowupPolicy
from app.rsi.subflows.base import BaseSubFlowHandler, StepResult, SubFlowRunContext
from app.rsi.subflows.registry import SubFlowRegistry, subflow_registry
from app.rsi.subflows.runtime import GlobalQATopicSpec, sanitize_llm_output
from app.rsi.subflows.yaml_flow import (
    McpToolServerSpec,
    RsiCredentialProxy,
    RsiHttpToolSpec,
    YamlSubFlowHandler,
    YamlSubFlowSpec,
)

DEFAULT_AUTO_FINANCE_YAML_PATH = (
    Path(__file__).resolve().parents[2] / "domains" / "auto_finance_rsi.yaml"
)

_VIN_ISO3779_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")
_VIN_FORBIDDEN_CHARS_RE = re.compile(r"[IOQ]")
_DATE_RE = re.compile(
    r"(?P<year>19\d{2}|20\d{2})[-/.年](?P<month>1[0-2]|0?[1-9])(?:[-/.月](?P<day>3[01]|[12]\d|0?[1-9])日?)?"
)
_TEXT_VIN_CANDIDATE_RE = re.compile(r"(?<![A-Za-z0-9])([A-Za-z0-9]{6,20})(?![A-Za-z0-9])")
_PLATE_RE = re.compile(r"[\u4e00-\u9fa5][A-Z][A-Z0-9]{4,6}")
_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)


def normalize_and_validate_vin(raw_vin: Optional[str]) -> Tuple[bool, str, str]:
    """Normalize and validate 17-character ISO 3779 VIN code."""
    cleaned = re.sub(r"[\s\-]+", "", str(raw_vin or "")).upper()
    if not cleaned:
        return False, "", "missing_vin"
    if len(cleaned) != 17:
        return False, cleaned, f"invalid_vin_length:{len(cleaned)}"
    if _VIN_FORBIDDEN_CHARS_RE.search(cleaned):
        return False, cleaned, "invalid_vin_forbidden_chars"
    if not _VIN_ISO3779_RE.match(cleaned):
        return False, cleaned, "invalid_vin_charset"
    return True, cleaned, "ok"


def parse_registration_date(raw_date: Optional[str]) -> date:
    """Parse registration date from ISO, slash, dot, or Chinese date string."""
    text = str(raw_date or "").strip()
    match = _DATE_RE.search(text)
    if not match:
        raise ValueError(f"Unparseable registration date: {raw_date!r}")
    year = int(match.group("year"))
    month = int(match.group("month"))
    day = int(match.group("day") or 1)
    return date(year, month, day)


def calculate_car_age_years(
    registration_date: str,
    *,
    reference_date: Optional[date] = None,
) -> float:
    """Compute exact car age in years rounded to 2 decimal places."""
    reg = parse_registration_date(registration_date)
    ref = reference_date or datetime.now(timezone.utc).date()
    days = max(0, (ref - reg).days)
    return round(days / 365.25, 2)


def generate_precheck_code(
    vin: str,
    entity_id: str,
    *,
    prefix: str = "AFPC",
) -> str:
    """Issue a deterministic verifiable precheck code for an admitted vehicle."""
    digest = hashlib.sha256(f"{entity_id}:{vin}".encode("utf-8")).hexdigest()[:6].upper()
    return f"{prefix}-{vin[-6:]}-{digest}"


class VehicleLicenseOcrResult(BaseModel):
    vin: str = ""
    plate_no: str = ""
    owner_name: str = ""
    brand_model: str = ""
    registration_date: str = ""
    use_character: str = ""
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    provider: str = "qwen3_vl_ocr"
    raw_text: str = ""


class BaseVisionOcrClient(ABC):
    """Abstract multimodal / OCR client interface for vehicle license extraction."""

    @abstractmethod
    async def extract_vehicle_license(
        self,
        image_url: str,
        *,
        context: Optional[Dict[str, Any]] = None,
    ) -> VehicleLicenseOcrResult:
        """Inspect vehicle license image and return structured fields."""


class Qwen3VlOcrClient(BaseVisionOcrClient):
    """OpenAI-compatible multimodal Qwen3-VL-32B + OCR extractor."""

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        model_name: str = "Qwen3-VL-32B",
        timeout_seconds: float = 15.0,
        http_transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        llm_cfg = config.llm_factory_config
        self.base_url = (base_url or llm_cfg.base_url or "").rstrip("/")
        self.api_key = api_key if api_key is not None else (llm_cfg.api_key or "")
        self.model_name = model_name
        self.timeout_seconds = timeout_seconds
        self.http_transport = http_transport

    async def extract_vehicle_license(
        self,
        image_url: str,
        *,
        context: Optional[Dict[str, Any]] = None,
    ) -> VehicleLicenseOcrResult:
        endpoint = f"{self.base_url}/chat/completions"
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload = {
            "model": self.model_name,
            "temperature": 0.0,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "你是高精度车辆行驶证识别引擎。请只输出 JSON 对象，"
                        "包含字段：vin, plate_no, owner_name, brand_model, "
                        "registration_date(YYYY-MM-DD), use_character, confidence。"
                    ),
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "请识别此行驶证主页的关键字段并输出JSON"},
                        {"type": "image_url", "image_url": {"url": image_url}},
                    ],
                },
            ],
        }

        client_kwargs: Dict[str, Any] = {"timeout": self.timeout_seconds}
        if self.http_transport is not None:
            client_kwargs["transport"] = self.http_transport

        async with httpx.AsyncClient(**client_kwargs) as client:
            resp = await client.post(endpoint, json=payload, headers=headers)
            resp.raise_for_status()
            body = resp.json()

        raw_content = str(
            body.get("choices", [{}])[0].get("message", {}).get("content") or ""
        )
        cleaned = sanitize_llm_output(raw_content)
        fence_match = _CODE_FENCE_RE.search(cleaned)
        if fence_match:
            cleaned = fence_match.group(1).strip()

        parsed: Dict[str, Any] = json.loads(cleaned) if cleaned else {}
        return VehicleLicenseOcrResult(
            vin=str(parsed.get("vin") or ""),
            plate_no=str(parsed.get("plate_no") or ""),
            owner_name=str(parsed.get("owner_name") or ""),
            brand_model=str(parsed.get("brand_model") or ""),
            registration_date=str(parsed.get("registration_date") or ""),
            use_character=str(parsed.get("use_character") or ""),
            confidence=float(parsed.get("confidence", 0.95)),
            provider=f"{self.model_name}+ocr",
            raw_text=cleaned,
        )


def _extract_text_slots(text: str) -> Dict[str, str]:
    extracted: Dict[str, str] = {}
    raw = (text or "").strip()
    if not raw:
        return extracted

    for match in _TEXT_VIN_CANDIDATE_RE.finditer(raw):
        token = match.group(1).upper()
        if any(ch.isalpha() for ch in token) and any(ch.isdigit() for ch in token):
            # Prefer a 17-char candidate if present; otherwise preserve suspicious VIN token
            if "vin" not in extracted or len(token) == 17:
                extracted["vin"] = token
                if len(token) == 17:
                    break

    date_match = _DATE_RE.search(raw)
    if date_match:
        y = int(date_match.group("year"))
        m = int(date_match.group("month"))
        d = int(date_match.group("day") or 1)
        extracted["registration_date"] = f"{y:04d}-{m:02d}-{d:02d}"

    plate_match = _PLATE_RE.search(raw.upper())
    if plate_match:
        extracted["plate_no"] = plate_match.group(0)

    return extracted


class VehicleLicensePrecheckHandler(BaseSubFlowHandler):
    """Pluggable Python multimodal handler for vehicle license verification."""

    subflow_id = "auto_vehicle_license_precheck"
    description = "行驶证多模态预审（VIN 17位校验 + 车龄核算 + 预审码下发）"
    initial_node_id = "collect_license_image"

    def __init__(
        self,
        *,
        vision_client: Optional[BaseVisionOcrClient] = None,
        max_car_age_years: float = 10.0,
        fixed_opening: str = (
            "请拍一张清晰的《机动车行驶证》主页照片发给我，"
            "我这边帮您自动识别车架号 VIN 和注册日期并生成预审码。"
        ),
        resume_hint: str = "请发一下清晰的行驶证照片，或提供17位车架号VIN与首次注册日期～",
        qa_topics: Optional[Sequence[GlobalQATopicSpec]] = None,
        reference_date_fn: Optional[Callable[[], date]] = None,
        precheck_code_prefix: str = "AFPC",
    ) -> None:
        self.vision_client = vision_client
        self.max_car_age_years = float(max_car_age_years)
        self.fixed_opening = fixed_opening
        self.resume_hint = resume_hint
        self.qa_topics = list(qa_topics or [])
        self.reference_date_fn = reference_date_fn or (
            lambda: datetime.now(timezone.utc).date()
        )
        self.precheck_code_prefix = precheck_code_prefix

    def get_node_resume_hint(self, node_id: str) -> str:
        return self.resume_hint

    async def step(self, ctx: SubFlowRunContext) -> StepResult:
        if (
            ctx.first_visit_to_node
            and self.fixed_opening
            and not ctx.incoming_text.strip()
            and not ctx.media_urls
        ):
            return StepResult(
                status="in_progress",
                reply_text=sanitize_llm_output(self.fixed_opening),
                next_node_id=self.initial_node_id,
                used_fixed_opening=True,
                used_llm=False,
            )

        updated_slots: Dict[str, Any] = {}
        merged_slots = dict(ctx.collected_slots)
        used_multimodal = False

        if ctx.media_urls:
            client = self.vision_client or Qwen3VlOcrClient()
            ocr = await client.extract_vehicle_license(
                ctx.media_urls[0],
                context={
                    "tenant_id": ctx.tenant_id,
                    "entity_id": ctx.entity_id,
                    "collected_slots": merged_slots,
                },
            )
            used_multimodal = True
            updated_slots["image_url"] = ctx.media_urls[0]
            updated_slots["ocr_provider"] = ocr.provider
            updated_slots["ocr_confidence"] = ocr.confidence
            for k in (
                "vin",
                "plate_no",
                "owner_name",
                "brand_model",
                "registration_date",
                "use_character",
            ):
                val = getattr(ocr, k, "")
                if val:
                    updated_slots[k] = val
            merged_slots.update(updated_slots)

        if ctx.incoming_text.strip():
            text_slots = _extract_text_slots(ctx.incoming_text)
            for k, val in text_slots.items():
                if not merged_slots.get(k) or (
                    k == "vin" and not normalize_and_validate_vin(merged_slots.get("vin"))[0]
                ):
                    updated_slots[k] = val
                    merged_slots[k] = val

        raw_vin = str(merged_slots.get("vin") or "")
        vin_ok, clean_vin, vin_reason = normalize_and_validate_vin(raw_vin)
        if not vin_ok:
            if vin_reason == "missing_vin":
                reply = "未识别到有效的车架号（VIN），请重新发送清晰的行驶证主页照片，或直接输入17位VIN码。"
            else:
                # Clear bad cached VIN so user correction replaces cleanly
                updated_slots.pop("vin", None)
                reply = (
                    f"识别或输入的车架号 `{clean_vin}` 格式有误（{vin_reason}，"
                    "标准VIN需为17位且不包含字母I/O/Q），请重新核对后发送。"
                )
            return StepResult(
                status="in_progress",
                reply_text=reply,
                next_node_id=self.initial_node_id,
                updated_slots=updated_slots,
                used_llm=used_multimodal,
            )

        updated_slots["vin"] = clean_vin
        merged_slots["vin"] = clean_vin

        raw_reg = str(merged_slots.get("registration_date") or "").strip()
        try:
            reg_parsed = parse_registration_date(raw_reg)
        except ValueError:
            return StepResult(
                status="in_progress",
                reply_text=(
                    f"已核验车架号 {clean_vin}，但暂未识别到首次注册日期，"
                    "请补充行驶证上的注册日期（例如 2022-05-18）以便核算车龄。"
                ),
                next_node_id=self.initial_node_id,
                updated_slots=updated_slots,
                used_llm=used_multimodal,
            )

        normalized_reg_date = reg_parsed.isoformat()
        updated_slots["registration_date"] = normalized_reg_date
        car_age_years = calculate_car_age_years(
            normalized_reg_date, reference_date=self.reference_date_fn()
        )
        updated_slots["car_age_years"] = car_age_years

        if car_age_years > self.max_car_age_years:
            updated_slots["precheck_passed"] = False
            updated_slots["precheck_code"] = None
            conclusion = {
                "precheck_passed": False,
                "reject_reason": "car_age_exceeds_limit",
                "vin": clean_vin,
                "plate_no": merged_slots.get("plate_no", ""),
                "brand_model": merged_slots.get("brand_model", ""),
                "registration_date": normalized_reg_date,
                "car_age_years": car_age_years,
                "max_car_age_years": self.max_car_age_years,
                "precheck_code": None,
                "ocr_provider": merged_slots.get("ocr_provider", "text"),
                "ocr_confidence": merged_slots.get("ocr_confidence", 1.0),
            }
            return StepResult(
                status="completed",
                reply_text=(
                    f"已核对行驶证（VIN：{clean_vin}），核算车龄约 {car_age_years} 年，"
                    f"超出当前线上准入上限（{self.max_car_age_years:.0f} 年），"
                    "后续可为您安排专属经理评估替代融资方案。"
                ),
                next_node_id=self.initial_node_id,
                updated_slots=updated_slots,
                conclusion=conclusion,
                used_llm=used_multimodal,
            )

        precheck_code = generate_precheck_code(
            clean_vin, ctx.entity_id, prefix=self.precheck_code_prefix
        )
        updated_slots["precheck_passed"] = True
        updated_slots["precheck_code"] = precheck_code
        conclusion = {
            "precheck_passed": True,
            "precheck_code": precheck_code,
            "vin": clean_vin,
            "plate_no": merged_slots.get("plate_no", ""),
            "brand_model": merged_slots.get("brand_model", ""),
            "registration_date": normalized_reg_date,
            "car_age_years": car_age_years,
            "ocr_provider": merged_slots.get("ocr_provider", "text"),
            "ocr_confidence": merged_slots.get("ocr_confidence", 1.0),
        }
        return StepResult(
            status="completed",
            reply_text=(
                f"行驶证预审已通过！识别车架号 {clean_vin}，核算车龄 {car_age_years} 年，"
                f"您的专属预审码为 {precheck_code}。"
            ),
            next_node_id=self.initial_node_id,
            updated_slots=updated_slots,
            conclusion=conclusion,
            used_llm=used_multimodal,
        )


def load_auto_finance_plugin(
    *,
    registry: Optional[SubFlowRegistry] = None,
    yaml_path: str | Path = DEFAULT_AUTO_FINANCE_YAML_PATH,
    vision_client: Optional[BaseVisionOcrClient] = None,
    credential_proxy: Optional[RsiCredentialProxy] = None,
    http_transport: Optional[httpx.AsyncBaseTransport] = None,
    reference_date_fn: Optional[Callable[[], date]] = None,
    overwrite: bool = True,
) -> Dict[str, Any]:
    """Load `auto_finance_rsi.yaml` + multimodal handler into a SubFlowRegistry."""
    target_registry = registry if registry is not None else subflow_registry
    raw = yaml.safe_load(Path(yaml_path).read_text(encoding="utf-8")) or {}

    qa_topics = [
        GlobalQATopicSpec.model_validate(item)
        for item in (raw.get("global_qa_topics") or [])
    ]
    http_tools = [
        RsiHttpToolSpec.model_validate(item) for item in (raw.get("http_tools") or [])
    ]
    mcp_servers = [
        McpToolServerSpec.model_validate(item) for item in (raw.get("mcp_servers") or [])
    ]

    registered_ids: List[str] = []
    subflows_map = raw.get("subflows") or {}
    for sid, cfg in subflows_map.items():
        item = dict(cfg or {})
        item.setdefault("subflow_id", sid)
        handler_type = str(item.get("handler_type") or "yaml")
        if handler_type == "python_multimodal":
            handler: BaseSubFlowHandler = VehicleLicensePrecheckHandler(
                vision_client=vision_client,
                max_car_age_years=float(item.get("max_car_age_years", 10.0)),
                fixed_opening=str(item.get("fixed_opening") or ""),
                resume_hint=str(item.get("resume_hint") or ""),
                qa_topics=qa_topics,
                reference_date_fn=reference_date_fn,
            )
            target_registry.register(handler, overwrite=overwrite)
            registered_ids.append(handler.subflow_id)
        else:
            existing_qa = [
                GlobalQATopicSpec.model_validate(q)
                for q in (item.get("qa_topics") or [])
            ]
            spec = YamlSubFlowSpec.model_validate(
                {
                    **item,
                    "qa_topics": [q.model_dump() for q in (existing_qa or qa_topics)],
                    "http_tools": [
                        t.model_dump()
                        for t in (item.get("http_tools") or http_tools)
                    ],
                    "mcp_servers": [
                        m.model_dump()
                        for m in (item.get("mcp_servers") or mcp_servers)
                    ],
                }
            )
            yaml_handler = YamlSubFlowHandler(
                spec,
                credential_proxy=credential_proxy,
                http_transport=http_transport,
            )
            target_registry.register(yaml_handler, overwrite=overwrite)
            registered_ids.append(yaml_handler.subflow_id)

    return {
        "domain_id": raw.get("domain_id", "auto_finance"),
        "policy_id": raw.get("policy_id", "auto_finance_v1"),
        "policy_config": dict(raw.get("policy_config") or {}),
        "registered_subflow_ids": registered_ids,
        "global_qa_topics": qa_topics,
        "mcp_servers": mcp_servers,
    }


async def ensure_auto_finance_policy(
    *,
    tenant_id: str = "default",
    plugin_meta: Optional[Dict[str, Any]] = None,
) -> FollowupPolicy:
    """Persist the YAML-defined Auto Finance FollowupPolicy into DB for a tenant."""
    meta = plugin_meta or yaml.safe_load(
        Path(DEFAULT_AUTO_FINANCE_YAML_PATH).read_text(encoding="utf-8")
    )
    policy_id = str(meta.get("policy_id") or "auto_finance_v1")
    policy_cfg = dict(meta.get("policy_config") or {})
    existing = await FollowupPolicy.get_or_none(
        tenant_id=tenant_id, policy_id=policy_id
    )
    if existing is not None:
        existing.config = policy_cfg
        existing.is_active = True
        await existing.save()
        return existing
    return await FollowupPolicy.create(
        tenant_id=tenant_id,
        policy_id=policy_id,
        name=str(meta.get("domain_name") or "Auto Finance Follow-up"),
        version=str(meta.get("version") or "1.0"),
        config=policy_cfg,
        is_active=True,
    )
