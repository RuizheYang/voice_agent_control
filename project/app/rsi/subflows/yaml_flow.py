"""Declarative YAML subflow engine, CredentialProxy HTTP tools, and MCP specs.

Allows rapid onboarding of domain subflows (qualification checks, info collection,
tool-assisted pre-checks) purely via YAML configuration without leaking API
credentials into LLM prompts or tool parameter schemas.
"""

from __future__ import annotations

import inspect
import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Type

import httpx
import yaml
from pydantic import BaseModel, Field, model_validator
from pydantic_ai.toolsets import FunctionToolset

from app.rsi.models import RsiTenantAuth
from app.rsi.schemas import ActionProposal
from app.rsi.subflows.base import BaseSubFlowHandler, StepResult, SubFlowRunContext
from app.rsi.subflows.runtime import (
    GlobalQATopicSpec,
    build_dynamic_step_model,
    run_typed_step_agent,
    sanitize_llm_output,
    trim_node_history,
)


class RsiCredentialProxy:
    """Resolves secret headers/tokens server-side from memory, env, or RsiTenantAuth.

    Credentials never appear in LLM prompts, YAML node descriptions, or tool schemas.
    """

    def __init__(self, static_secrets: Optional[Dict[str, str]] = None) -> None:
        self._static_secrets: Dict[str, str] = dict(static_secrets or {})

    def register_secret(self, auth_ref: str, secret_value: str) -> None:
        self._static_secrets[auth_ref] = secret_value

    async def resolve_secret(
        self,
        *,
        tenant_id: str,
        auth_ref: Optional[str] = None,
        env_var: Optional[str] = None,
    ) -> Optional[str]:
        if auth_ref and auth_ref in self._static_secrets:
            return self._static_secrets[auth_ref]

        if env_var:
            val = os.environ.get(env_var)
            if val:
                return val

        if auth_ref:
            row = await RsiTenantAuth.get_or_none(
                tenant_id=tenant_id, app_id=auth_ref, is_active=True
            )
            if row is not None and row.app_secret:
                return row.app_secret
            env_fallback = os.environ.get(auth_ref)
            if env_fallback:
                return env_fallback

        return None

    async def build_headers(
        self,
        *,
        tenant_id: str,
        base_headers: Optional[Dict[str, str]] = None,
        auth_ref: Optional[str] = None,
        env_var: Optional[str] = None,
        auth_header_name: str = "Authorization",
        auth_scheme: str = "Bearer",
    ) -> Dict[str, str]:
        headers = dict(base_headers or {})
        secret = await self.resolve_secret(
            tenant_id=tenant_id, auth_ref=auth_ref, env_var=env_var
        )
        if secret:
            headers[auth_header_name] = (
                f"{auth_scheme} {secret}".strip() if auth_scheme else secret
            )
        return headers


default_credential_proxy = RsiCredentialProxy()


class RsiHttpToolParamSpec(BaseModel):
    name: str
    type: str = "str"
    required: bool = True
    description: str = ""


class RsiHttpToolSpec(BaseModel):
    name: str
    description: str = ""
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    url: str
    headers: Dict[str, str] = Field(default_factory=dict)
    auth_ref: Optional[str] = None
    auth_env_var: Optional[str] = None
    auth_header_name: str = "Authorization"
    auth_scheme: str = "Bearer"
    params: List[RsiHttpToolParamSpec] = Field(default_factory=list)
    timeout_seconds: float = Field(default=10.0, gt=0)


def _param_python_type(type_name: str) -> Any:
    mapping: Dict[str, Any] = {
        "str": str,
        "string": str,
        "int": int,
        "integer": int,
        "float": float,
        "number": float,
        "bool": bool,
        "boolean": bool,
        "dict": Dict[str, Any],
        "list": List[Any],
    }
    return mapping.get(type_name.strip().lower(), str)


def build_http_toolset(
    tool_specs: Sequence[RsiHttpToolSpec],
    *,
    tenant_id: str = "default",
    credential_proxy: Optional[RsiCredentialProxy] = None,
    transport: Optional[httpx.AsyncBaseTransport] = None,
    toolset_id: str = "rsi_http_tools",
) -> FunctionToolset:
    """Compile HTTP tool specs into a Pydantic-AI FunctionToolset with secret injection."""
    proxy = credential_proxy or default_credential_proxy
    toolset = FunctionToolset(id=toolset_id)

    for spec in tool_specs:

        def _make_fn(s: RsiHttpToolSpec):
            sig_params: List[inspect.Parameter] = []
            annotations: Dict[str, Any] = {}
            for p in s.params:
                py_t = _param_python_type(p.type)
                ann = py_t if p.required else Optional[py_t]
                default_val = inspect.Parameter.empty if p.required else None
                sig_params.append(
                    inspect.Parameter(
                        p.name,
                        kind=inspect.Parameter.KEYWORD_ONLY,
                        default=default_val,
                        annotation=ann,
                    )
                )
                annotations[p.name] = ann
            annotations["return"] = Dict[str, Any]

            async def _http_tool(**kwargs: Any) -> Dict[str, Any]:
                headers = await proxy.build_headers(
                    tenant_id=tenant_id,
                    base_headers=s.headers,
                    auth_ref=s.auth_ref,
                    env_var=s.auth_env_var,
                    auth_header_name=s.auth_header_name,
                    auth_scheme=s.auth_scheme,
                )
                client_kwargs: Dict[str, Any] = {"timeout": s.timeout_seconds}
                if transport is not None:
                    client_kwargs["transport"] = transport

                async with httpx.AsyncClient(**client_kwargs) as client:
                    if s.method == "GET":
                        resp = await client.get(s.url, params=kwargs, headers=headers)
                    else:
                        resp = await client.request(
                            s.method, s.url, json=kwargs, headers=headers
                        )
                    resp.raise_for_status()
                    try:
                        body = resp.json()
                    except Exception:
                        body = {"text": resp.text}
                    return {"status_code": resp.status_code, "data": body}

            _http_tool.__name__ = s.name
            _http_tool.__doc__ = s.description or f"HTTP {s.method} tool {s.name}"
            _http_tool.__signature__ = inspect.Signature(sig_params)  # type: ignore[attr-defined]
            _http_tool.__annotations__ = annotations
            return _http_tool

        toolset.add_function(
            _make_fn(spec),
            name=spec.name,
            description=spec.description or f"Call external HTTP tool {spec.name}",
            takes_ctx=False,
        )

    return toolset


class McpToolServerSpec(BaseModel):
    """Declarative MCP server attachment specification for YAML subflows."""

    server_id: str
    transport: Literal["streamable_http", "sse", "stdio"] = "streamable_http"
    url: Optional[str] = None
    command: Optional[str] = None
    args: List[str] = Field(default_factory=list)
    headers: Dict[str, str] = Field(default_factory=dict)
    auth_ref: Optional[str] = None
    auth_env_var: Optional[str] = None
    enabled: bool = True

    async def build_resolved_headers(
        self,
        *,
        tenant_id: str,
        credential_proxy: Optional[RsiCredentialProxy] = None,
    ) -> Dict[str, str]:
        proxy = credential_proxy or default_credential_proxy
        return await proxy.build_headers(
            tenant_id=tenant_id,
            base_headers=self.headers,
            auth_ref=self.auth_ref,
            env_var=self.auth_env_var,
        )


class YamlSubFlowNodeSpec(BaseModel):
    node_id: str
    prompt: str
    fixed_opening: Optional[str] = None
    output_fields: List[Dict[str, Any]] = Field(default_factory=list)
    next_node_id: Optional[str] = None
    completion_reply: str = ""
    resume_hint: str = ""
    tools: List[str] = Field(default_factory=list)
    max_history_turns: int = Field(default=12, ge=1)
    outbound_channel: Optional[str] = None


class YamlSubFlowSpec(BaseModel):
    subflow_id: str
    description: str = ""
    initial_node_id: Optional[str] = None
    nodes: Dict[str, YamlSubFlowNodeSpec]
    qa_topics: List[GlobalQATopicSpec] = Field(default_factory=list)
    http_tools: List[RsiHttpToolSpec] = Field(default_factory=list)
    mcp_servers: List[McpToolServerSpec] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _normalize_nodes(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        raw_nodes = data.get("nodes")
        if isinstance(raw_nodes, list):
            normalized: Dict[str, Any] = {}
            first_id: Optional[str] = None
            for item in raw_nodes:
                if isinstance(item, dict) and "node_id" in item:
                    nid = str(item["node_id"])
                    if first_id is None:
                        first_id = nid
                    normalized[nid] = item
            data = dict(data)
            data["nodes"] = normalized
            if not data.get("initial_node_id") and first_id:
                data["initial_node_id"] = first_id
        elif isinstance(raw_nodes, dict):
            normalized = {}
            first_id = None
            for nid, item in raw_nodes.items():
                if first_id is None:
                    first_id = str(nid)
                if isinstance(item, dict):
                    item_copy = dict(item)
                    item_copy.setdefault("node_id", str(nid))
                    normalized[str(nid)] = item_copy
                else:
                    normalized[str(nid)] = item
            data = dict(data)
            data["nodes"] = normalized
            if not data.get("initial_node_id") and first_id:
                data["initial_node_id"] = first_id
        return data


class YamlSubFlowHandler(BaseSubFlowHandler):
    """SubFlowHandler compiled from declarative YAML + HTTP/MCP tool definitions."""

    def __init__(
        self,
        spec: YamlSubFlowSpec,
        *,
        credential_proxy: Optional[RsiCredentialProxy] = None,
        http_transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self.spec = spec
        self.subflow_id = spec.subflow_id
        self.description = spec.description
        self.initial_node_id = spec.initial_node_id or next(iter(spec.nodes.keys()))
        self.qa_topics = list(spec.qa_topics)
        self.credential_proxy = credential_proxy or default_credential_proxy
        self.http_transport = http_transport
        self._compiled_schemas: Dict[str, Optional[Type[BaseModel]]] = {}
        for nid, node in spec.nodes.items():
            if node.output_fields:
                self._compiled_schemas[nid] = build_dynamic_step_model(
                    f"{spec.subflow_id}_{nid}_Output",
                    node.output_fields,
                )
            else:
                self._compiled_schemas[nid] = None

    @classmethod
    def from_yaml_str(
        cls,
        yaml_text: str,
        *,
        credential_proxy: Optional[RsiCredentialProxy] = None,
        http_transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> "YamlSubFlowHandler":
        raw = yaml.safe_load(yaml_text) or {}
        spec = YamlSubFlowSpec.model_validate(raw)
        return cls(
            spec,
            credential_proxy=credential_proxy,
            http_transport=http_transport,
        )

    @classmethod
    def from_yaml_file(
        cls,
        file_path: str | Path,
        *,
        credential_proxy: Optional[RsiCredentialProxy] = None,
        http_transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> "YamlSubFlowHandler":
        content = Path(file_path).read_text(encoding="utf-8")
        return cls.from_yaml_str(
            content,
            credential_proxy=credential_proxy,
            http_transport=http_transport,
        )

    def get_node_resume_hint(self, node_id: str) -> str:
        node = self.spec.nodes.get(node_id)
        if node is None:
            return ""
        return node.resume_hint or ""

    def _build_node_toolsets(
        self, node: YamlSubFlowNodeSpec, tenant_id: str
    ) -> List[Any]:
        if not node.tools or not self.spec.http_tools:
            return []
        wanted = set(node.tools)
        selected = [t for t in self.spec.http_tools if t.name in wanted]
        if not selected:
            return []
        return [
            build_http_toolset(
                selected,
                tenant_id=tenant_id,
                credential_proxy=self.credential_proxy,
                transport=self.http_transport,
                toolset_id=f"{self.subflow_id}_{node.node_id}_http",
            )
        ]

    async def step(self, ctx: SubFlowRunContext) -> StepResult:
        node = self.spec.nodes.get(ctx.node_id)
        if node is None:
            raise ValueError(
                f"Node {ctx.node_id!r} not found in YAML subflow {self.subflow_id!r}"
            )

        # 0-LLM deterministic fixed opening upon initial entry with empty incoming message
        if ctx.first_visit_to_node and node.fixed_opening and not ctx.incoming_text.strip():
            return StepResult(
                status="in_progress",
                reply_text=sanitize_llm_output(node.fixed_opening),
                next_node_id=node.node_id,
                used_fixed_opening=True,
                used_llm=False,
            )

        trimmed_ctx = ctx.model_copy(
            update={
                "node_history": trim_node_history(
                    ctx.node_history, max_turns=node.max_history_turns
                )
            }
        )
        toolsets = self._build_node_toolsets(node, ctx.tenant_id)
        output_schema = self._compiled_schemas.get(node.node_id)

        completed, extracted_slots, reply_text = await run_typed_step_agent(
            ctx=trimmed_ctx,
            system_prompt=node.prompt,
            output_schema=output_schema,
            toolsets=toolsets,
            completion_reply=node.completion_reply,
        )

        if not completed:
            return StepResult(
                status="in_progress",
                reply_text=reply_text,
                next_node_id=node.node_id,
                used_fixed_opening=False,
                used_llm=True,
            )

        merged_slots = {**ctx.collected_slots, **extracted_slots}

        if node.next_node_id:
            next_node = self.spec.nodes.get(node.next_node_id)
            used_opening = bool(next_node and next_node.fixed_opening)
            reply_parts: List[str] = []
            if reply_text:
                reply_parts.append(reply_text)
            if next_node and next_node.fixed_opening:
                reply_parts.append(sanitize_llm_output(next_node.fixed_opening))
            combined_reply = "\n".join(p for p in reply_parts if p).strip()

            return StepResult(
                status="in_progress",
                reply_text=combined_reply,
                next_node_id=node.next_node_id,
                updated_slots=extracted_slots,
                used_fixed_opening=used_opening,
                used_llm=True,
            )

        # Terminal node completed
        final_reply = reply_text or sanitize_llm_output(node.completion_reply)
        proposed: List[ActionProposal] = []
        if node.outbound_channel and final_reply:
            proposed.append(
                ActionProposal(
                    tenant_id=ctx.tenant_id,
                    entity_id=ctx.entity_id,
                    idempotency_key=(
                        f"subflow:{self.subflow_id}:{node.node_id}:"
                        f"{ctx.entity_id}:{len(ctx.visited_nodes)}"
                    ),
                    channel=node.outbound_channel,
                    action_type="send_message",
                    payload={"text": final_reply, "subflow_id": self.subflow_id},
                    policy_id=self.subflow_id,
                    source_agent=f"subflow:{self.subflow_id}",
                )
            )

        return StepResult(
            status="completed",
            reply_text=final_reply,
            next_node_id=node.node_id,
            updated_slots=extracted_slots,
            conclusion=merged_slots,
            proposed_actions=proposed,
            used_fixed_opening=False,
            used_llm=True,
        )
