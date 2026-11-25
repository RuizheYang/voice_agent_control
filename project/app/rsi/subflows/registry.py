"""Hot-pluggable registry for declarative YAML and custom Python subflows."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import httpx

from app.rsi.subflows.base import BaseSubFlowHandler
from app.rsi.subflows.yaml_flow import RsiCredentialProxy, YamlSubFlowHandler


class SubFlowRegistry:
    """In-process registry storing registered subflow handlers keyed by `subflow_id`."""

    def __init__(self) -> None:
        self._handlers: Dict[str, BaseSubFlowHandler] = {}

    def register(
        self,
        handler: BaseSubFlowHandler,
        *,
        overwrite: bool = True,
    ) -> BaseSubFlowHandler:
        sid = getattr(handler, "subflow_id", "").strip()
        if not sid:
            raise ValueError("SubFlowHandler must define a non-empty `subflow_id`")
        if sid in self._handlers and not overwrite:
            raise ValueError(f"Subflow {sid!r} is already registered")
        self._handlers[sid] = handler
        return handler

    def register_yaml(
        self,
        yaml_text: str,
        *,
        credential_proxy: Optional[RsiCredentialProxy] = None,
        http_transport: Optional[httpx.AsyncBaseTransport] = None,
        overwrite: bool = True,
    ) -> YamlSubFlowHandler:
        handler = YamlSubFlowHandler.from_yaml_str(
            yaml_text,
            credential_proxy=credential_proxy,
            http_transport=http_transport,
        )
        self.register(handler, overwrite=overwrite)
        return handler

    def register_yaml_file(
        self,
        file_path: str | Path,
        *,
        credential_proxy: Optional[RsiCredentialProxy] = None,
        http_transport: Optional[httpx.AsyncBaseTransport] = None,
        overwrite: bool = True,
    ) -> YamlSubFlowHandler:
        handler = YamlSubFlowHandler.from_yaml_file(
            file_path,
            credential_proxy=credential_proxy,
            http_transport=http_transport,
        )
        self.register(handler, overwrite=overwrite)
        return handler

    def load_yaml_dir(
        self,
        dir_path: str | Path,
        *,
        credential_proxy: Optional[RsiCredentialProxy] = None,
        http_transport: Optional[httpx.AsyncBaseTransport] = None,
        overwrite: bool = True,
    ) -> List[str]:
        directory = Path(dir_path)
        if not directory.is_dir():
            return []
        loaded: List[str] = []
        for path in sorted(directory.iterdir()):
            if path.is_file() and path.suffix.lower() in (".yaml", ".yml"):
                handler = self.register_yaml_file(
                    path,
                    credential_proxy=credential_proxy,
                    http_transport=http_transport,
                    overwrite=overwrite,
                )
                loaded.append(handler.subflow_id)
        return loaded

    def unregister(self, subflow_id: str) -> bool:
        return self._handlers.pop(subflow_id, None) is not None

    def clear(self) -> None:
        self._handlers.clear()

    def has(self, subflow_id: str) -> bool:
        return subflow_id in self._handlers

    def get(self, subflow_id: str) -> BaseSubFlowHandler:
        if subflow_id not in self._handlers:
            raise KeyError(f"Unknown subflow_id: {subflow_id!r}")
        return self._handlers[subflow_id]

    def list_specs(self) -> List[Dict[str, str]]:
        return [
            {
                "subflow_id": sid,
                "description": h.description,
                "initial_node_id": h.initial_node_id,
                "kind": "yaml" if isinstance(h, YamlSubFlowHandler) else "python",
            }
            for sid, h in self._handlers.items()
        ]


subflow_registry = SubFlowRegistry()
