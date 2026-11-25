"""Edge Channel Adapters & ActionResultStore package."""
from app.rsi.adapters.base import (
    AdapterRegistry,
    BaseChannelAdapter,
    ack_action_result,
    query_action_results,
    record_action_result,
)
from app.rsi.adapters.crm_passive import CrmPassiveAdapter
from app.rsi.adapters.voice import VoiceChannelAdapter
from app.rsi.adapters.webhook import WebhookChannelAdapter


def build_default_adapter_registry() -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(CrmPassiveAdapter())
    registry.register(WebhookChannelAdapter())
    registry.register(VoiceChannelAdapter())
    return registry


default_adapter_registry = build_default_adapter_registry()

__all__ = [
    "BaseChannelAdapter",
    "AdapterRegistry",
    "CrmPassiveAdapter",
    "WebhookChannelAdapter",
    "VoiceChannelAdapter",
    "default_adapter_registry",
    "build_default_adapter_registry",
    "record_action_result",
    "query_action_results",
    "ack_action_result",
]
