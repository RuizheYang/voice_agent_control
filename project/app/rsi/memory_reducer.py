"""L1 immutable event ledger ingest, deterministic L2 memory reducer, and full Replay."""
from __future__ import annotations

import copy
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from tortoise.exceptions import IntegrityError
from tortoise.transactions import in_transaction

from app.rsi.coalescer import message_coalescer
from app.rsi.guardrails.outbox_guardrail import cancel_entity_outbox
from app.rsi.identity import merge_entities, resolve_canonical_entity, resolve_or_create_entity
from app.rsi.models import (
    EntityAlias,
    EntityEventLog,
    EntityMemoryState,
    EntityWorkingMemory,
    FollowupEntity,
    FollowupPolicy,
)
from app.rsi.rule_gateway import _parse_iso_dt, evaluate_rule_gateway
from app.rsi.schemas import (
    EntityKeyRef,
    EntityReplayResponse,
    EntityStateResponse,
    EntityTimelineItem,
    EntityTimelineResponse,
    EventIngestResult,
    RSIEventEnvelope,
)
from app.rsi.timers import boost_entity_tasks, cancel_entity_tasks, schedule_timer
from app.rsi.worker import notify_rsi_worker

logger = logging.getLogger(__name__)


def to_utc(dt: datetime) -> datetime:
    """Normalize naive or aware datetime cleanly to UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _db(queryset, connection):
    return queryset.using_db(connection) if connection is not None else queryset


def _default_hard_boundaries() -> Dict[str, Any]:
    return {
        "dnd": False,
        "blacklisted": False,
        "human_takeover_until": None,
        "risk_flags": [],
        "erasure_requested": False,
    }


def _default_lifecycle() -> Dict[str, Any]:
    return {
        "stage": "new",
        "converted": False,
        "churn_risk": None,
        "churn_signals": [],
        "fulfillment_status": None,
        "fulfillment_detail": {},
        "orders": {},
    }


def _default_ownership() -> Dict[str, Any]:
    return {
        "current_role": None,
        "owner_id": None,
        "handoff_history": [],
        "pending_approvals": {},
    }


def _default_channel_stats() -> Dict[str, Any]:
    return {
        "by_channel": {},
        "wechat_added": False,
        "content_signals": [],
        "private_domain_events": [],
        "broadcasts": [],
        "last_receipt": None,
    }


def _lww_accept(watermarks: Dict[str, str], slot: str, occurred_at: datetime) -> bool:
    """Slot-level Last-Write-Wins guard protecting against out-of-order arrival."""
    occ = to_utc(occurred_at)
    prev_raw = watermarks.get(slot)
    if prev_raw:
        try:
            prev_dt = to_utc(datetime.fromisoformat(prev_raw))
            if occ < prev_dt:
                return False
        except ValueError:
            pass
    watermarks[slot] = occ.isoformat()
    return True


def _append_bounded_item(
    items: List[Dict[str, Any]],
    new_item: Dict[str, Any],
    *,
    max_items: int,
) -> List[Dict[str, Any]]:
    eid = new_item.get("event_id")
    filtered = [it for it in items if it.get("event_id") != eid]
    filtered.append(new_item)
    filtered.sort(key=lambda it: (it.get("occurred_at") or "", str(it.get("event_id") or "")))
    if len(filtered) > max_items:
        filtered = filtered[-max_items:]
    return filtered


def _is_voice_answered(event_type: str, facts: Dict[str, Any]) -> Optional[bool]:
    if "answered" in facts and facts["answered"] is not None:
        return bool(facts["answered"])
    outcome = str(facts.get("outcome") or "").strip().lower()
    if outcome == "answered":
        return True
    if outcome in ("no_answer", "unanswered", "busy", "rejected", "hangup_short", "power_off", "voicemail"):
        return False
    if event_type == "voice.call_answered":
        return True
    if event_type in ("voice.call_unanswered", "voice.call_no_answer"):
        return False
    return None


def apply_event_to_l2(
    state: EntityMemoryState,
    *,
    event_id: str,
    event_type: str,
    category: str,
    occurred_at: datetime,
    actor_type: str,
    actor_id: Optional[str],
    facts: Dict[str, Any],
    summary: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Deterministically fold a single L1 event into L2 EntityMemoryState slots."""
    occ = to_utc(occurred_at)
    occ_iso = occ.isoformat()
    facts_copy = copy.deepcopy(facts or {})

    hard_boundaries = copy.deepcopy(state.hard_boundaries) if isinstance(state.hard_boundaries, dict) and state.hard_boundaries else _default_hard_boundaries()
    commitments = copy.deepcopy(state.commitments) if isinstance(state.commitments, list) else []
    lifecycle = copy.deepcopy(state.lifecycle) if isinstance(state.lifecycle, dict) and state.lifecycle else _default_lifecycle()
    ownership = copy.deepcopy(state.ownership) if isinstance(state.ownership, dict) and state.ownership else _default_ownership()
    channel_stats = copy.deepcopy(state.channel_stats) if isinstance(state.channel_stats, dict) and state.channel_stats else _default_channel_stats()
    watermarks = copy.deepcopy(state.slot_watermarks) if isinstance(state.slot_watermarks, dict) else {}

    by_channel = channel_stats.setdefault("by_channel", {})

    if category == "voice":
        voice = dict(by_channel.get("voice") or {})
        voice["count"] = int(voice.get("count", 0)) + 1
        answered_flag = _is_voice_answered(event_type, facts_copy)
        if answered_flag is True:
            voice["answered_count"] = int(voice.get("answered_count", 0)) + 1
        elif "answered_count" not in voice:
            voice["answered_count"] = 0

        if _lww_accept(watermarks, "channel_stats.voice", occ):
            voice["last_at"] = occ_iso
            if facts_copy.get("outcome") is not None:
                voice["last_outcome"] = str(facts_copy["outcome"])
            elif answered_flag is not None:
                voice["last_outcome"] = "answered" if answered_flag else "no_answer"
            hangup_code = facts_copy.get("hangup_code") or facts_copy.get("sip_code") or facts_copy.get("result_code")
            if hangup_code is not None:
                voice["last_hangup_code"] = str(hangup_code)
            if answered_flag is True:
                voice["no_answer_streak"] = 0
            elif answered_flag is False:
                voice["no_answer_streak"] = int(voice.get("no_answer_streak", 0)) + 1
            elif "no_answer_streak" not in voice:
                voice["no_answer_streak"] = 0
        by_channel["voice"] = voice

    elif category == "wechat":
        wechat = dict(by_channel.get("wechat") or {})
        wechat["count"] = int(wechat.get("count", 0)) + 1
        if _lww_accept(watermarks, "channel_stats.wechat", occ):
            wechat["last_at"] = occ_iso
            wechat["last_actor_type"] = actor_type
            wechat["last_event_type"] = event_type
        by_channel["wechat"] = wechat

        if event_type in ("wechat.friend_added", "wechat.contact_added") or facts_copy.get("wechat_added") is True:
            if _lww_accept(watermarks, "channel_stats.wechat_added", occ):
                channel_stats["wechat_added"] = True
        elif event_type == "wechat.friend_deleted" or facts_copy.get("wechat_added") is False:
            if _lww_accept(watermarks, "channel_stats.wechat_added", occ):
                channel_stats["wechat_added"] = False

    elif category == "messaging":
        ch_name = str(facts_copy.get("channel") or "messaging")
        msg_stats = dict(by_channel.get(ch_name) or {})
        msg_stats["count"] = int(msg_stats.get("count", 0)) + 1
        if _lww_accept(watermarks, f"channel_stats.msg:{ch_name}", occ):
            msg_stats["last_at"] = occ_iso
            msg_stats["last_actor_type"] = actor_type
            msg_stats["last_event_type"] = event_type
        by_channel[ch_name] = msg_stats

    elif category == "content_engagement":
        item = {
            "event_id": event_id,
            "event_type": event_type,
            "occurred_at": occ_iso,
            "page": facts_copy.get("page") or facts_copy.get("content"),
            "score": facts_copy.get("score"),
            "summary": summary,
            "facts": facts_copy,
        }
        channel_stats["content_signals"] = _append_bounded_item(
            list(channel_stats.get("content_signals") or []),
            item,
            max_items=20,
        )

    elif category == "private_domain":
        item = {
            "event_id": event_id,
            "event_type": event_type,
            "occurred_at": occ_iso,
            "summary": summary,
            "facts": facts_copy,
        }
        channel_stats["private_domain_events"] = _append_bounded_item(
            list(channel_stats.get("private_domain_events") or []),
            item,
            max_items=20,
        )

    elif category == "churn_signal":
        if _lww_accept(watermarks, "lifecycle.churn_risk", occ):
            lifecycle["churn_risk"] = facts_copy.get("risk_level") or event_type
        item = {
            "event_id": event_id,
            "event_type": event_type,
            "occurred_at": occ_iso,
            "risk_level": facts_copy.get("risk_level"),
            "summary": summary,
            "facts": facts_copy,
        }
        lifecycle["churn_signals"] = _append_bounded_item(
            list(lifecycle.get("churn_signals") or []),
            item,
            max_items=10,
        )

    elif category == "fulfillment":
        if _lww_accept(watermarks, "lifecycle.fulfillment", occ):
            lifecycle["fulfillment_status"] = facts_copy.get("status") or event_type
            lifecycle["fulfillment_detail"] = facts_copy

    elif category == "finance_order":
        order_id = str(facts_copy.get("order_id") or event_id)
        orders = dict(lifecycle.get("orders") or {})
        raw_status = str(facts_copy.get("status") or event_type.split(".")[-1])
        if _lww_accept(watermarks, f"order:{order_id}", occ):
            orders[order_id] = {
                "order_id": order_id,
                "status": raw_status,
                "amount": facts_copy.get("amount"),
                "currency": facts_copy.get("currency"),
                "updated_at": occ_iso,
                "facts": facts_copy,
            }
            lifecycle["orders"] = orders
        if any(
            str(o.get("status") or "").lower() in ("paid", "completed", "settled")
            for o in (lifecycle.get("orders") or {}).values()
        ) or (event_type in ("order.paid", "order.completed") and watermarks.get(f"order:{order_id}") == occ_iso):
            lifecycle["converted"] = True

    elif category == "risk_boundary":
        if "dnd" in facts_copy or event_type in ("risk.dnd_set", "risk.dnd_cleared"):
            if _lww_accept(watermarks, "hard_boundaries.dnd", occ):
                hard_boundaries["dnd"] = (
                    bool(facts_copy["dnd"]) if "dnd" in facts_copy else (event_type == "risk.dnd_set")
                )

        if facts_copy.get("blacklisted") is True or event_type in ("risk.blacklisted", "risk.complaint"):
            if _lww_accept(watermarks, "hard_boundaries.blacklisted", occ):
                hard_boundaries["blacklisted"] = True
                hard_boundaries["blacklist_reason"] = facts_copy.get("reason") or summary or event_type
        elif facts_copy.get("blacklisted") is False or event_type == "risk.blacklist_cleared":
            if _lww_accept(watermarks, "hard_boundaries.blacklisted", occ):
                hard_boundaries["blacklisted"] = False
                hard_boundaries.pop("blacklist_reason", None)

        flags = list(hard_boundaries.get("risk_flags") or [])
        incoming_flags: List[str] = []
        if facts_copy.get("risk_flag"):
            incoming_flags.append(str(facts_copy["risk_flag"]))
        if isinstance(facts_copy.get("risk_flags"), list):
            incoming_flags.extend(str(f) for f in facts_copy["risk_flags"] if f)
        for flag in incoming_flags:
            if flag not in flags:
                flags.append(flag)
        hard_boundaries["risk_flags"] = flags

        if event_type == "risk.erasure_requested" or facts_copy.get("erasure_requested") is True:
            if _lww_accept(watermarks, "hard_boundaries.erasure_requested", occ):
                hard_boundaries["erasure_requested"] = True

    elif category == "human_touch":
        human_stats = dict(by_channel.get("human") or {})
        human_stats["count"] = int(human_stats.get("count", 0)) + 1
        if _lww_accept(watermarks, "channel_stats.human", occ):
            human_stats["last_at"] = occ_iso
            human_stats["last_actor_id"] = actor_id
        by_channel["human"] = human_stats

        if event_type == "human.takeover_ended" or facts_copy.get("takeover") is False:
            if _lww_accept(watermarks, "hard_boundaries.human_takeover", occ):
                hard_boundaries["human_takeover_until"] = None
        elif (
            event_type == "human.takeover_started"
            or facts_copy.get("takeover_until") is not None
            or (actor_type == "human_rep" and facts_copy.get("takeover_minutes") is not None)
        ):
            if _lww_accept(watermarks, "hard_boundaries.human_takeover", occ):
                raw_until = facts_copy.get("takeover_until")
                if raw_until is not None:
                    if isinstance(raw_until, datetime):
                        hard_boundaries["human_takeover_until"] = to_utc(raw_until).isoformat()
                    else:
                        hard_boundaries["human_takeover_until"] = str(raw_until)
                else:
                    mins = int(facts_copy.get("takeover_minutes", 30))
                    hard_boundaries["human_takeover_until"] = (occ + timedelta(minutes=mins)).isoformat()
                owner = actor_id or facts_copy.get("owner_id")
                if owner:
                    hard_boundaries["human_owner_id"] = owner

    elif category == "approval":
        approval_id = str(facts_copy.get("approval_id") or event_id)
        approvals = dict(ownership.get("pending_approvals") or {})
        if _lww_accept(watermarks, f"approval:{approval_id}", occ):
            status_val = facts_copy.get("status")
            if not status_val:
                if facts_copy.get("approved") is True:
                    status_val = "approved"
                elif facts_copy.get("approved") is False:
                    status_val = "rejected"
                else:
                    status_val = "pending"
            approvals[approval_id] = {
                "approval_id": approval_id,
                "status": str(status_val),
                "reviewer": facts_copy.get("reviewer"),
                "comment": facts_copy.get("comment"),
                "tool_name": facts_copy.get("tool_name"),
                "updated_at": occ_iso,
            }
            ownership["pending_approvals"] = approvals

    elif category == "role_handoff":
        if _lww_accept(watermarks, "ownership.current", occ):
            role = facts_copy.get("to_role") or facts_copy.get("role")
            owner = facts_copy.get("to_owner_id") or facts_copy.get("owner_id") or actor_id
            if role:
                ownership["current_role"] = role
            if owner:
                ownership["owner_id"] = owner
        item = {
            "event_id": event_id,
            "occurred_at": occ_iso,
            "from_role": facts_copy.get("from_role"),
            "to_role": facts_copy.get("to_role") or facts_copy.get("role"),
            "owner_id": facts_copy.get("to_owner_id") or facts_copy.get("owner_id") or actor_id,
            "reason": facts_copy.get("reason") or summary,
        }
        ownership["handoff_history"] = _append_bounded_item(
            list(ownership.get("handoff_history") or []),
            item,
            max_items=20,
        )

    elif category == "commitment":
        cid = str(facts_copy.get("commitment_id") or event_id)
        if _lww_accept(watermarks, f"commitment:{cid}", occ):
            due_val = facts_copy.get("due_at")
            if isinstance(due_val, datetime):
                due_val = to_utc(due_val).isoformat()
            entry = {
                "commitment_id": cid,
                "description": str(facts_copy.get("description") or summary or ""),
                "due_at": due_val,
                "status": str(facts_copy.get("status") or "open"),
                "source_event_id": event_id,
                "occurred_at": occ_iso,
            }
            commitments = [c for c in commitments if c.get("commitment_id") != cid]
            commitments.append(entry)

    elif category == "lifecycle":
        stage = str(facts_copy.get("stage") or event_type)
        if _lww_accept(watermarks, "lifecycle.stage", occ):
            lifecycle["stage"] = stage
            if stage.lower() in ("converted", "won") or event_type == "lifecycle.converted":
                lifecycle["converted"] = True

    elif category == "business_broadcast":
        item = {
            "event_id": event_id,
            "event_type": event_type,
            "occurred_at": occ_iso,
            "summary": summary,
            "facts": facts_copy,
        }
        channel_stats["broadcasts"] = _append_bounded_item(
            list(channel_stats.get("broadcasts") or []),
            item,
            max_items=10,
        )

    elif category == "action_receipt":
        if _lww_accept(watermarks, "channel_stats.last_receipt", occ):
            channel_stats["last_receipt"] = {
                "event_id": event_id,
                "event_type": event_type,
                "occurred_at": occ_iso,
                "facts": facts_copy,
            }

    state.hard_boundaries = hard_boundaries
    state.commitments = commitments
    state.lifecycle = lifecycle
    state.ownership = ownership
    state.channel_stats = channel_stats
    state.slot_watermarks = watermarks


async def _ensure_memory_state(
    tenant_id: str,
    entity_id: str,
    *,
    connection=None,
    for_update: bool = False,
) -> EntityMemoryState:
    qs = _db(
        EntityMemoryState.filter(tenant_id=tenant_id, entity_id=entity_id),
        connection,
    )
    if for_update:
        qs = qs.select_for_update()
    state = await qs.first()
    if state is not None:
        return state
    kwargs = dict(
        tenant_id=tenant_id,
        entity_id=entity_id,
        hard_boundaries=_default_hard_boundaries(),
        commitments=[],
        lifecycle=_default_lifecycle(),
        ownership=_default_ownership(),
        channel_stats=_default_channel_stats(),
        slot_watermarks={},
    )
    try:
        return (
            await EntityMemoryState.create(using_db=connection, **kwargs)
            if connection is not None
            else await EntityMemoryState.create(**kwargs)
        )
    except IntegrityError:
        qs2 = _db(EntityMemoryState.filter(tenant_id=tenant_id, entity_id=entity_id), connection)
        if for_update:
            qs2 = qs2.select_for_update()
        return await qs2.get()


async def _ensure_working_memory(tenant_id: str, entity_id: str, connection=None) -> EntityWorkingMemory:
    existing = await _db(
        EntityWorkingMemory.filter(tenant_id=tenant_id, entity_id=entity_id),
        connection,
    ).first()
    if existing is not None:
        return existing
    try:
        kwargs = dict(
            tenant_id=tenant_id,
            entity_id=entity_id,
            customer_profile_notes={},
            rolling_digest=[],
            active_guidance="",
        )
        return (
            await EntityWorkingMemory.create(using_db=connection, **kwargs)
            if connection is not None
            else await EntityWorkingMemory.create(**kwargs)
        )
    except IntegrityError:
        return await _db(
            EntityWorkingMemory.filter(tenant_id=tenant_id, entity_id=entity_id),
            connection,
        ).get()


def _derive_entity_status(
    current_status: str,
    hard_boundaries: Dict[str, Any],
    lifecycle: Dict[str, Any],
) -> str:
    if hard_boundaries.get("blacklisted"):
        return "blacklisted"
    if lifecycle.get("converted"):
        return "converted"
    if current_status == "paused":
        return "paused"
    return "active"


async def _build_state_response(
    entity: FollowupEntity,
    state: Optional[EntityMemoryState],
    working: Optional[EntityWorkingMemory],
    aliases: List[EntityAlias],
) -> EntityStateResponse:
    hb = state.hard_boundaries if state and isinstance(state.hard_boundaries, dict) and state.hard_boundaries else _default_hard_boundaries()
    cm = state.commitments if state and isinstance(state.commitments, list) else []
    lc = state.lifecycle if state and isinstance(state.lifecycle, dict) and state.lifecycle else _default_lifecycle()
    ow = state.ownership if state and isinstance(state.ownership, dict) and state.ownership else _default_ownership()
    cs = state.channel_stats if state and isinstance(state.channel_stats, dict) and state.channel_stats else _default_channel_stats()
    wm_dict = (
        {
            "customer_profile_notes": working.customer_profile_notes or {},
            "rolling_digest": working.rolling_digest or [],
            "active_guidance": working.active_guidance or "",
            "last_dreamed_at": working.last_dreamed_at.isoformat() if working.last_dreamed_at else None,
        }
        if working
        else {
            "customer_profile_notes": {},
            "rolling_digest": [],
            "active_guidance": "",
            "last_dreamed_at": None,
        }
    )
    return EntityStateResponse(
        tenant_id=entity.tenant_id,
        entity_id=entity.entity_id,
        policy_id=entity.policy_id,
        status=entity.status,
        merged_into_entity_id=entity.merged_into_entity_id,
        state_version=entity.state_version,
        projection_version=state.projection_version if state else 0,
        inbound_msg_seq=entity.inbound_msg_seq,
        last_event_at=entity.last_event_at,
        aliases=[EntityKeyRef(key_type=a.key_type, key_value=a.key_value) for a in aliases],
        hard_boundaries=hb,
        commitments=cm,
        lifecycle=lc,
        ownership=ow,
        channel_stats=cs,
        working_memory=wm_dict,
    )


async def ingest_event(envelope: RSIEventEnvelope) -> EventIngestResult:
    """Idempotently record event in L1 ledger and synchronously fold into L2 state."""
    async with in_transaction() as conn:
        existing_log = await EntityEventLog.filter(
            tenant_id=envelope.tenant_id,
            event_id=envelope.event_id,
        ).using_db(conn).first()
        if existing_log is not None:
            canon = await resolve_canonical_entity(
                envelope.tenant_id, existing_log.entity_id, connection=conn
            )
            resolved_id = canon.entity_id if canon else existing_log.entity_id
            state = await EntityMemoryState.filter(
                tenant_id=envelope.tenant_id, entity_id=resolved_id
            ).using_db(conn).first()
            return EventIngestResult(
                event_id=envelope.event_id,
                tenant_id=envelope.tenant_id,
                entity_id=resolved_id,
                duplicate=True,
                created_entity=False,
                state_version=canon.state_version if canon else 0,
                projection_version=state.projection_version if state else 0,
                inbound_msg_seq=canon.inbound_msg_seq if canon else 0,
            )

        entity, created_entity, warnings = await resolve_or_create_entity(
            tenant_id=envelope.tenant_id,
            entity_id=envelope.entity_id,
            entity_keys=envelope.entity_keys,
            policy_id=envelope.policy_id,
            connection=conn,
        )

        occ = to_utc(envelope.occurred_at)
        try:
            await EntityEventLog.create(
                using_db=conn,
                tenant_id=envelope.tenant_id,
                event_id=envelope.event_id,
                entity_id=entity.entity_id,
                schema_version=envelope.schema_version,
                event_type=envelope.event_type,
                category=envelope.category,
                occurred_at=occ,
                source=envelope.source,
                actor_type=envelope.actor_type,
                actor_id=envelope.actor_id,
                correlation_id=envelope.correlation_id,
                causation_id=envelope.causation_id,
                policy_id=envelope.policy_id,
                summary=envelope.summary,
                facts=envelope.facts,
                extra=envelope.extra,
                entity_keys=[k.model_dump() for k in envelope.entity_keys],
                ingest_status="ingested",
            )
        except IntegrityError:
            dup_log = await EntityEventLog.filter(
                tenant_id=envelope.tenant_id, event_id=envelope.event_id
            ).using_db(conn).first()
            resolved_id = dup_log.entity_id if dup_log else entity.entity_id
            canon = await resolve_canonical_entity(envelope.tenant_id, resolved_id, connection=conn)
            final_id = canon.entity_id if canon else resolved_id
            state = await EntityMemoryState.filter(
                tenant_id=envelope.tenant_id, entity_id=final_id
            ).using_db(conn).first()
            return EventIngestResult(
                event_id=envelope.event_id,
                tenant_id=envelope.tenant_id,
                entity_id=final_id,
                duplicate=True,
                created_entity=False,
                state_version=canon.state_version if canon else 0,
                projection_version=state.projection_version if state else 0,
                inbound_msg_seq=canon.inbound_msg_seq if canon else 0,
            )

        # Handle entity.merged targeting another canonical entity inside the same transaction
        target_id = envelope.facts.get("target_entity_id") if envelope.event_type == "entity.merged" else None
        if target_id and str(target_id).strip() and str(target_id).strip() != entity.entity_id:
            target_entity, _ = await merge_entities(
                envelope.tenant_id,
                entity.entity_id,
                str(target_id).strip(),
                connection=conn,
            )
            await _ensure_working_memory(envelope.tenant_id, target_entity.entity_id, connection=conn)
            replay_res = await replay_entity_state(
                envelope.tenant_id,
                target_entity.entity_id,
                connection=conn,
            )
            refreshed = await FollowupEntity.filter(
                tenant_id=envelope.tenant_id, entity_id=target_entity.entity_id
            ).using_db(conn).get()
            return EventIngestResult(
                event_id=envelope.event_id,
                tenant_id=envelope.tenant_id,
                entity_id=refreshed.entity_id,
                duplicate=False,
                created_entity=created_entity,
                state_version=refreshed.state_version,
                projection_version=replay_res.projection_version if replay_res else 1,
                inbound_msg_seq=refreshed.inbound_msg_seq,
                warnings=warnings,
            )

        locked_entity = await FollowupEntity.filter(id=entity.id).using_db(conn).select_for_update().get()
        state = await _ensure_memory_state(
            envelope.tenant_id, locked_entity.entity_id, connection=conn, for_update=True
        )
        apply_event_to_l2(
            state,
            event_id=envelope.event_id,
            event_type=envelope.event_type,
            category=envelope.category,
            occurred_at=occ,
            actor_type=envelope.actor_type,
            actor_id=envelope.actor_id,
            facts=envelope.facts,
            summary=envelope.summary,
            extra=envelope.extra,
        )

        policy = await FollowupPolicy.filter(
            tenant_id=envelope.tenant_id,
            policy_id=locked_entity.policy_id,
            is_active=True,
        ).using_db(conn).first()
        decision = evaluate_rule_gateway(
            envelope,
            state,
            policy.config if policy and isinstance(policy.config, dict) else None,
        )

        if decision.blacklist_entity:
            watermarks = dict(state.slot_watermarks or {})
            if _lww_accept(watermarks, "hard_boundaries.blacklisted", occ):
                hb = dict(state.hard_boundaries or _default_hard_boundaries())
                hb["blacklisted"] = True
                hb["blacklist_reason"] = decision.blacklist_reason
                state.hard_boundaries = hb
                state.slot_watermarks = watermarks

        if decision.set_takeover_until is not None:
            watermarks = dict(state.slot_watermarks or {})
            if _lww_accept(watermarks, "hard_boundaries.human_takeover", occ):
                hb = dict(state.hard_boundaries or _default_hard_boundaries())
                hb["human_takeover_until"] = to_utc(decision.set_takeover_until).isoformat()
                state.hard_boundaries = hb
                state.slot_watermarks = watermarks

        state.projection_version += 1
        state.last_event_id = envelope.event_id
        await state.save(using_db=conn)

        locked_entity.state_version += 1
        if locked_entity.last_event_at is None or occ > to_utc(locked_entity.last_event_at):
            locked_entity.last_event_at = occ
        if envelope.category in ("wechat", "messaging") and envelope.actor_type == "customer":
            locked_entity.inbound_msg_seq += 1
        locked_entity.status = _derive_entity_status(
            locked_entity.status, state.hard_boundaries, state.lifecycle
        )
        await locked_entity.save(using_db=conn)
        await _ensure_working_memory(envelope.tenant_id, locked_entity.entity_id, connection=conn)

        cancelled_count = 0
        scheduled_task_id: Optional[int] = None
        boosted_count = 0
        should_preempt = False

        if decision.cancel_pending_tasks:
            cancelled_count = await cancel_entity_tasks(
                envelope.tenant_id,
                locked_entity.entity_id,
                reason=decision.matched_rule,
                connection=conn,
            )
            cancelled_count += await cancel_entity_outbox(
                envelope.tenant_id,
                locked_entity.entity_id,
                reason=decision.matched_rule,
                connection=conn,
            )

        if decision.auto_create_commitment_timer:
            due_dt = _parse_iso_dt(envelope.facts.get("due_at"))
            if due_dt is not None:
                cid = str(envelope.facts.get("commitment_id") or envelope.event_id)
                timer_task = await schedule_timer(
                    envelope.tenant_id,
                    locked_entity.entity_id,
                    "timer_commitment",
                    due_dt,
                    payload={
                        "commitment_id": cid,
                        "description": str(envelope.facts.get("description") or envelope.summary or ""),
                        "source_event_id": envelope.event_id,
                    },
                    priority=7,
                    replace_existing=False,
                    connection=conn,
                )
                scheduled_task_id = timer_task.id

        if decision.boost_existing_tasks:
            now_utc = datetime.now(timezone.utc)
            boosted_count = await boost_entity_tasks(
                envelope.tenant_id,
                locked_entity.entity_id,
                new_priority=decision.wakeup_priority,
                run_at=now_utc,
                reason=decision.matched_rule,
                connection=conn,
            )
            if boosted_count == 0 and decision.schedule_wakeup:
                wakeup_task = await schedule_timer(
                    envelope.tenant_id,
                    locked_entity.entity_id,
                    "cognitive_wakeup",
                    now_utc,
                    payload={
                        "trigger_event_id": envelope.event_id,
                        "trigger_reason": decision.matched_rule,
                        "boosted": True,
                    },
                    priority=decision.wakeup_priority,
                    replace_existing=False,
                    connection=conn,
                )
                scheduled_task_id = wakeup_task.id
        elif decision.schedule_wakeup:
            if (
                envelope.category in ("wechat", "messaging")
                and envelope.actor_type == "customer"
                and decision.wakeup_delay_ms >= 0
            ):
                debounced_task = await message_coalescer.enqueue_debounced_message_wakeup(
                    envelope.tenant_id,
                    locked_entity.entity_id,
                    envelope.event_id,
                    debounce_ms=decision.wakeup_delay_ms,
                    priority=decision.wakeup_priority,
                    connection=conn,
                )
                scheduled_task_id = debounced_task.id
                should_preempt = True
            else:
                due_at = datetime.now(timezone.utc) + timedelta(milliseconds=decision.wakeup_delay_ms)
                wakeup_task = await schedule_timer(
                    envelope.tenant_id,
                    locked_entity.entity_id,
                    "cognitive_wakeup",
                    due_at,
                    payload={
                        "trigger_event_id": envelope.event_id,
                        "trigger_reason": decision.matched_rule,
                    },
                    priority=decision.wakeup_priority,
                    replace_existing=False,
                    connection=conn,
                )
                scheduled_task_id = wakeup_task.id

        result = EventIngestResult(
            event_id=envelope.event_id,
            tenant_id=envelope.tenant_id,
            entity_id=locked_entity.entity_id,
            duplicate=False,
            created_entity=created_entity,
            state_version=locked_entity.state_version,
            projection_version=state.projection_version,
            inbound_msg_seq=locked_entity.inbound_msg_seq,
            rule_tier=decision.tier,
            matched_rule=decision.matched_rule,
            scheduled_task_id=scheduled_task_id,
            cancelled_task_count=cancelled_count,
            warnings=warnings,
        )

    if should_preempt:
        message_coalescer.preempt_inflight_run(
            envelope.tenant_id,
            result.entity_id,
            reason=f"new_msg:{envelope.event_id}",
        )
    if scheduled_task_id is not None or boosted_count > 0:
        notify_rsi_worker()

    return result


async def get_entity_state(
    tenant_id: str,
    entity_id: str,
    connection=None,
) -> Optional[EntityStateResponse]:
    """Fetch canonical L2 + L3 state snapshot and bound cross-channel aliases."""
    entity = await resolve_canonical_entity(tenant_id, entity_id, connection=connection)
    if entity is None:
        return None
    state = await _db(
        EntityMemoryState.filter(tenant_id=tenant_id, entity_id=entity.entity_id),
        connection,
    ).first()
    working = await _db(
        EntityWorkingMemory.filter(tenant_id=tenant_id, entity_id=entity.entity_id),
        connection,
    ).first()
    aliases = await _db(
        EntityAlias.filter(tenant_id=tenant_id, entity_id=entity.entity_id),
        connection,
    ).order_by("id").all()
    return await _build_state_response(entity, state, working, aliases)


async def get_entity_timeline(
    tenant_id: str,
    entity_id: str,
    *,
    limit: int = 50,
    category: Optional[str] = None,
) -> Optional[EntityTimelineResponse]:
    """Fetch immutable L1 event ledger timeline for an entity."""
    entity = await resolve_canonical_entity(tenant_id, entity_id)
    if entity is None:
        return None
    qs = EntityEventLog.filter(tenant_id=tenant_id, entity_id=entity.entity_id)
    if category:
        qs = qs.filter(category=category)
    total = await qs.count()
    rows = await qs.order_by("-occurred_at", "-id").limit(limit).all()
    items = [
        EntityTimelineItem(
            id=r.id,
            event_id=r.event_id,
            event_type=r.event_type,
            category=r.category,
            occurred_at=r.occurred_at,
            received_at=r.received_at,
            source=r.source,
            actor_type=r.actor_type,
            actor_id=r.actor_id,
            correlation_id=r.correlation_id,
            causation_id=r.causation_id,
            policy_id=r.policy_id,
            summary=r.summary,
            facts=r.facts or {},
            extra=r.extra or {},
            ingest_status=r.ingest_status,
        )
        for r in rows
    ]
    return EntityTimelineResponse(
        tenant_id=tenant_id,
        entity_id=entity.entity_id,
        total=total,
        items=items,
    )


async def replay_entity_state(
    tenant_id: str,
    entity_id: str,
    connection=None,
) -> Optional[EntityReplayResponse]:
    """Deterministically rebuild L2 EntityMemoryState from immutable L1 event ledger."""

    async def _run_replay(conn) -> Optional[EntityReplayResponse]:
        canon = await resolve_canonical_entity(tenant_id, entity_id, connection=conn)
        if canon is None:
            return None

        locked_entity = await FollowupEntity.filter(id=canon.id).using_db(conn).select_for_update().get()
        state = await _ensure_memory_state(
            tenant_id, locked_entity.entity_id, connection=conn, for_update=True
        )
        events = await EntityEventLog.filter(
            tenant_id=tenant_id, entity_id=locked_entity.entity_id
        ).using_db(conn).order_by("occurred_at", "id").all()

        state.hard_boundaries = _default_hard_boundaries()
        state.commitments = []
        state.lifecycle = _default_lifecycle()
        state.ownership = _default_ownership()
        state.channel_stats = _default_channel_stats()
        state.slot_watermarks = {}
        state.last_event_id = None

        latest_occ: Optional[datetime] = None
        for ev in events:
            occ = to_utc(ev.occurred_at)
            apply_event_to_l2(
                state,
                event_id=ev.event_id,
                event_type=ev.event_type,
                category=ev.category,
                occurred_at=occ,
                actor_type=ev.actor_type,
                actor_id=ev.actor_id,
                facts=ev.facts or {},
                summary=ev.summary,
                extra=ev.extra or {},
            )
            state.last_event_id = ev.event_id
            if latest_occ is None or occ > latest_occ:
                latest_occ = occ

        state.projection_version += 1
        await state.save(using_db=conn)

        locked_entity.status = _derive_entity_status(
            locked_entity.status, state.hard_boundaries, state.lifecycle
        )
        if latest_occ is not None:
            locked_entity.last_event_at = latest_occ
        await locked_entity.save(using_db=conn)

        working = await _ensure_working_memory(tenant_id, locked_entity.entity_id, connection=conn)
        aliases = await EntityAlias.filter(
            tenant_id=tenant_id, entity_id=locked_entity.entity_id
        ).using_db(conn).order_by("id").all()
        state_resp = await _build_state_response(locked_entity, state, working, aliases)
        return EntityReplayResponse(
            tenant_id=tenant_id,
            entity_id=locked_entity.entity_id,
            replayed_events=len(events),
            projection_version=state.projection_version,
            state=state_resp,
        )

    if connection is not None:
        return await _run_replay(connection)
    async with in_transaction() as conn:
        return await _run_replay(conn)
