"""Cross-channel entity identity resolution & entity merge helpers."""
from __future__ import annotations

import logging
import uuid
from typing import Dict, List, Optional, Tuple

from tortoise.exceptions import IntegrityError
from tortoise.expressions import Q

from app.rsi.models import EntityAlias, EntityEventLog, FollowupEntity
from app.rsi.schemas import EntityKeyRef

logger = logging.getLogger(__name__)
_MAX_MERGE_HOPS = 10


def _db(queryset, connection):
    return queryset.using_db(connection) if connection is not None else queryset


async def resolve_canonical_entity(
    tenant_id: str,
    entity_id: str,
    connection=None,
) -> Optional[FollowupEntity]:
    """Follow merged_into_entity_id chain (with cycle guard) to return canonical FollowupEntity."""
    current_id = entity_id
    visited = set()
    for _ in range(_MAX_MERGE_HOPS):
        if current_id in visited:
            logger.warning("[RSI-Identity] Merge cycle detected at entity_id=%s", current_id)
            break
        visited.add(current_id)
        entity = await _db(
            FollowupEntity.filter(tenant_id=tenant_id, entity_id=current_id),
            connection,
        ).first()
        if entity is None:
            return None
        if entity.status == "merged" and entity.merged_into_entity_id:
            current_id = entity.merged_into_entity_id
            continue
        return entity
    return await _db(
        FollowupEntity.filter(tenant_id=tenant_id, entity_id=current_id),
        connection,
    ).first()


async def _find_existing_aliases(
    tenant_id: str,
    keys: List[EntityKeyRef],
    connection=None,
) -> Dict[Tuple[str, str], EntityAlias]:
    if not keys:
        return {}
    cond = Q(key_type=keys[0].key_type, key_value=keys[0].key_value)
    for key in keys[1:]:
        cond = cond | Q(key_type=key.key_type, key_value=key.key_value)
    rows = await _db(
        EntityAlias.filter(cond, tenant_id=tenant_id),
        connection,
    ).all()
    return {(r.key_type, r.key_value): r for r in rows}


async def _bind_keys_to_entity(
    tenant_id: str,
    target_entity_id: str,
    keys: List[EntityKeyRef],
    existing_map: Dict[Tuple[str, str], EntityAlias],
    warnings: List[str],
    connection=None,
) -> None:
    for key in keys:
        pair = (key.key_type, key.key_value)
        existing = existing_map.get(pair)
        if existing is not None:
            canonical = await resolve_canonical_entity(tenant_id, existing.entity_id, connection=connection)
            existing_target = canonical.entity_id if canonical else existing.entity_id
            if existing_target != target_entity_id:
                warnings.append(
                    f"alias_conflict:{key.key_type}:{key.key_value}->existing:{existing_target}"
                )
            continue
        try:
            create_kwargs = dict(
                tenant_id=tenant_id,
                key_type=key.key_type,
                key_value=key.key_value,
                entity_id=target_entity_id,
            )
            if connection is not None:
                alias = await EntityAlias.create(using_db=connection, **create_kwargs)
            else:
                alias = await EntityAlias.create(**create_kwargs)
            existing_map[pair] = alias
        except IntegrityError:
            race_row = await _db(
                EntityAlias.filter(tenant_id=tenant_id, key_type=key.key_type, key_value=key.key_value),
                connection,
            ).first()
            if race_row:
                existing_map[pair] = race_row
                if race_row.entity_id != target_entity_id:
                    warnings.append(
                        f"alias_conflict:{key.key_type}:{key.key_value}->existing:{race_row.entity_id}"
                    )


def _dedupe_keys(entity_keys: List[EntityKeyRef]) -> List[EntityKeyRef]:
    seen = set()
    result: List[EntityKeyRef] = []
    for k in entity_keys:
        pair = (k.key_type, k.key_value)
        if pair not in seen:
            seen.add(pair)
            result.append(k)
    return result


async def resolve_or_create_entity(
    tenant_id: str,
    entity_id: Optional[str],
    entity_keys: List[EntityKeyRef],
    policy_id: Optional[str] = None,
    connection=None,
) -> Tuple[FollowupEntity, bool, List[str]]:
    """Resolve explicit entity_id or cross-channel aliases to a canonical FollowupEntity."""
    warnings: List[str] = []
    deduped = _dedupe_keys(entity_keys)
    existing_aliases = await _find_existing_aliases(tenant_id, deduped, connection=connection)
    created = False

    if entity_id:
        entity = await resolve_canonical_entity(tenant_id, entity_id, connection=connection)
        if entity is None:
            kwargs = dict(
                tenant_id=tenant_id,
                entity_id=entity_id,
                policy_id=policy_id or "default",
            )
            entity = (
                await FollowupEntity.create(using_db=connection, **kwargs)
                if connection is not None
                else await FollowupEntity.create(**kwargs)
            )
            created = True
        await _bind_keys_to_entity(
            tenant_id, entity.entity_id, deduped, existing_aliases, warnings, connection=connection
        )
    else:
        matched_entities: Dict[str, FollowupEntity] = {}
        for key in deduped:
            alias = existing_aliases.get((key.key_type, key.key_value))
            if alias is None:
                continue
            canon = await resolve_canonical_entity(tenant_id, alias.entity_id, connection=connection)
            if canon is not None:
                matched_entities[canon.entity_id] = canon

        if matched_entities:
            sorted_matches = sorted(matched_entities.values(), key=lambda e: e.id)
            entity = sorted_matches[0]
            if len(sorted_matches) > 1:
                warnings.append("multiple_entities_matched")
            await _bind_keys_to_entity(
                tenant_id, entity.entity_id, deduped, existing_aliases, warnings, connection=connection
            )
        else:
            new_entity_id = f"ent_{uuid.uuid4().hex[:16]}"
            kwargs = dict(
                tenant_id=tenant_id,
                entity_id=new_entity_id,
                policy_id=policy_id or "default",
            )
            entity = (
                await FollowupEntity.create(using_db=connection, **kwargs)
                if connection is not None
                else await FollowupEntity.create(**kwargs)
            )
            created = True
            await _bind_keys_to_entity(
                tenant_id, entity.entity_id, deduped, existing_aliases, warnings, connection=connection
            )

    if policy_id and entity.policy_id == "default" and policy_id != "default":
        entity.policy_id = policy_id
        if connection is not None:
            await entity.save(using_db=connection, update_fields=["policy_id", "update_time"])
        else:
            await entity.save(update_fields=["policy_id", "update_time"])

    return entity, created, warnings


async def merge_entities(
    tenant_id: str,
    source_entity_id: str,
    target_entity_id: str,
    connection=None,
) -> Tuple[FollowupEntity, int]:
    """Merge source entity aliases and immutable L1 events into canonical target entity."""
    target = await resolve_canonical_entity(tenant_id, target_entity_id, connection=connection)
    if target is None:
        kwargs = dict(tenant_id=tenant_id, entity_id=target_entity_id)
        target = (
            await FollowupEntity.create(using_db=connection, **kwargs)
            if connection is not None
            else await FollowupEntity.create(**kwargs)
        )

    source = await resolve_canonical_entity(tenant_id, source_entity_id, connection=connection)
    if source is None or source.entity_id == target.entity_id:
        return target, 0

    await _db(
        EntityAlias.filter(tenant_id=tenant_id, entity_id=source.entity_id),
        connection,
    ).update(entity_id=target.entity_id)

    migrated_events = await _db(
        EntityEventLog.filter(tenant_id=tenant_id, entity_id=source.entity_id),
        connection,
    ).update(entity_id=target.entity_id)

    source.status = "merged"
    source.merged_into_entity_id = target.entity_id
    if connection is not None:
        await source.save(using_db=connection, update_fields=["status", "merged_into_entity_id", "update_time"])
    else:
        await source.save(update_fields=["status", "merged_into_entity_id", "update_time"])

    target.inbound_msg_seq = max(target.inbound_msg_seq, source.inbound_msg_seq)
    target.state_version += 1
    if connection is not None:
        await target.save(using_db=connection, update_fields=["inbound_msg_seq", "state_version", "update_time"])
    else:
        await target.save(update_fields=["inbound_msg_seq", "state_version", "update_time"])

    logger.info(
        "[RSI-Identity] Merged source=%s -> target=%s (tenant=%s, migrated_events=%s)",
        source.entity_id,
        target.entity_id,
        tenant_id,
        migrated_events,
    )
    return target, int(migrated_events)
