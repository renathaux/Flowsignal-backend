from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from db import SessionLocal
from fundamentals.normalization.events import normalize_economic_event
from models import EconomicEvent, EconomicEventObservation, EconomicProviderFetch


def _json_safe(value):
    return json.loads(json.dumps(value, default=str))


def _observation_hash(event):
    payload = {
        "event_id": event.event_id,
        "provider": event.provider,
        "actual": event.actual,
        "forecast": event.forecast,
        "previous": event.previous,
        "revised_previous": event.revised_previous,
        "provider_timestamp": (
            event.provider_timestamp.isoformat() if event.provider_timestamp else None
        ),
        "data_status": event.data_status,
    }
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def prepare_calendar_batch(provider, raw_events, normalized_events=None, *, completed_at=None):
    completed = completed_at or datetime.now(timezone.utc)
    canonical = []
    for item in normalized_events if normalized_events is not None else raw_events:
        event = normalize_economic_event(item, provider=provider, fetched_at=completed)
        if event is not None:
            canonical.append(event)
    return canonical


def preview_calendar_batch(session, provider, raw_events, normalized_events=None, *, completed_at=None):
    """Read-only insert preview used by --dry-run."""
    canonical = prepare_calendar_batch(
        provider, raw_events, normalized_events, completed_at=completed_at
    )
    hashes = [_observation_hash(event) for event in canonical]
    existing = set()
    if hashes:
        existing = {
            value for (value,) in session.query(EconomicEventObservation.observation_hash)
            .filter(EconomicEventObservation.observation_hash.in_(hashes)).all()
        }
    duplicates_in_payload = len(hashes) - len(set(hashes))
    duplicates_existing = sum(value in existing for value in set(hashes))
    duplicates = duplicates_in_payload + duplicates_existing
    return {
        "provider": str(provider).lower(),
        "events": len(canonical),
        "observations_would_add": max(0, len(hashes) - duplicates),
        "duplicates_skipped": duplicates,
        "canonical": canonical,
    }


def persist_calendar_batch_in_session(
    session,
    provider,
    raw_events,
    normalized_events=None,
    *,
    status="SUCCESS",
    error=None,
    started_at=None,
    completed_at=None,
):
    """Stage one provider batch in the caller's current transaction."""
    completed = completed_at or datetime.now(timezone.utc)
    started = started_at or completed
    canonical = prepare_calendar_batch(
        provider, raw_events, normalized_events, completed_at=completed
    )
    fetch = EconomicProviderFetch(
        provider=str(provider).lower(),
        started_at=started,
        completed_at=completed,
        status=str(status).upper(),
        raw_event_count=len(raw_events or []),
        normalized_event_count=len(canonical),
        error=str(error) if error else None,
    )
    session.add(fetch)
    observations_added = 0
    duplicates_skipped = 0
    for event in canonical:
        row = session.query(EconomicEvent).filter_by(event_id=event.event_id).one_or_none()
        if row is None:
            row = EconomicEvent(
                event_id=event.event_id,
                event_name=event.event_name,
                indicator=event.indicator,
                country=event.country,
                currency=event.currency,
                impact=event.impact,
                release_time=event.release_time,
                provider=event.provider,
                provider_event_id=event.provider_event_id,
                data_status=event.data_status,
                first_seen_at=event.fetched_at,
                last_seen_at=event.fetched_at,
            )
            session.add(row)
            session.flush()
        else:
            row.event_name = event.event_name
            row.indicator = event.indicator
            row.country = event.country
            row.currency = event.currency
            row.impact = event.impact
            row.release_time = event.release_time
            row.provider = event.provider
            row.provider_event_id = event.provider_event_id
            row.data_status = event.data_status
            row.last_seen_at = event.fetched_at

        observation_hash = _observation_hash(event)
        exists = session.query(EconomicEventObservation.id).filter_by(
            observation_hash=observation_hash
        ).first()
        if exists:
            duplicates_skipped += 1
            continue
        session.add(EconomicEventObservation(
            observation_hash=observation_hash,
            economic_event_id=row.id,
            actual=None if event.actual is None else str(event.actual),
            forecast=None if event.forecast is None else str(event.forecast),
            previous=None if event.previous is None else str(event.previous),
            revised_previous=(
                None if event.revised_previous is None else str(event.revised_previous)
            ),
            provider=event.provider,
            provider_timestamp=event.provider_timestamp,
            fetched_at=event.fetched_at,
            data_status=event.data_status,
            raw_payload=_json_safe(event.raw),
        ))
        observations_added += 1
    session.flush()
    return {
        "provider": str(provider).lower(),
        "events": len(canonical),
        "observations_added": observations_added,
        "duplicates_skipped": duplicates_skipped,
        "fetch_id": fetch.id,
    }


def persist_calendar_batch(
    provider,
    raw_events,
    normalized_events=None,
    *,
    status="SUCCESS",
    error=None,
    started_at=None,
    completed_at=None,
    session_factory=None,
):
    """Persist one provider result without overwriting observation history."""
    factory = session_factory or SessionLocal
    with factory() as session:
        try:
            result = persist_calendar_batch_in_session(
                session,
                provider,
                raw_events,
                normalized_events,
                status=status,
                error=error,
                started_at=started_at,
                completed_at=completed_at,
            )
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise


def record_failed_fetch(provider, error, *, started_at=None, session_factory=None):
    return persist_calendar_batch(
        provider,
        [],
        [],
        status="FAILED",
        error=error,
        started_at=started_at,
        session_factory=session_factory,
    )
