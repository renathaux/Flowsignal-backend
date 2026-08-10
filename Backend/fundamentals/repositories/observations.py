from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import func

from db import SessionLocal
from fundamentals.authority import choose_field
from models import EconomicEvent, EconomicEventObservation, EconomicProviderFetch


TRUSTED_PROVIDERS = {
    "jblanked", "jblanked_live", "jblanked_cache", "jblanked_mql5",
    "jblanked_forex_factory", "jblanked_fxstreet", "fmp", "finnhub",
    "bls", "bea", "eurostat", "federal_reserve", "ecb",
}


def _serialize(event, observation):
    return {
        "event_id": event.event_id,
        "event_name": event.event_name,
        "indicator": event.indicator,
        "country": event.country,
        "currency": event.currency,
        "impact": event.impact,
        "release_time": event.release_time,
        "provider": observation.provider or event.provider,
        "provider_event_id": event.provider_event_id,
        "actual": observation.actual,
        "forecast": observation.forecast,
        "previous": observation.previous,
        "revised_previous": observation.revised_previous,
        "provider_timestamp": observation.provider_timestamp,
        "fetched_at": observation.fetched_at,
        "data_status": observation.data_status,
    }


def _serialize_authoritative(event, observations):
    """Merge fields by authority while preserving source attribution per field."""
    result = {
        "event_id": event.event_id,
        "event_name": event.event_name,
        "indicator": event.indicator,
        "country": event.country,
        "currency": event.currency,
        "impact": event.impact,
        "release_time": event.release_time,
        "provider_event_id": event.provider_event_id,
        "data_status": event.data_status,
        "field_sources": {},
    }
    for field in ("actual", "forecast", "previous", "revised_previous"):
        candidates = [{
            "value": getattr(observation, field),
            "provider": observation.provider,
            "sequence": observation.id or 0,
            "observation": observation,
        } for observation in observations]
        selected = choose_field(field, candidates, event.indicator, event.currency)
        result[field] = selected["value"] if selected else None
        result["field_sources"][field] = selected["provider"] if selected else None
    actual_source = result["field_sources"].get("actual")
    forecast_source = result["field_sources"].get("forecast")
    selected_observation = next((
        item for item in observations if item.provider == actual_source
    ), observations[0] if observations else None)
    result.update({
        "provider": actual_source or forecast_source or event.provider,
        "provider_timestamp": selected_observation.provider_timestamp if selected_observation else None,
        "fetched_at": max((item.fetched_at for item in observations), default=event.last_seen_at),
    })
    return result


def _group_authoritative(rows):
    grouped = {}
    for event, observation in rows:
        grouped.setdefault(event.id, [event, []])[1].append(observation)
    return [_serialize_authoritative(event, observations) for event, observations in grouped.values()]


def latest_released_observations(
    currencies,
    *,
    now=None,
    lookback_days=180,
    session_factory=None,
):
    factory = session_factory or SessionLocal
    current = now or datetime.now(timezone.utc)
    cutoff = current - timedelta(days=lookback_days)
    with factory() as session:
        rows = (
            session.query(EconomicEvent, EconomicEventObservation)
            .join(
                EconomicEventObservation,
                EconomicEventObservation.economic_event_id == EconomicEvent.id,
            )
            .filter(
                EconomicEvent.currency.in_([str(value).upper() for value in currencies]),
                EconomicEvent.release_time <= current,
                EconomicEvent.release_time >= cutoff,
            )
            .order_by(
                EconomicEvent.release_time.desc(),
                EconomicEventObservation.fetched_at.desc(),
            )
            .all()
        )
        return [item for item in _group_authoritative(rows)
                if item.get("actual") is not None and item.get("forecast") is not None]


def historical_surprises(indicator, currency, *, before=None, session_factory=None):
    factory = session_factory or SessionLocal
    with factory() as session:
        query = (
            session.query(EconomicEvent, EconomicEventObservation)
            .join(
                EconomicEventObservation,
                EconomicEventObservation.economic_event_id == EconomicEvent.id,
            )
            .filter(
                EconomicEvent.indicator == indicator,
                EconomicEvent.currency == currency,
            )
        )
        if before is not None:
            query = query.filter(EconomicEvent.release_time < before)
        rows = query.order_by(
            EconomicEvent.release_time.desc(),
            EconomicEventObservation.fetched_at.desc(),
        ).all()
        return [item for item in _group_authoritative(rows)
                if item.get("actual") is not None and item.get("forecast") is not None]


def next_high_impact_event(currencies, *, now=None, session_factory=None):
    factory = session_factory or SessionLocal
    current = now or datetime.now(timezone.utc)
    with factory() as session:
        rows = (
            session.query(EconomicEvent, EconomicEventObservation)
            .join(
                EconomicEventObservation,
                EconomicEventObservation.economic_event_id == EconomicEvent.id,
            )
            .filter(
                EconomicEvent.currency.in_([str(value).upper() for value in currencies]),
                EconomicEvent.release_time >= current,
                EconomicEvent.impact == "HIGH",
                EconomicEvent.data_status != "UNRELIABLE_STATIC",
                EconomicEvent.provider.in_(sorted(TRUSTED_PROVIDERS)),
            )
            .order_by(
                EconomicEvent.release_time.asc(),
                EconomicEventObservation.fetched_at.desc(),
            )
            .all()
        )
        grouped = _group_authoritative(rows)
        return grouped[0] if grouped else None


def provider_health(*, now=None, session_factory=None):
    factory = session_factory or SessionLocal
    current = now or datetime.now(timezone.utc)
    with factory() as session:
        fetches = (
            session.query(EconomicProviderFetch)
            .order_by(EconomicProviderFetch.completed_at.desc())
            .limit(100)
            .all()
        )
        latest_by_provider = {}
        last_attempt = fetches[0].completed_at if fetches else None
        last_success = None
        failures = 0
        authoritative = None
        for row in fetches:
            latest_by_provider.setdefault(row.provider, row)
            if row.status == "FAILED":
                failures += 1
            if row.status == "SUCCESS" and row.provider in TRUSTED_PROVIDERS:
                if last_success is None:
                    last_success = row.completed_at
                    authoritative = row.provider
        observation_count = session.query(func.count(EconomicEventObservation.id)).scalar() or 0
        event_count = session.query(func.count(EconomicEvent.id)).scalar() or 0
        if last_success and last_success.tzinfo is None:
            last_success = last_success.replace(tzinfo=timezone.utc)
        if last_attempt and last_attempt.tzinfo is None:
            last_attempt = last_attempt.replace(tzinfo=timezone.utc)
        stale_age = (
            max(0.0, (current - last_success).total_seconds())
            if last_success
            else None
        )
        return {
            "last_successful_provider_fetch": last_success,
            "last_provider_fetch_attempt": last_attempt,
            "authoritative_provider": authoritative,
            "stale_age_seconds": stale_age,
            "observation_count": int(observation_count),
            "event_count": int(event_count),
            "provider_failures_recent": failures,
            "providers": {
                provider: {
                    "status": row.status,
                    "completed_at": row.completed_at,
                    "error": row.error,
                    "event_count": row.normalized_event_count,
                }
                for provider, row in latest_by_provider.items()
            },
        }
