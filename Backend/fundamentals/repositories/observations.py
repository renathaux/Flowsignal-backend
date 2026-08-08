from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import func

from db import SessionLocal
from models import EconomicEvent, EconomicEventObservation, EconomicProviderFetch


TRUSTED_PROVIDERS = {"jblanked", "jblanked_live", "jblanked_cache", "fmp", "finnhub"}


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
                EconomicEventObservation.actual.isnot(None),
                EconomicEventObservation.forecast.isnot(None),
            )
            .order_by(
                EconomicEvent.release_time.desc(),
                EconomicEventObservation.fetched_at.desc(),
            )
            .all()
        )
        latest = {}
        for event, observation in rows:
            latest.setdefault(event.event_id, _serialize(event, observation))
        return list(latest.values())


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
                EconomicEventObservation.actual.isnot(None),
                EconomicEventObservation.forecast.isnot(None),
            )
        )
        if before is not None:
            query = query.filter(EconomicEvent.release_time < before)
        rows = query.order_by(
            EconomicEvent.release_time.desc(),
            EconomicEventObservation.fetched_at.desc(),
        ).all()
        latest = {}
        for event, observation in rows:
            latest.setdefault(event.event_id, _serialize(event, observation))
        return list(latest.values())


def next_high_impact_event(currencies, *, now=None, session_factory=None):
    factory = session_factory or SessionLocal
    current = now or datetime.now(timezone.utc)
    with factory() as session:
        row = (
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
            .first()
        )
        return _serialize(*row) if row else None


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
