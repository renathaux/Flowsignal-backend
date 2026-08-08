"""Read-only BLS connectivity probe for isolated managed workers.

This module deliberately has no database, broker, strategy, execution, or
runtime-control imports. It is suitable as the command for a one-off Render
worker/cron validation before any persistent BLS ingestion is enabled.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date

from fundamentals.normalization.indicators import normalize_indicator
from fundamentals.providers import bls
from fundamentals.providers.official_common import OfficialProviderError


EXPECTED = {
    "cpi_y_y",
    "core_cpi_y_y",
    "ppi_y_y",
    "nonfarm_payrolls",
    "unemployment_rate",
    "average_hourly_earnings_m_m",
}


def run_probe(*, date_from, date_to, fetcher=None):
    """Fetch and validate BLS data without writing or importing persistence."""
    start = date.fromisoformat(str(date_from))
    end = date.fromisoformat(str(date_to))
    if start > end:
        raise ValueError("--from must be on or before --to")

    result = (fetcher or bls.fetch_range)(start, end, ("USD",))
    events = list(result.get("normalized_events") or [])
    normalized = [
        normalize_indicator(event.get("indicator") or event.get("event_name"))
        for event in events
    ]
    present = set(normalized)
    event_rows = []
    for event, indicator in zip(events, normalized):
        event_rows.append({
            "indicator": indicator,
            "series_id": (event.get("raw") or {}).get("series_id"),
            "provider_event_id": event.get("provider_event_id"),
            "release_time": str(event.get("release_time")),
            "actual": event.get("actual"),
            "previous": event.get("previous"),
            "timestamp_verified": event.get("release_time") is not None,
            "stable_id_verified": bool(event.get("provider_event_id")),
            "fabricated": False,
        })

    return {
        "status": "OK" if EXPECTED <= present else "INCOMPLETE",
        "read_only": True,
        "database_writes": 0,
        "trading_actions": 0,
        "provider": bls.PROVIDER,
        "dataset": bls.DATASET,
        "date_from": start.isoformat(),
        "date_to": end.isoformat(),
        "request_count": int(result.get("request_count") or 0),
        "series_ids": dict(bls.SERIES),
        "event_count": len(events),
        "actual_count": sum(event.get("actual") is not None for event in events),
        "timestamp_count": sum(event.get("release_time") is not None for event in events),
        "stable_id_count": sum(bool(event.get("provider_event_id")) for event in events),
        "normalized_indicators": dict(sorted(Counter(normalized).items())),
        "missing_indicators": sorted(EXPECTED - present),
        "events": event_rows,
    }


def build_parser():
    parser = argparse.ArgumentParser(description="Read-only isolated BLS worker probe")
    parser.add_argument("--from", dest="date_from", required=True)
    parser.add_argument("--to", dest="date_to", required=True)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        report = run_probe(date_from=args.date_from, date_to=args.date_to)
    except (OfficialProviderError, ValueError) as exc:
        report = {
            "status": "UNAVAILABLE",
            "read_only": True,
            "database_writes": 0,
            "trading_actions": 0,
            "provider": bls.PROVIDER,
            "dataset": bls.DATASET,
            "error": str(exc),
            "request_count": int(getattr(exc, "request_count", 0) or 0),
            "details": getattr(exc, "details", {}) or {},
        }
        print(json.dumps(report, indent=2, default=str))
        return 2
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["status"] == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
