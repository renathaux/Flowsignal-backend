"""Managed-runtime connectivity probe for the official BLS bulk fallback."""
from __future__ import annotations

import argparse
import json

from fundamentals.providers import bls_bulk
from fundamentals.providers.official_common import OfficialProviderError


def run_probe(*, date_from, date_to, reference_period, fetcher=None):
    result = (fetcher or bls_bulk.fetch_range)(
        date_from, date_to, ("USD",), reference_period=reference_period,
    )
    observations = list(result.get("period_observations") or [])
    return {
        "status": "OK" if result.get("values_ready") else "INCOMPLETE",
        "read_only": True,
        "database_writes": 0,
        "trading_actions": 0,
        "provider": bls_bulk.PROVIDER,
        "dataset": bls_bulk.DATASET,
        "date_from": str(date_from), "date_to": str(date_to),
        "reference_period": reference_period,
        "request_count": int(result.get("request_count") or 0),
        "bulk_files": result.get("bulk_files") or [],
        "calendar": result.get("calendar") or {},
        "values_ready": bool(result.get("values_ready")),
        "timestamps_ready": bool(result.get("timestamps_ready")),
        "missing_series": result.get("missing_series") or [],
        "observations": observations,
        "normalized_event_count": len(result.get("normalized_events") or []),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Read-only BLS official bulk-file probe")
    parser.add_argument("--from", dest="date_from", required=True)
    parser.add_argument("--to", dest="date_to", required=True)
    parser.add_argument("--reference-period", required=True, help="YYYY-MM statistical reference period")
    args = parser.parse_args(argv)
    try:
        report = run_probe(
            date_from=args.date_from, date_to=args.date_to,
            reference_period=args.reference_period,
        )
    except (OfficialProviderError, ValueError) as exc:
        report = {
            "status": "UNAVAILABLE", "read_only": True,
            "database_writes": 0, "trading_actions": 0,
            "provider": bls_bulk.PROVIDER, "dataset": bls_bulk.DATASET,
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
