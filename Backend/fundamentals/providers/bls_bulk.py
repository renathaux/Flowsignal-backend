"""Read-only adapter for official BLS bulk time-series distributions."""
from __future__ import annotations

import csv
import re
from datetime import date, datetime, timedelta
from io import StringIO
from zoneinfo import ZoneInfo

import requests

from fundamentals.providers.official_common import (
    OfficialProviderError, USER_AGENT, official_event,
)


PROVIDER = "bls"
DATASET = "official_bulk_time_series"
BASE_URL = "https://download.bls.gov/pub/time.series"
ICS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
EASTERN = ZoneInfo("America/New_York")

SERIES = {
    "cpi": "CUSR0000SA0",
    "core_cpi": "CUSR0000SA0L1E",
    "ppi": "WPSFD4",
    "nonfarm_payrolls": "CES0000000001",
    "unemployment_rate": "LNS14000000",
    "average_hourly_earnings": "CES0500000003",
}

# Use the smallest official file that contains each requested series. BLS does
# not publish a smaller current-only LN file, so that file is streamed and
# terminated as soon as the requested sorted series block has been read.
FILES = {
    "cu": {
        "url": f"{BASE_URL}/cu/cu.data.1.AllItems",
        "series": {SERIES["cpi"], SERIES["core_cpi"]},
    },
    "wp": {
        "url": f"{BASE_URL}/wp/wp.data.22.FD-ID",
        "series": {SERIES["ppi"]},
    },
    "ce_nfp": {
        "url": f"{BASE_URL}/ce/ce.data.00a.TotalNonfarm.Employment",
        "series": {SERIES["nonfarm_payrolls"]},
    },
    "ce_ahe": {
        "url": f"{BASE_URL}/ce/ce.data.05b.TotalPrivate.AllEmployeeHoursAndEarnings",
        "series": {SERIES["average_hourly_earnings"]},
    },
    "ln": {
        "url": f"{BASE_URL}/ln/ln.data.1.AllData",
        "series": {SERIES["unemployment_rate"]},
        "stop_after_series_block": True,
    },
}

RELEASE_TITLES = {
    "cpi": "consumer price index",
    "core_cpi": "consumer price index",
    "ppi": "producer price index",
    "nonfarm_payrolls": "employment situation",
    "unemployment_rate": "employment situation",
    "average_hourly_earnings": "employment situation",
}


def _month_key(value):
    match = re.fullmatch(r"(20\d{2})-(0[1-9]|1[0-2])", str(value))
    if not match:
        raise ValueError("reference period must use YYYY-MM")
    return int(match.group(1)), int(match.group(2))


def _shift_month(period, offset):
    year, month = period
    shifted = date(year, month, 1)
    if offset < 0:
        for _ in range(-offset):
            shifted = shifted - timedelta(days=1)
            shifted = shifted.replace(day=1)
    else:
        for _ in range(offset):
            shifted = (shifted.replace(day=28) + timedelta(days=4)).replace(day=1)
    return shifted.year, shifted.month


def _row_status(codes):
    values = {item.strip().upper() for item in str(codes or "").split(",") if item.strip()}
    if "P" in values:
        return "PRELIMINARY"
    if "C" in values:
        return "CORRECTED"
    return "RELEASED"


def _decode_lines(response):
    for raw in response.iter_lines():
        if raw is None:
            continue
        if isinstance(raw, bytes):
            yield raw.decode("utf-8-sig", errors="replace")
        else:
            yield str(raw).lstrip("\ufeff")


def _parse_bulk_response(response, required_series, *, stop_after_series_block=False):
    """Parse only requested series while streaming the official TSV file."""
    lines = _decode_lines(response)
    try:
        header = next(lines)
    except StopIteration:
        return {}
    fields = [item.strip() for item in next(csv.reader([header], delimiter="\t"))]
    required = set(required_series)
    output = {series_id: {} for series_id in required}
    seen_requested = False
    completed = set()
    active = None
    for line in lines:
        if not line.strip():
            continue
        values = next(csv.reader([line], delimiter="\t"))
        row = {fields[index]: values[index].strip() if index < len(values) else "" for index in range(len(fields))}
        series_id = row.get("series_id", "")
        if active in required and series_id != active:
            completed.add(active)
        active = series_id
        if stop_after_series_block and seen_requested and required <= completed:
            break
        if series_id not in required:
            continue
        seen_requested = True
        period = row.get("period", "")
        if not re.fullmatch(r"M(0[1-9]|1[0-2])", period):
            continue
        try:
            key = (int(row["year"]), int(period[1:]))
            value = float(row["value"].replace(",", ""))
        except (KeyError, TypeError, ValueError):
            continue
        output[series_id][key] = {
            "value": value,
            "footnote_codes": row.get("footnote_codes", ""),
            "status": _row_status(row.get("footnote_codes")),
        }
    return output


def _fetch_bulk_files(*, timeout=90, request_get=None):
    getter = request_get or requests.get
    merged, reports = {}, []
    for name, spec in FILES.items():
        try:
            response = getter(
                spec["url"], stream=True, timeout=timeout,
                headers={"User-Agent": USER_AGENT, "Accept": "text/plain"},
            )
        except requests.RequestException as exc:
            raise OfficialProviderError(
                f"BLS bulk file {name} failed: {type(exc).__name__}",
                request_count=len(reports) + 1,
            ) from exc
        status = int(getattr(response, "status_code", 0) or 0)
        if status != 200:
            raise OfficialProviderError(
                f"BLS bulk file {name} failed (HTTP {status})",
                request_count=len(reports) + 1,
                details={"failed_file": name, "url": spec["url"]},
            )
        parsed = _parse_bulk_response(
            response, spec["series"],
            stop_after_series_block=bool(spec.get("stop_after_series_block")),
        )
        merged.update(parsed)
        reports.append({
            "file": name,
            "url": spec["url"],
            "http_status": status,
            "required_series": sorted(spec["series"]),
            "found_series": sorted(series for series, rows in parsed.items() if rows),
        })
    return merged, reports


def _unfold_ics(text):
    output = []
    for line in str(text or "").replace("\r\n", "\n").split("\n"):
        if line.startswith((" ", "\t")) and output:
            output[-1] += line[1:]
        else:
            output.append(line)
    return output


def _parse_ics(text):
    events, current = [], None
    for line in _unfold_ics(text):
        if line == "BEGIN:VEVENT":
            current = {}
        elif line == "END:VEVENT" and current is not None:
            raw_time = current.get("DTSTART")
            if raw_time:
                try:
                    if raw_time.endswith("Z"):
                        timestamp = datetime.strptime(raw_time, "%Y%m%dT%H%M%SZ").replace(tzinfo=ZoneInfo("UTC"))
                    else:
                        timestamp = datetime.strptime(raw_time, "%Y%m%dT%H%M%S").replace(tzinfo=EASTERN)
                    current["release_time"] = timestamp
                    events.append(current)
                except ValueError:
                    pass
            current = None
        elif current is not None and ":" in line:
            key, value = line.split(":", 1)
            current[key.split(";", 1)[0]] = value.replace("\\,", ",").replace("\\n", " ")
    return events


def _fetch_calendar(*, timeout=30, request_get=None):
    getter = request_get or requests.get
    try:
        response = getter(
            ICS_URL, timeout=timeout,
            headers={"User-Agent": USER_AGENT, "Accept": "text/calendar"},
        )
    except requests.RequestException as exc:
        return [], {"ready": False, "error": f"{type(exc).__name__}", "http_status": None}
    status = int(getattr(response, "status_code", 0) or 0)
    if status != 200:
        return [], {"ready": False, "error": f"HTTP {status}", "http_status": status}
    events = _parse_ics(response.text)
    return events, {"ready": bool(events), "error": None if events else "no parseable events", "http_status": status}


def _pct(current, previous):
    if current is None or previous in (None, 0):
        return None
    return round((current / previous - 1) * 100, 3)


def _entry(values, series_id, period):
    return (values.get(series_id) or {}).get(period)


def _combine_status(*rows):
    statuses = {row.get("status") for row in rows if row}
    if "PRELIMINARY" in statuses:
        return "PRELIMINARY"
    if "CORRECTED" in statuses:
        return "CORRECTED"
    return "RELEASED"


def _calculate_observation(indicator, values, period):
    series_id = SERIES[indicator]
    current = _entry(values, series_id, period)
    prior_period = _shift_month(period, -1)
    prior = _entry(values, series_id, prior_period)
    year_ago = _entry(values, series_id, (period[0] - 1, period[1]))
    prior_year_ago = _entry(values, series_id, (prior_period[0] - 1, prior_period[1]))
    before_prior = _entry(values, series_id, _shift_month(period, -2))
    if not current:
        return None
    if indicator in {"cpi", "core_cpi", "ppi"}:
        actual = _pct(current["value"], year_ago["value"] if year_ago else None)
        previous = _pct(prior["value"], prior_year_ago["value"] if prior_year_ago else None) if prior else None
        rows = (current, year_ago)
    elif indicator == "nonfarm_payrolls":
        actual = round(current["value"] - prior["value"], 1) if prior else None
        previous = round(prior["value"] - before_prior["value"], 1) if prior and before_prior else None
        rows = (current, prior)
    elif indicator == "average_hourly_earnings":
        actual = _pct(current["value"], prior["value"] if prior else None)
        previous = _pct(prior["value"], before_prior["value"] if before_prior else None) if prior else None
        rows = (current, prior)
    else:
        actual = current["value"]
        previous = prior["value"] if prior else None
        rows = (current,)
    if actual is None:
        return None
    suffix = "_y_y" if indicator in {"cpi", "core_cpi", "ppi"} else ("_m_m" if indicator == "average_hourly_earnings" else "")
    return {
        "indicator": f"{indicator}{suffix}",
        "series_id": series_id,
        "reference_period": f"{period[0]}-M{period[1]:02d}",
        "provider_event_id": f"bls:{series_id}:{period[0]}:M{period[1]:02d}",
        "actual": actual,
        "previous": previous,
        "status": _combine_status(*rows),
        "footnote_codes": sorted({
            code.strip() for row in rows if row
            for code in str(row.get("footnote_codes") or "").split(",") if code.strip()
        }),
        "source_value": current["value"],
        "release_time": None,
    }


def _reference_from_calendar(item):
    text = " ".join((item.get("SUMMARY", ""), item.get("DESCRIPTION", "")))
    match = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})\b",
        text, flags=re.I,
    )
    if not match:
        return None
    month = list(("", "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")).index(match.group(1).title())
    return int(match.group(2)), month


def fetch_range(
    date_from, date_to, currencies=("USD",), *, reference_period=None,
    timeout=90, request_get=None,
):
    """Fetch official values and attach timestamps only when the ICS proves them."""
    start, end = date.fromisoformat(str(date_from)), date.fromisoformat(str(date_to))
    if "USD" not in {str(item).upper() for item in currencies}:
        return {"provider": PROVIDER, "dataset": DATASET, "request_count": 0, "normalized_events": []}
    calendar_events, calendar_report = _fetch_calendar(timeout=min(timeout, 30), request_get=request_get)
    if reference_period:
        periods = {_month_key(reference_period)}
    else:
        periods = {
            period for item in calendar_events
            if start <= item["release_time"].astimezone(EASTERN).date() <= end
            for period in [_reference_from_calendar(item)] if period
        }
    if not periods:
        return {
            "provider": PROVIDER, "dataset": DATASET,
            "request_count": 1, "normalized_events": [], "period_observations": [],
            "bulk_files": [], "calendar": calendar_report,
            "values_ready": False, "timestamps_ready": False,
            "warning": "no reference period could be proven; pass an explicit reference period for values-only validation",
        }
    values, file_reports = _fetch_bulk_files(timeout=timeout, request_get=request_get)
    observations = []
    for period in sorted(periods):
        for indicator in SERIES:
            observation = _calculate_observation(indicator, values, period)
            if observation:
                observations.append(observation)

    normalized_events = []
    for observation in observations:
        indicator = observation["indicator"].removesuffix("_y_y").removesuffix("_m_m")
        period = _month_key(observation["reference_period"].replace("-M", "-"))
        matches = [
            item for item in calendar_events
            if RELEASE_TITLES[indicator] in " ".join((item.get("SUMMARY", ""), item.get("DESCRIPTION", ""))).lower()
            and _reference_from_calendar(item) == period
            and start <= item["release_time"].astimezone(EASTERN).date() <= end
        ]
        if len(matches) != 1:
            continue
        timestamp = matches[0]["release_time"]
        observation["release_time"] = timestamp
        normalized_events.append(official_event(
            provider=PROVIDER, dataset=DATASET,
            provider_event_id=observation["provider_event_id"],
            event_name={
                "cpi": "US CPI y/y", "core_cpi": "US Core CPI y/y", "ppi": "US PPI y/y",
                "nonfarm_payrolls": "US Non-Farm Payrolls", "unemployment_rate": "US Unemployment Rate",
                "average_hourly_earnings": "US Average Hourly Earnings m/m",
            }[indicator],
            indicator=observation["indicator"], currency="USD", country="United States",
            release_time=timestamp, actual=observation["actual"], previous=observation["previous"],
            data_status=observation["status"],
            raw={
                "series_id": observation["series_id"],
                "reference_period": observation["reference_period"],
                "footnote_codes": observation["footnote_codes"],
                "source_value": observation["source_value"],
            },
        ))
    found = {item["series_id"] for item in observations}
    return {
        "provider": PROVIDER, "dataset": DATASET,
        "provider_identity": "US Bureau of Labor Statistics official bulk files",
        "source": "BLS official bulk time-series files and official BLS ICS calendar",
        "request_count": len(file_reports) + 1,
        "estimated_credit_usage": 0,
        "bulk_files": file_reports,
        "calendar": calendar_report,
        "period_observations": observations,
        "normalized_events": normalized_events,
        "missing_series": sorted(set(SERIES.values()) - found),
        "values_ready": set(SERIES.values()) <= found,
        "timestamps_ready": len(normalized_events) == len(observations) and bool(observations),
        "revision_method": "preserve BLS footnote status and append changed repeated snapshots",
        "release_time_status": "official BLS ICS only; never inferred from reference period",
    }
