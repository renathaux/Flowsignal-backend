import re


INDICATOR_RULES = (
    ("average_hourly_earnings", ("average hourly earnings", "wage growth", "earnings"), "employment", True),
    ("core_cpi", ("core cpi", "core consumer price"), "inflation", True),
    ("cpi", ("cpi", "consumer price"), "inflation", True),
    ("core_hicp", ("core hicp", "core harmonised", "core harmonized"), "inflation", True),
    ("hicp", ("hicp", "harmonised index", "harmonized index"), "inflation", True),
    ("core_pce", ("core pce",), "inflation", True),
    ("pce", ("pce", "personal consumption expenditure"), "inflation", True),
    ("nonfarm_payrolls", ("nonfarm payroll", "non-farm payroll", "nfp"), "employment", True),
    ("unemployment_rate", ("unemployment rate",), "employment", False),
    ("jobless_claims", ("jobless claims", "unemployment claims"), "employment", False),
    ("retail_sales", ("retail sales",), "growth", True),
    ("industrial_production", ("industrial production",), "growth", True),
    ("services_pmi", ("services pmi",), "growth", True),
    ("manufacturing_pmi", ("manufacturing pmi",), "growth", True),
    ("gdp", ("gdp", "gross domestic product"), "growth", True),
    ("pmi", ("pmi", "purchasing managers"), "growth", True),
    ("interest_rate", (
        "interest rate", "rate decision", "federal funds rate", "deposit facility rate",
        "main refinancing rate", "ecb policy rate", "fed funds target"
    ), "policy", True),
)


def normalize_indicator(name):
    title = re.sub(r"\s+", " ", str(name or "").strip().lower())
    for indicator, patterns, _category, _higher_bullish in INDICATOR_RULES:
        if any(pattern in title for pattern in patterns):
            return indicator
    slug = re.sub(r"[^a-z0-9]+", "_", title).strip("_")
    return slug or "unknown"


def indicator_metadata(indicator_or_name):
    normalized = normalize_indicator(indicator_or_name)
    for indicator, _patterns, category, higher_bullish in INDICATOR_RULES:
        if indicator == normalized:
            return {
                "indicator": indicator,
                "category": category,
                "higher_is_currency_bullish": higher_bullish,
                "recognized": True,
            }
    return {
        "indicator": normalized,
        "category": "other",
        "higher_is_currency_bullish": None,
        "recognized": False,
    }
