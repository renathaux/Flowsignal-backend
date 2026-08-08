from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
)
from db import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)


class RuntimeSetting(Base):
    __tablename__ = "runtime_settings"

    setting_name = Column(String(100), primary_key=True)
    setting_value = Column(String(100), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)
    updated_by = Column(String(255), nullable=False)


class NewsTradingModeAudit(Base):
    __tablename__ = "news_trading_mode_audit"

    id = Column(Integer, primary_key=True, autoincrement=True)
    previous_mode = Column(String(32), nullable=False)
    new_mode = Column(String(32), nullable=False)
    user_id = Column(String(255), nullable=False)
    active_broker_account = Column(String(100), nullable=True)
    broker_environment = Column(String(32), nullable=False)
    timestamp = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    request_source = Column(String(100), nullable=False)
    success = Column(Boolean, nullable=False)
    failure_reason = Column(Text, nullable=True)


class AutoTradeStateAudit(Base):
    __tablename__ = "auto_trade_state_audit"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trading_mode = Column(String(16), nullable=False)
    previous_enabled = Column(Boolean, nullable=False)
    new_enabled = Column(Boolean, nullable=False)
    updated_by = Column(String(255), nullable=False)
    active_broker_account = Column(String(100), nullable=True)
    broker_environment = Column(String(32), nullable=True)
    timestamp = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    request_source = Column(String(100), nullable=False)
    reason = Column(Text, nullable=True)


class CTraderOAuthToken(Base):
    __tablename__ = "ctrader_oauth_tokens"

    provider = Column(String(32), primary_key=True)
    encrypted_access_token = Column(Text, nullable=False)
    encrypted_refresh_token = Column(Text, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False)
    updated_by = Column(String(255), nullable=False)


class StrategyCycleDiagnostic(Base):
    """Durable, read-only audit snapshot for one strategy evaluation cycle."""

    __tablename__ = "strategy_cycle_diagnostics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cycle_id = Column(String(64), nullable=False, unique=True, index=True)
    session_id = Column(String(128), nullable=False, index=True)
    symbol = Column(String(16), nullable=False, index=True)
    evaluation_timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    latest_closed_m15_timestamp = Column(DateTime(timezone=True), nullable=True)
    latest_closed_m5_timestamp = Column(DateTime(timezone=True), nullable=True)
    decision = Column(String(32), nullable=False, index=True)
    block_reason = Column(String(255), nullable=True, index=True)
    progress_percent = Column(Integer, nullable=False, default=0)
    snapshot_json = Column(JSON, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index(
            "ix_strategy_cycle_symbol_evaluated",
            "symbol",
            "evaluation_timestamp",
        ),
        Index(
            "ix_strategy_cycle_decision_evaluated",
            "decision",
            "evaluation_timestamp",
        ),
    )


class EconomicEvent(Base):
    __tablename__ = "economic_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(128), nullable=False, unique=True, index=True)
    event_name = Column(String(255), nullable=False)
    indicator = Column(String(128), nullable=False, index=True)
    country = Column(String(100), nullable=True)
    currency = Column(String(8), nullable=False, index=True)
    impact = Column(String(16), nullable=False, default="UNKNOWN")
    release_time = Column(DateTime(timezone=True), nullable=False, index=True)
    provider = Column(String(32), nullable=False, index=True)
    provider_event_id = Column(String(255), nullable=True)
    data_status = Column(String(32), nullable=False, default="SCHEDULED")
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_economic_event_currency_release", "currency", "release_time"),
        Index("ix_economic_event_indicator_release", "indicator", "release_time"),
    )


class EconomicEventObservation(Base):
    """Append-only provider observation preserving forecasts and revisions."""

    __tablename__ = "economic_event_observations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    observation_hash = Column(String(64), nullable=False, unique=True, index=True)
    economic_event_id = Column(
        Integer,
        ForeignKey("economic_events.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    actual = Column(String(100), nullable=True)
    forecast = Column(String(100), nullable=True)
    previous = Column(String(100), nullable=True)
    revised_previous = Column(String(100), nullable=True)
    provider = Column(String(32), nullable=False, index=True)
    provider_timestamp = Column(DateTime(timezone=True), nullable=True)
    fetched_at = Column(DateTime(timezone=True), nullable=False, index=True)
    data_status = Column(String(32), nullable=False)
    raw_payload = Column(JSON, nullable=True)


class EconomicProviderFetch(Base):
    __tablename__ = "economic_provider_fetches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    provider = Column(String(32), nullable=False, index=True)
    started_at = Column(DateTime(timezone=True), nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    status = Column(String(24), nullable=False, index=True)
    raw_event_count = Column(Integer, nullable=False, default=0)
    normalized_event_count = Column(Integer, nullable=False, default=0)
    error = Column(Text, nullable=True)


class FundamentalFactorInput(Base):
    __tablename__ = "fundamental_factor_inputs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    calculation_id = Column(String(64), nullable=False, index=True)
    currency = Column(String(8), nullable=False, index=True)
    factor = Column(String(32), nullable=False, index=True)
    score = Column(Float, nullable=True)
    status = Column(String(32), nullable=False)
    weight = Column(Float, nullable=False, default=0.0)
    evidence = Column(JSON, nullable=False, default=list)
    calculated_at = Column(DateTime(timezone=True), nullable=False, index=True)


class CurrencyStrengthSnapshot(Base):
    __tablename__ = "currency_strength_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(String(64), nullable=False, unique=True, index=True)
    calculation_id = Column(String(64), nullable=False, index=True)
    currency = Column(String(8), nullable=False, index=True)
    score = Column(Float, nullable=True)
    status = Column(String(32), nullable=False)
    coverage = Column(Float, nullable=False)
    confidence = Column(Float, nullable=False)
    factors = Column(JSON, nullable=False)
    evidence = Column(JSON, nullable=False)
    calculated_at = Column(DateTime(timezone=True), nullable=False, index=True)


class FundamentalInsightSnapshot(Base):
    __tablename__ = "fundamental_insight_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    insight_id = Column(String(64), nullable=False, unique=True, index=True)
    calculation_id = Column(String(64), nullable=False, index=True)
    symbol = Column(String(16), nullable=False, index=True)
    pair_score = Column(Float, nullable=True)
    direction = Column(String(16), nullable=False)
    confidence = Column(Float, nullable=False)
    status = Column(String(32), nullable=False)
    response_json = Column(JSON, nullable=False)
    generated_at = Column(DateTime(timezone=True), nullable=False, index=True)
