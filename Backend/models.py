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


class StrategySettingAudit(Base):
    """Append-only history for backend-authoritative strategy setting changes."""

    __tablename__ = "strategy_setting_audit"

    id = Column(Integer, primary_key=True, autoincrement=True)
    setting_name = Column(String(100), nullable=False, index=True)
    previous_value = Column(String(100), nullable=False)
    new_value = Column(String(100), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, index=True)
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


class StrategyShadowRuntime(Base):
    """Restart-safe state for one non-executing strategy shadow."""

    __tablename__ = "strategy_shadow_runtime"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(16), nullable=False)
    strategy_version = Column(String(64), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, index=True)
    state_json = Column(JSON, nullable=False, default=dict)

    __table_args__ = (
        Index(
            "uq_strategy_shadow_runtime_symbol_version",
            "symbol",
            "strategy_version",
            unique=True,
        ),
    )


class StrategyShadowEvaluation(Base):
    """Durable V1-versus-V2 decision for one closed-candle opportunity."""

    __tablename__ = "strategy_shadow_evaluations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    evaluation_key = Column(String(64), nullable=False, unique=True, index=True)
    evaluated_at = Column(DateTime(timezone=True), nullable=False, index=True)
    symbol = Column(String(16), nullable=False, index=True)
    timeframe = Column(String(8), nullable=False, default="M5")
    strategy_version = Column(String(64), nullable=False, index=True)
    setup_fingerprint = Column(String(128), nullable=True, index=True)
    direction = Column(String(8), nullable=True)
    structure_type = Column(String(16), nullable=True)
    bos_level = Column(Float, nullable=True)
    bos_timestamp = Column(DateTime(timezone=True), nullable=True)
    bos_buffer = Column(Float, nullable=True)
    atr14 = Column(Float, nullable=True)
    ema_state = Column(String(32), nullable=True)
    consolidation_state = Column(String(32), nullable=True)
    m5_confirmation_timestamp = Column(DateTime(timezone=True), nullable=True)
    reference_price = Column(Float, nullable=True)
    extension_atr = Column(Float, nullable=True)
    v1_decision = Column(String(32), nullable=False, index=True)
    v1_reason = Column(String(255), nullable=True)
    v2_decision = Column(String(32), nullable=False, index=True)
    v2_reason = Column(String(255), nullable=True, index=True)
    hypothetical_entry = Column(Float, nullable=True)
    hypothetical_sl = Column(Float, nullable=True)
    hypothetical_tp1 = Column(Float, nullable=True)
    hypothetical_tp2 = Column(Float, nullable=True)
    hypothetical_rr = Column(Float, nullable=True)
    hypothetical_risk_percent = Column(Float, nullable=True)
    retest_timestamp = Column(DateTime(timezone=True), nullable=True)
    continuation_timestamp = Column(DateTime(timezone=True), nullable=True)
    setup_expiry = Column(DateTime(timezone=True), nullable=True)
    related_previous_trade_id = Column(Integer, nullable=True)
    post_sl_reset_state = Column(String(64), nullable=True)
    diagnostics_json = Column(JSON, nullable=False, default=dict)
    v1_order_id = Column(String(128), nullable=True)
    v1_position_id = Column(String(128), nullable=True)
    v1_outcome_json = Column(JSON, nullable=True)

    __table_args__ = (
        Index(
            "ix_shadow_eval_symbol_time",
            "symbol",
            "evaluated_at",
        ),
        Index(
            "ix_shadow_eval_comparison",
            "symbol",
            "v1_decision",
            "v2_decision",
        ),
    )


class StrategyShadowTrade(Base):
    """Hypothetical trade lifecycle. This table has no broker relationship."""

    __tablename__ = "strategy_shadow_trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    shadow_trade_id = Column(String(64), nullable=False, unique=True, index=True)
    symbol = Column(String(16), nullable=False, index=True)
    strategy_version = Column(String(64), nullable=False, index=True)
    setup_fingerprint = Column(String(128), nullable=False)
    direction = Column(String(8), nullable=False)
    entry_timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    entry = Column(Float, nullable=False)
    sl = Column(Float, nullable=False)
    tp1 = Column(Float, nullable=False)
    tp2 = Column(Float, nullable=False)
    protected_sl = Column(Float, nullable=False)
    risk_percent = Column(Float, nullable=True)
    rr = Column(Float, nullable=False)
    status = Column(String(32), nullable=False, default="OPEN", index=True)
    exit_timestamp = Column(DateTime(timezone=True), nullable=True)
    exit_price = Column(Float, nullable=True)
    r_result = Column(Float, nullable=True)
    mae_r = Column(Float, nullable=False, default=0.0)
    mfe_r = Column(Float, nullable=False, default=0.0)
    tp1_reached = Column(Boolean, nullable=False, default=False)
    tp2_reached = Column(Boolean, nullable=False, default=False)
    sl_reached = Column(Boolean, nullable=False, default=False)
    last_processed_m5 = Column(DateTime(timezone=True), nullable=True)
    related_previous_trade_id = Column(Integer, nullable=True)
    v1_evaluation_id = Column(Integer, nullable=True)
    v1_order_id = Column(String(128), nullable=True)
    v1_position_id = Column(String(128), nullable=True)
    v1_outcome_json = Column(JSON, nullable=True)
    diagnostics_json = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index(
            "uq_shadow_trade_strategy_setup",
            "strategy_version",
            "setup_fingerprint",
            unique=True,
        ),
        Index("ix_shadow_trade_symbol_status", "symbol", "status"),
    )


class ExecutionRiskAudit(Base):
    """Append-only observability for application-generated risk changes."""

    __tablename__ = "execution_risk_audits"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    symbol = Column(String(16), nullable=False, index=True)
    event_type = Column(String(48), nullable=False, index=True)
    source = Column(String(32), nullable=False)
    broker_position_id = Column(String(128), nullable=True, index=True)
    old_entry = Column(Float, nullable=True)
    new_entry = Column(Float, nullable=True)
    old_sl = Column(Float, nullable=True)
    new_sl = Column(Float, nullable=True)
    volume_units = Column(Float, nullable=True)
    approved_risk_amount = Column(Float, nullable=True)
    resulting_risk_amount = Column(Float, nullable=True)
    approved_risk_percent = Column(Float, nullable=True)
    resulting_risk_percent = Column(Float, nullable=True)
    status = Column(String(48), nullable=False, index=True)
    details_json = Column(JSON, nullable=False, default=dict)

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


class EconomicEventProviderLink(Base):
    """Durable link from a provider release to one canonical economic event."""

    __tablename__ = "economic_event_provider_links"

    id = Column(Integer, primary_key=True, autoincrement=True)
    economic_event_id = Column(
        Integer,
        ForeignKey("economic_events.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    provider = Column(String(32), nullable=False, index=True)
    provider_dataset = Column(String(64), nullable=False, default="default")
    provider_event_id = Column(String(255), nullable=True)
    provider_fingerprint = Column(String(64), nullable=False, unique=True, index=True)
    reported_event_name = Column(String(255), nullable=False)
    reported_indicator = Column(String(128), nullable=False)
    reported_release_time = Column(DateTime(timezone=True), nullable=False)
    reported_impact = Column(String(16), nullable=False, default="UNKNOWN")
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index(
            "ix_economic_provider_link_identity",
            "provider",
            "provider_dataset",
            "provider_event_id",
        ),
    )


class EconomicEventDisagreement(Base):
    """Append-only record of conflicting non-null provider field values."""

    __tablename__ = "economic_event_disagreements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    disagreement_hash = Column(String(64), nullable=False, unique=True, index=True)
    economic_event_id = Column(
        Integer,
        ForeignKey("economic_events.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    field_name = Column(String(40), nullable=False, index=True)
    authoritative_provider = Column(String(32), nullable=False)
    authoritative_value = Column(Text, nullable=False)
    conflicting_provider = Column(String(32), nullable=False)
    conflicting_value = Column(Text, nullable=False)
    rule_version = Column(String(24), nullable=False)
    detected_at = Column(DateTime(timezone=True), nullable=False, index=True)
    resolved_at = Column(DateTime(timezone=True), nullable=True)


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


class EconomicBackfillJob(Base):
    """Durable cursor and counters for a resumable historical provider job."""

    __tablename__ = "economic_backfill_jobs"

    job_id = Column(String(64), primary_key=True)
    provider = Column(String(32), nullable=False, index=True)
    date_from = Column(DateTime(timezone=True), nullable=False)
    date_to = Column(DateTime(timezone=True), nullable=False)
    current_cursor = Column(DateTime(timezone=True), nullable=False)
    chunk_days = Column(Integer, nullable=False)
    status = Column(String(24), nullable=False, index=True)
    events_seen = Column(Integer, nullable=False, default=0)
    observations_added = Column(Integer, nullable=False, default=0)
    duplicates_skipped = Column(Integer, nullable=False, default=0)
    attempt_count = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, index=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)


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
