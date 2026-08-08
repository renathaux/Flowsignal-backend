from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Index, Integer, JSON, String, Text
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
