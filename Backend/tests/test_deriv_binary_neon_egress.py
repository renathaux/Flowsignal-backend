from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import create_engine, event

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from services.deriv_binary_execution_service import (
    binary_accounts,
    binary_executions,
    metadata as execution_metadata,
)
from services.deriv_binary_history_service import execution_history
from services.deriv_binary_read_service import execution_snapshot, latest_relay_signal
from services.deriv_v5_demo_relay_service import (
    RULE_HASH,
    STRATEGY_VERSION,
    metadata as relay_metadata,
    relay_signals,
)


def _engine():
    engine = create_engine("sqlite://")
    execution_metadata.create_all(engine)
    relay_metadata.create_all(engine)
    return engine


def _capture_selects(engine):
    statements = []

    def before_cursor_execute(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement.lower())

    event.listen(engine, "before_cursor_execute", before_cursor_execute)
    return statements, before_cursor_execute


def test_latest_signal_keeps_newest_valid_semantics_without_downloading_relay_payload_json():
    engine = _engine()
    valid_signal_id = f"{STRATEGY_VERSION}:frxEURUSD:1700000000:RISE"
    malformed_newer = f"{STRATEGY_VERSION}:frxEURUSD:bad:RISE"
    with engine.begin() as connection:
        connection.execute(relay_signals.insert(), [
            {
                "signal_id": valid_signal_id,
                "strategy_version": STRATEGY_VERSION,
                "rule_hash": RULE_HASH,
                "direction": "RISE",
                "symbol": "frxEURUSD",
                "decision_timestamp": 1700000000,
                "entry_timestamp": 1700000000,
                "entry_quote": 1.101,
                "entry_quote_epoch": 1700000000,
                "settlement_target_timestamp": 1700000300,
                "payload_json": "x" * 250_000,
                "received_at": 1700000000.0,
                "status": "RECEIVED",
            },
            {
                "signal_id": malformed_newer,
                "strategy_version": STRATEGY_VERSION,
                "rule_hash": RULE_HASH,
                "direction": "RISE",
                "symbol": "frxEURUSD",
                "decision_timestamp": 1700000100,
                "entry_timestamp": 1700000100,
                "entry_quote": 1.102,
                "entry_quote_epoch": 1700000100,
                "settlement_target_timestamp": 1700000400,
                "payload_json": "y" * 250_000,
                "received_at": 1700000100.0,
                "status": "RECEIVED",
            },
        ])

    statements, listener = _capture_selects(engine)
    try:
        result = latest_relay_signal(engine=engine)
    finally:
        event.remove(engine, "before_cursor_execute", listener)

    assert result["signal_id"] == valid_signal_id
    assert result["signal"] == "RISE"
    assert statements
    assert all("payload_json" not in statement for statement in statements)
    engine.dispose()


def test_execution_status_and_history_do_not_download_private_broker_payload_json():
    engine = _engine()
    signal_id = f"{STRATEGY_VERSION}:frxEURUSD:1700000000:FALL"
    with engine.begin() as connection:
        connection.execute(binary_accounts.insert().values(
            user_id="user-a",
            deriv_account_id="V1",
            connection_id="connection-a",
            account_type="DEMO",
            currency="USD",
            balance=1000.0,
            auth_state="CONNECTED",
            selected=True,
            binary_auto_enabled=True,
            binary_stake=2.0,
            binary_duration_minutes=5,
            updated_at=1700000000.0,
        ))
        connection.execute(relay_signals.insert().values(
            signal_id=signal_id,
            strategy_version=STRATEGY_VERSION,
            rule_hash=RULE_HASH,
            direction="FALL",
            symbol="frxEURUSD",
            decision_timestamp=1700000000,
            entry_timestamp=1700000000,
            entry_quote=1.101,
            entry_quote_epoch=1700000000,
            settlement_target_timestamp=1700000300,
            payload_json="relay" * 50_000,
            received_at=1700000000.0,
            status="RECEIVED",
        ))
        connection.execute(binary_executions.insert().values(
            user_id="user-a",
            deriv_account_id="V1",
            account_type="DEMO",
            strategy_version=STRATEGY_VERSION,
            rule_hash=RULE_HASH,
            signal_id=signal_id,
            direction="FALL",
            contract_type="PUT",
            symbol="frxEURUSD",
            duration=5,
            duration_unit="m",
            stake=2.0,
            currency="USD",
            proposal_id="P1",
            contract_id="C1",
            transaction_id="T1",
            buy_price=2.0,
            potential_payout=3.6,
            purchase_timestamp=1700000000,
            expiry_timestamp=1700000300,
            broker_status="WON",
            outcome="WIN",
            profit_loss=1.6,
            settlement_payout=3.6,
            settlement_timestamp=1700000300,
            settlement_price=1.100,
            broker_payload_json="broker" * 100_000,
            recovery_status="SETTLED",
            recovery_attempt_count=0,
            created_at=1700000000.0,
            updated_at=1700000300.0,
        ))

    statements, listener = _capture_selects(engine)
    try:
        snapshot = execution_snapshot("user-a", "V1", engine=engine)
        history = execution_history("user-a", "V1", engine=engine)
    finally:
        event.remove(engine, "before_cursor_execute", listener)

    assert snapshot["last_execution"]["contract_id"] == "C1"
    assert "broker_payload_json" not in snapshot["last_execution"]
    assert history["items"][0]["contract_id"] == "C1"
    assert "broker_payload_json" not in history["items"][0]
    assert statements
    assert all("broker_payload_json" not in statement for statement in statements)
    engine.dispose()
