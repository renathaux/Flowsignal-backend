import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select, update

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from services.deriv_binary_execution_service import binary_executions, sync_accounts
from services.deriv_binary_settlement_recovery import recover_once
from services.deriv_v5_demo_relay_service import RULE_HASH, STRATEGY_VERSION


def demo(account_id="V1"):
    return {"account_id": account_id, "account_type": "virtual", "currency": "USD", "balance": 1000}


def seed(engine, *, user="user-a", account="V1", contract="C1", status="SETTLEMENT_PENDING",
         signal_id=None, settled=False):
    sync_accounts(user, f"connection-{user}", [demo(account)], selected_account_id=account, engine=engine)
    stamp = 1700000000
    signal_id = signal_id or f"{STRATEGY_VERSION}:frxEURUSD:{stamp}:RISE"
    with engine.begin() as connection:
        result = connection.execute(binary_executions.insert().values(
            user_id=user, deriv_account_id=account, account_type="DEMO",
            strategy_version=STRATEGY_VERSION, rule_hash=RULE_HASH, signal_id=signal_id,
            direction="RISE", contract_type="CALL", symbol="frxEURUSD", duration=5,
            duration_unit="m", stake=2, currency="USD", contract_id=contract,
            broker_status="WON" if settled else status, outcome="WIN" if settled else None,
            settlement_timestamp=stamp + 300 if settled else None,
            recovery_attempt_count=0, created_at=time.time(), updated_at=time.time(),
        ))
        return result.inserted_primary_key[0]


def final(outcome):
    profit = 1.6 if outcome == "WIN" else -2.0 if outcome == "LOSS" else 0.0
    return {"broker_status": "WON" if outcome == "WIN" else "LOST" if outcome == "LOSS" else "SOLD",
            "outcome": outcome, "profit_loss": profit, "settlement_payout": 3.6 if profit > 0 else 0,
            "settlement_timestamp": 1700000300, "settlement_price": 1.101}


def owned_context(*_args, **_kwargs):
    return {"access_token": "encrypted-store-token", "account": demo(), "account_id": "V1"}


def test_restart_recovery_settles_win_and_loss_without_trade_calls():
    for outcome in ("WIN", "LOSS"):
        with tempfile.TemporaryDirectory() as directory:
            engine = create_engine(f"sqlite:///{Path(directory) / 'recovery.sqlite3'}")
            seed(engine)
            proposal_calls = []; buy_calls = []; monitor_calls = []
            with patch("services.deriv_binary_settlement_recovery.private_account", side_effect=owned_context):
                report = recover_once(engine=engine, worker_id="restart-worker",
                    monitor=lambda _context, contract: monitor_calls.append(contract) or final(outcome), now=1000)
            assert report["settled"] == 1
            assert monitor_calls == ["C1"] and proposal_calls == [] and buy_calls == []
            with engine.begin() as connection:
                row = connection.execute(select(binary_executions)).mappings().one()
            assert row["outcome"] == outcome and row["recovery_status"] == "SETTLED"
            engine.dispose()


def test_multiple_restarts_and_two_workers_are_idempotent():
    with tempfile.TemporaryDirectory() as directory:
        engine = create_engine(f"sqlite:///{Path(directory) / 'race.sqlite3'}")
        seed(engine); calls=[]
        def monitor(_context, contract):
            calls.append(contract); time.sleep(.05); return final("WIN")
        with patch("services.deriv_binary_settlement_recovery.private_account", side_effect=owned_context):
            with ThreadPoolExecutor(max_workers=2) as pool:
                reports = list(pool.map(lambda worker: recover_once(engine=engine, worker_id=worker, monitor=monitor, now=1000), ("w1", "w2")))
            again = recover_once(engine=engine, worker_id="w3", monitor=monitor, now=2000)
        assert calls == ["C1"]
        assert sum(report["settled"] for report in reports) == 1
        assert again["found"] == 0
        engine.dispose()


def test_expired_credential_stays_recoverable_then_reconnects():
    with tempfile.TemporaryDirectory() as directory:
        engine = create_engine(f"sqlite:///{Path(directory) / 'reconnect.sqlite3'}")
        seed(engine); monitor_calls=[]
        with patch("services.deriv_binary_settlement_recovery.private_account", side_effect=RuntimeError("DERIV_RECOVERY_RECONNECT_REQUIRED")):
            first = recover_once(engine=engine, worker_id="w1", monitor=lambda *_: monitor_calls.append(1), now=1000)
        with engine.begin() as connection:
            row = connection.execute(select(binary_executions)).mappings().one()
        assert first["deferred"] == 1 and row["broker_status"] == "RECONNECT_REQUIRED" and monitor_calls == []
        with patch("services.deriv_binary_settlement_recovery.private_account", side_effect=owned_context):
            second = recover_once(engine=engine, worker_id="w2", monitor=lambda *_: final("WIN"), now=1020)
        assert second["settled"] == 1
        engine.dispose()


def test_malformed_test_and_already_settled_rows_never_recover():
    with tempfile.TemporaryDirectory() as directory:
        engine = create_engine(f"sqlite:///{Path(directory) / 'ignored.sqlite3'}")
        seed(engine, account="V1", contract="", signal_id="malformed")
        seed(engine, account="V2", contract="C2", signal_id="TEST-V5-RELAY-ROW")
        seed(engine, account="V3", contract="C3", settled=True)
        calls=[]
        report = recover_once(engine=engine, worker_id="ignored", monitor=lambda *_: calls.append(1), now=1000)
        assert report["settled"] == 0 and calls == []
        engine.dispose()


def test_user_a_recovery_never_uses_user_b_account_or_contract():
    with tempfile.TemporaryDirectory() as directory:
        engine = create_engine(f"sqlite:///{Path(directory) / 'users.sqlite3'}")
        seed(engine, user="user-a", account="A1", contract="CA")
        seed(engine, user="user-b", account="B1", contract="CB")
        seen=[]
        def exact(connection_id, user_id, account_id):
            seen.append((connection_id, user_id, account_id))
            return {"access_token": f"token-{user_id}", "account": demo(account_id), "account_id": account_id}
        with patch("services.deriv_binary_settlement_recovery.private_account", side_effect=exact):
            report = recover_once(engine=engine, worker_id="isolation", monitor=lambda context, contract: final("WIN"), now=1000)
        assert report["settled"] == 2
        assert set(seen) == {("connection-user-a", "user-a", "A1"), ("connection-user-b", "user-b", "B1")}
        engine.dispose()
