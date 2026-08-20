import tempfile
import time
import sys
import unittest
from pathlib import Path

from sqlalchemy import create_engine

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from services.deriv_binary_execution_service import binary_executions, sync_accounts
from services.deriv_binary_history_service import execution_history


def demo(account_id):
    return {"account_id": account_id, "account_type": "virtual", "currency": "USD", "balance": 1000}


def insert_execution(engine, *, user_id, account_id, index):
    now = time.time() + index
    with engine.begin() as connection:
        connection.execute(binary_executions.insert().values(
            user_id=user_id, deriv_account_id=account_id, account_type="DEMO",
            strategy_version="DERIV_BINARY_V5_NOISY_REVERSAL_FROZEN_1",
            rule_hash="secret-internal-rule-hash", signal_id=f"signal-{user_id}-{index}",
            direction="RISE", contract_type="CALL", symbol="frxEURUSD",
            duration=5, duration_unit="m", stake=2.0, currency="USD",
            proposal_id=f"proposal-{index}", contract_id=f"contract-{index}",
            transaction_id=f"transaction-{index}", buy_price=2.0,
            potential_payout=3.6, purchase_timestamp=1000 + index,
            expiry_timestamp=1300 + index, broker_status="WON", outcome="WIN",
            profit_loss=1.6, settlement_payout=3.6,
            settlement_timestamp=1300 + index, settlement_price=1.1,
            broker_payload_json='{"access_token":"must-never-leave-backend"}',
            created_at=now, updated_at=now,
        ))


class BinaryHistoryTests(unittest.TestCase):
    def test_history_is_user_scoped_paginated_and_sanitized(self):
      with tempfile.TemporaryDirectory() as directory:
        engine = create_engine(f"sqlite:///{Path(directory) / 'history.sqlite3'}")
        sync_accounts("user-a", "conn-a", [demo("A-DEMO")], engine=engine)
        sync_accounts("user-b", "conn-b", [demo("B-DEMO")], engine=engine)
        for index in range(3):
            insert_execution(engine, user_id="user-a", account_id="A-DEMO", index=index)
        insert_execution(engine, user_id="user-b", account_id="B-DEMO", index=9)

        first = execution_history("user-a", "A-DEMO", limit=2, offset=0, engine=engine)
        second = execution_history("user-a", "A-DEMO", limit=2, offset=2, engine=engine)

        assert first["total"] == 3 and first["count"] == 2 and first["has_more"] is True
        assert second["count"] == 1 and second["has_more"] is False
        assert [item["signal_id"] for item in first["items"]] == ["signal-user-a-2", "signal-user-a-1"]
        assert all(item["deriv_account_id"] == "A-DEMO" for item in first["items"])
        assert all("user_id" not in item for item in first["items"])
        assert all("broker_payload_json" not in item for item in first["items"])
        assert all("rule_hash" not in item for item in first["items"])

        with self.assertRaisesRegex(RuntimeError, "DERIV_ACCOUNT_NOT_FOUND"):
            execution_history("user-a", "B-DEMO", engine=engine)
        engine.dispose()


    def test_history_bounds_limit_and_offset(self):
      with tempfile.TemporaryDirectory() as directory:
        engine = create_engine(f"sqlite:///{Path(directory) / 'bounds.sqlite3'}")
        sync_accounts("user-a", "conn-a", [demo("A-DEMO")], engine=engine)
        result = execution_history("user-a", "A-DEMO", limit=1000, offset=-4, engine=engine)
        assert result["limit"] == 100
        assert result["offset"] == 0
        engine.dispose()


if __name__ == "__main__":
    unittest.main()
