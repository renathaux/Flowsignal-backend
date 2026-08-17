import unittest
from types import SimpleNamespace

from services.v1_outcome_reconciliation_service import (
    _closed_payload,
    _execution_result,
)


class V1OutcomeReconciliationTests(unittest.TestCase):
    def test_execution_result_uses_execution_block_not_active_trade_overlay(self):
        snapshot = {
            "active_trade": {
                "position_id": "old-position",
                "order_id": "old-order",
            },
            "execution": {
                "result": {
                    "ok": True,
                    "position_id": "new-position",
                    "order_id": "new-order",
                    "direction": "BUY",
                }
            },
        }
        result = _execution_result(snapshot)
        self.assertEqual(result["position_id"], "new-position")
        self.assertEqual(result["order_id"], "new-order")

    def test_closed_payload_persists_broker_win_and_realized_pnl(self):
        evaluation = SimpleNamespace(
            v1_outcome_json={"entry": 1.1000, "closed": False}
        )
        payload = _closed_payload(
            {
                "position_id": 123,
                "order_id": 456,
                "deal_id": 789,
                "result": "WIN",
                "broker_realized_profit": 42.75,
                "closed_at": 1786982400,
                "close_price": 1.1050,
            },
            evaluation,
        )
        self.assertTrue(payload["closed"])
        self.assertEqual(payload["outcome"], "WIN")
        self.assertEqual(payload["realized_pnl"], 42.75)
        self.assertEqual(payload["position_id"], "123")
        self.assertEqual(payload["order_id"], "456")
        self.assertEqual(payload["deal_id"], "789")
        self.assertEqual(payload["source"], "ctrader_closed_history")
        self.assertEqual(payload["entry"], 1.1000)

    def test_closed_payload_derives_loss_from_pnl_when_status_is_generic(self):
        evaluation = SimpleNamespace(v1_outcome_json={})
        payload = _closed_payload(
            {
                "position_id": "p1",
                "result": "BROKER_CLOSED",
                "pnl": -18.25,
            },
            evaluation,
        )
        self.assertEqual(payload["outcome"], "LOSS")
        self.assertEqual(payload["realized_pnl"], -18.25)


if __name__ == "__main__":
    unittest.main()
