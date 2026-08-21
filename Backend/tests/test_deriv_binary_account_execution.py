import ast
import hashlib
import hmac
import json
import os
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, func, select

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from services.deriv_binary_execution_service import (
    DURATION, DURATION_UNIT, SYMBOL, account_settings, account_type,
    binary_accounts, binary_executions, binary_signal_claims, execute_relayed_signal,
    execution_snapshot,
    execute_signal_candidates,
    genuine_signal_validation, latest_relay_signal,
    save_account_settings, select_account, sync_accounts,
)
from services.deriv_v5_demo_relay_service import RULE_HASH, STRATEGY_VERSION, receive_signal


class AccountAwareBinaryExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.engine = create_engine(f"sqlite:///{Path(self.temp.name) / 'binary.sqlite3'}")
        self.signal_id = f"{STRATEGY_VERSION}:{SYMBOL}:1000:RISE"
        body = json.dumps({
            "strategy_version": STRATEGY_VERSION, "rule_hash": RULE_HASH,
            "signal_id": self.signal_id, "direction": "RISE", "symbol": SYMBOL,
            "decision_timestamp": 1000, "entry_timestamp": 1000,
            "entry_quote": 1.1, "entry_quote_epoch": 1000,
            "settlement_target_timestamp": 1300,
        }, separators=(",", ":")).encode()
        sig = hmac.new(b"secret", b"1000." + body, hashlib.sha256).hexdigest()
        receive_signal(body, "1000", sig, secret="secret", now=1000, engine=self.engine)

    def tearDown(self):
        self.engine.dispose(); self.temp.cleanup()

    @staticmethod
    def demo(aid="VIRTUAL123", currency="USD"):
        return {"account_id": aid, "account_type": "virtual", "currency": currency, "balance": 10000}

    @staticmethod
    def real(aid="CR123", currency="USD"):
        return {"account_id": aid, "account_type": "real", "currency": currency, "balance": 500}

    @staticmethod
    def broker_result(outcome="WIN"):
        return {"proposal_id":"p1","contract_id":"c1","transaction_id":"t1","buy_price":1.0,
            "potential_payout":1.8,"purchase_timestamp":1100,"expiry_timestamp":1400,"broker_status":"WON" if outcome=="WIN" else "LOST",
            "outcome":outcome,"profit_loss":0.8 if outcome=="WIN" else -1.0,"settlement_payout":1.8 if outcome=="WIN" else 0,
            "settlement_timestamp":1400,"settlement_price":1.101,"raw":{"status":outcome.lower()}}

    def prepare(self, user="u1", account=None, enabled=True, stake=2.5):
        account = account or self.demo()
        sync_accounts(user, "conn", [account], engine=self.engine)
        save_account_settings(user, account["account_id"], enabled=enabled, stake=stake, engine=self.engine)
        private = {"access_token":"token","account":account,"account_id":account["account_id"]}
        return private

    def insert_inbox(self, signal_id, *, strategy=STRATEGY_VERSION, rule_hash=RULE_HASH,
                     symbol=SYMBOL, direction="RISE", entry_timestamp=2000, decision_timestamp=2000):
        with self.engine.begin() as c:
            c.exec_driver_sql(
                """INSERT INTO deriv_v5_relay_signals(
                   signal_id,strategy_version,rule_hash,direction,symbol,decision_timestamp,
                   entry_timestamp,entry_quote,entry_quote_epoch,settlement_target_timestamp,
                   payload_json,received_at,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (signal_id,strategy,rule_hash,direction,symbol,decision_timestamp,entry_timestamp,
                 1.2,entry_timestamp,entry_timestamp+300,"{}",float(decision_timestamp),"RECEIVED"),
            )

    def run_signal(self, private, user="u1", broker=None):
        broker = broker or (lambda *_: self.broker_result())
        with patch("services.deriv_binary_execution_service.private_selected_account", return_value=private):
            return execute_relayed_signal(user, "conn", self.signal_id, engine=self.engine, broker=broker)

    def test_demo_and_real_are_authoritatively_recognized(self):
        self.assertEqual(account_type(self.demo()), "DEMO")
        self.assertEqual(account_type(self.real()), "REAL")
        with self.assertRaisesRegex(RuntimeError, "TYPE_UNCERTAIN"):
            account_type({"account_id":"x", "currency":"USD"})

    def test_account_selection_never_falls_back(self):
        sync_accounts("u1", "conn", [self.demo("V1"), self.real("R1")], selected_account_id="V1", engine=self.engine)
        select_account("u1", "conn", "R1", engine=self.engine)
        self.assertTrue(account_settings("u1", "R1", engine=self.engine)["selected"])
        self.assertFalse(account_settings("u1", "V1", engine=self.engine)["selected"])
        with self.assertRaisesRegex(RuntimeError, "NOT_AUTHORIZED"):
            select_account("u1", "conn", "missing", engine=self.engine)

    def test_settings_are_per_user_and_account(self):
        sync_accounts("u1", "c1", [self.demo("V1"), self.real("R1")], selected_account_id="V1", engine=self.engine)
        sync_accounts("u2", "c2", [self.demo("V2")], engine=self.engine)
        save_account_settings("u1", "V1", enabled=True, stake=3, engine=self.engine)
        save_account_settings("u1", "R1", enabled=False, stake=8, engine=self.engine)
        save_account_settings("u2", "V2", enabled=True, stake=5, engine=self.engine)
        self.assertEqual(account_settings("u1", "V1", engine=self.engine)["binary_stake"], 3)
        self.assertEqual(account_settings("u1", "R1", engine=self.engine)["binary_stake"], 8)
        self.assertEqual(account_settings("u2", "V2", engine=self.engine)["binary_stake"], 5)

    def test_binary_auto_off_blocks_before_broker(self):
        private=self.prepare(enabled=False); calls=[]
        result=self.run_signal(private, broker=lambda *_: calls.append(1))
        self.assertEqual(result["reason"], "BINARY_AUTO_OFF"); self.assertEqual(calls, [])

    def test_real_reaches_policy_and_is_blocked_by_default_flag(self):
        private=self.prepare(account=self.real(), enabled=True); calls=[]
        with patch.dict(os.environ, {"BINARY_REAL_EXECUTION_ENABLED":"false"}):
            result=self.run_signal(private, broker=lambda *_: calls.append(1))
        self.assertEqual(result["reason"], "REAL_BINARY_EXECUTION_DISABLED"); self.assertEqual(calls, [])

    def test_demo_executes_authoritative_rise_as_call_for_exactly_five_minutes(self):
        private=self.prepare(); seen={}
        def broker(_account,direction,stake,currency): seen.update(direction=direction,stake=stake,currency=currency); return self.broker_result()
        self.assertTrue(self.run_signal(private,broker=broker)["executed"])
        with self.engine.begin() as c: row=c.execute(select(binary_executions)).mappings().one()
        self.assertEqual((seen["direction"],row["contract_type"],row["symbol"],row["duration"],row["duration_unit"]),("RISE","CALL",SYMBOL,5,"m"))

    def test_fall_maps_to_put_without_recalculation(self):
        fall_id=f"{STRATEGY_VERSION}:{SYMBOL}:1000:FALL"
        with self.engine.begin() as c:
            c.exec_driver_sql("UPDATE deriv_v5_relay_signals SET direction='FALL', signal_id=?",(fall_id,))
        self.signal_id=fall_id
        private=self.prepare(); self.run_signal(private,broker=lambda *_:self.broker_result("LOSS"))
        with self.engine.begin() as c: row=c.execute(select(binary_executions)).mappings().one()
        self.assertEqual((row["direction"],row["contract_type"],row["outcome"]),("FALL","PUT","LOSS"))

    def test_lifecycle_persists_proposal_contract_and_win(self):
        private=self.prepare(); self.run_signal(private)
        with self.engine.begin() as c: row=c.execute(select(binary_executions)).mappings().one()
        self.assertEqual((row["proposal_id"],row["contract_id"],row["outcome"],row["profit_loss"]),("p1","c1","WIN",0.8))
        self.assertEqual((row["strategy_version"],row["rule_hash"]),(STRATEGY_VERSION,RULE_HASH))
        snapshot=execution_snapshot("u1","VIRTUAL123",engine=self.engine)
        self.assertNotIn("user_id",snapshot["last_execution"])
        self.assertNotIn("rule_hash",snapshot["last_execution"])
        self.assertNotIn("broker_payload_json",snapshot["last_execution"])

    def test_same_signal_is_independent_across_accounts_but_duplicate_safe(self):
        first=self.prepare("u1",self.demo("V1")); self.run_signal(first,"u1")
        second=self.prepare("u2",self.demo("V2")); self.run_signal(second,"u2")
        duplicate=self.run_signal(first,"u1")
        self.assertEqual(duplicate["reason"],"SIGNAL_ALREADY_EXECUTED")
        with self.engine.begin() as c: self.assertEqual(c.execute(select(func.count()).select_from(binary_executions)).scalar_one(),2)

    def test_same_physical_account_is_claimed_once_across_legacy_users(self):
        first=self.prepare("u1",self.demo("V1")); second=self.prepare("u2",self.demo("V1")); calls=[]
        self.run_signal(first,"u1",broker=lambda *_:calls.append("u1") or self.broker_result())
        duplicate=self.run_signal(second,"u2",broker=lambda *_:calls.append("u2") or self.broker_result())
        self.assertEqual(duplicate["reason"],"SIGNAL_ALREADY_EXECUTED")
        self.assertEqual(calls,["u1"])
        with self.engine.begin() as c:
            self.assertEqual(c.execute(select(func.count()).select_from(binary_signal_claims)).scalar_one(),1)

    def test_simultaneous_attempts_make_only_one_broker_call(self):
        private=self.prepare(); calls=[]
        def broker(*_): calls.append(1); return self.broker_result()
        with patch("services.deriv_binary_execution_service.private_selected_account",return_value=private):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results=list(pool.map(lambda _:execute_relayed_signal("u1","conn",self.signal_id,engine=self.engine,broker=broker),range(2)))
        self.assertEqual(calls,[1])
        self.assertEqual(sum(bool(result.get("executed")) for result in results),1)
        self.assertEqual(sum(result.get("reason")=="SIGNAL_ALREADY_EXECUTED" for result in results),1)

    def test_restart_safe_reserved_row_blocks_second_purchase(self):
        private=self.prepare(); calls=[]
        def failed(*_): calls.append(1); raise RuntimeError("transport uncertain")
        with self.assertRaises(RuntimeError): self.run_signal(private,broker=failed)
        self.assertEqual(self.run_signal(private,broker=lambda *_:calls.append(2))["reason"],"SIGNAL_ALREADY_EXECUTED")
        self.assertEqual(calls,[1])

    def test_proposal_checkpoint_survives_failure_and_blocks_retry(self):
        private=self.prepare(); calls=[]
        def failed(context,*_):
            calls.append("proposal"); context["checkpoint"]("PROPOSED",proposal_id="proposal-durable")
            raise RuntimeError("socket closed before buy")
        with self.assertRaises(RuntimeError): self.run_signal(private,broker=failed)
        with self.engine.begin() as c: row=c.execute(select(binary_executions)).mappings().one()
        self.assertEqual((row["proposal_id"],row["broker_status"]),("proposal-durable","PROPOSAL_FAILED_SAFE"))
        self.assertEqual(self.run_signal(private,broker=lambda *_:calls.append("retry"))["reason"],"SIGNAL_ALREADY_EXECUTED")
        self.assertEqual(calls,["proposal"])

    def test_purchase_timeout_is_ambiguous_and_never_retried(self):
        private=self.prepare(); calls=[]
        def uncertain(context,*_):
            calls.append("buy-request"); context["checkpoint"]("PROPOSED",proposal_id="p-timeout")
            context["checkpoint"]("PURCHASE_REQUEST_SENT",proposal_id="p-timeout")
            raise TimeoutError("no buy response")
        with self.assertRaises(TimeoutError): self.run_signal(private,broker=uncertain)
        with self.engine.begin() as c: row=c.execute(select(binary_executions)).mappings().one()
        self.assertEqual(row["broker_status"],"PURCHASE_AMBIGUOUS")
        self.assertIn("no buy response",row["broker_payload_json"])
        self.assertEqual(self.run_signal(private,broker=lambda *_:calls.append("retry"))["reason"],"SIGNAL_ALREADY_EXECUTED")
        self.assertEqual(calls,["buy-request"])

    def test_purchase_checkpoint_survives_settlement_monitor_timeout(self):
        private=self.prepare()
        def pending(context,*_):
            context["checkpoint"]("PROPOSED",proposal_id="p1")
            context["checkpoint"]("PURCHASED",contract_id="c1",transaction_id="t1",buy_price=1.0,purchase_timestamp=1100)
            raise TimeoutError("settlement stream interrupted")
        with self.assertRaises(TimeoutError): self.run_signal(private,broker=pending)
        with self.engine.begin() as c: row=c.execute(select(binary_executions)).mappings().one()
        self.assertEqual((row["contract_id"],row["broker_status"]),("c1","SETTLEMENT_PENDING"))
        self.assertIsNotNone(execution_snapshot("u1","VIRTUAL123",engine=self.engine)["running_contract"])

    def test_failed_or_unpurchased_execution_is_never_reported_live(self):
        private=self.prepare()
        with self.assertRaisesRegex(RuntimeError,"proposal rejected"):
            self.run_signal(private,broker=lambda *_:(_ for _ in ()).throw(RuntimeError("proposal rejected")))
        snapshot=execution_snapshot("u1","VIRTUAL123",engine=self.engine)
        self.assertIsNone(snapshot["running_contract"])
        self.assertEqual(snapshot["last_execution"]["broker_status"],"FAILED_SAFE")

    def test_failure_diagnostic_redacts_access_token(self):
        private=self.prepare(); private["access_token"]="super-secret-token"
        with self.assertRaises(RuntimeError):
            self.run_signal(private,broker=lambda *_:(_ for _ in ()).throw(RuntimeError("failure super-secret-token")))
        with self.engine.begin() as c: payload=c.execute(select(binary_executions.c.broker_payload_json)).scalar_one()
        self.assertIn("[REDACTED]",payload)
        self.assertNotIn("super-secret-token",payload)

    def test_cross_user_cannot_read_or_change_other_account_settings(self):
        self.prepare("u1",self.demo("V1"),stake=7)
        with self.assertRaisesRegex(RuntimeError,"DERIV_ACCOUNT_NOT_FOUND"):
            account_settings("u2","V1",engine=self.engine)
        with self.assertRaisesRegex(RuntimeError,"DERIV_ACCOUNT_NOT_FOUND"):
            save_account_settings("u2","V1",enabled=True,stake=99,engine=self.engine)

    def test_candidate_dispatch_uses_each_selected_auto_account(self):
        first=self.prepare("u1",self.demo("V1")); self.prepare("u2",self.demo("V2")); calls=[]
        def selected(_connection,user):
            aid="V1" if user=="u1" else "V2"; return {"access_token":"t","account":self.demo(aid),"account_id":aid}
        with patch("services.deriv_binary_execution_service.private_selected_account",side_effect=selected):
            results=execute_signal_candidates(self.signal_id,engine=self.engine,broker=lambda context,*_: calls.append(context["account_id"]) or self.broker_result())
        self.assertEqual(sorted(calls),["V1","V2"]); self.assertTrue(all(r["ok"] for r in results))

    def test_candidate_dispatch_uses_newest_session_for_same_account(self):
        self.prepare("u1",self.demo("V1")); time.sleep(0.01); self.prepare("u2",self.demo("V1")); calls=[]
        def selected(_connection,user):
            return {"access_token":"t","account":self.demo("V1"),"account_id":"V1"}
        with patch("services.deriv_binary_execution_service.private_selected_account",side_effect=selected):
            results=execute_signal_candidates(self.signal_id,engine=self.engine,
                broker=lambda _context,*_:calls.append("broker") or self.broker_result())
        self.assertEqual(calls,["broker"])
        self.assertEqual(len(results),1)

    def test_invalid_stake_and_currency_are_blocked(self):
        sync_accounts("u1","c",[self.demo()],engine=self.engine)
        with self.assertRaisesRegex(RuntimeError,"STAKE_INVALID"): save_account_settings("u1","VIRTUAL123",enabled=True,stake=0,engine=self.engine)
        with self.assertRaisesRegex(RuntimeError,"CURRENCY_UNCERTAIN"): sync_accounts("u2","c",[{"account_id":"V2","account_type":"demo"}],engine=self.engine)

    def test_service_has_no_forex_or_ctrader_dependency(self):
        source=(BACKEND/"services"/"deriv_binary_execution_service.py").read_text()
        tree=ast.parse(source)
        modules={n.module for n in ast.walk(tree) if isinstance(n,ast.ImportFrom) and n.module}
        modules.update(a.name for n in ast.walk(tree) if isinstance(n,ast.Import) for a in n.names)
        calls={n.func.id for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)}
        self.assertFalse(any("ctrader" in m.lower() or "forex" in m.lower() for m in modules))
        self.assertNotIn("place_market_order",calls)

    def test_genuine_frozen_worker_signal_is_executable(self):
        with self.engine.begin() as c:
            row=c.exec_driver_sql("SELECT * FROM deriv_v5_relay_signals WHERE signal_id=?",(self.signal_id,)).mappings().one()
        self.assertTrue(genuine_signal_validation(row)["valid"])

    def test_all_non_genuine_identity_variants_are_blocked_before_broker(self):
        invalid = [
            ("TEST-V5-RELAY-20260818-A", STRATEGY_VERSION, RULE_HASH, SYMBOL, "RISE", 2000),
            ("arbitrary-malformed-id", STRATEGY_VERSION, RULE_HASH, SYMBOL, "RISE", 2001),
            (f"WRONG:{SYMBOL}:2002:RISE", "WRONG", RULE_HASH, SYMBOL, "RISE", 2002),
            (f"{STRATEGY_VERSION}:{SYMBOL}:2003:RISE", STRATEGY_VERSION, "0"*64, SYMBOL, "RISE", 2003),
            (f"{STRATEGY_VERSION}:R_100:2004:RISE", STRATEGY_VERSION, RULE_HASH, "R_100", "RISE", 2004),
            (f"{STRATEGY_VERSION}:{SYMBOL}:2005:FALL", STRATEGY_VERSION, RULE_HASH, SYMBOL, "RISE", 2005),
            (f"{STRATEGY_VERSION}:{SYMBOL}:invalid:RISE", STRATEGY_VERSION, RULE_HASH, SYMBOL, "RISE", 2006),
            (f"{STRATEGY_VERSION}:{SYMBOL}:9999:RISE", STRATEGY_VERSION, RULE_HASH, SYMBOL, "RISE", 2007),
        ]
        broker_calls=[]
        for signal_id,strategy,rule_hash,symbol,direction,entry in invalid:
            with self.subTest(signal_id=signal_id):
                self.insert_inbox(signal_id,strategy=strategy,rule_hash=rule_hash,symbol=symbol,direction=direction,entry_timestamp=entry,decision_timestamp=entry)
                result=execute_relayed_signal("u","connection",signal_id,engine=self.engine,broker=lambda *_:broker_calls.append(1))
                self.assertEqual(result["reason"],"NON_EXECUTABLE_V5_SIGNAL")
        self.assertEqual(broker_calls,[])

    def test_actionable_signal_retrieval_skips_newer_test_row(self):
        self.insert_inbox("TEST-V5-RELAY-NEWEST",entry_timestamp=9999,decision_timestamp=9999)
        actionable=latest_relay_signal(engine=self.engine)
        self.assertEqual(actionable["signal_id"],self.signal_id)

    def test_binary_auto_dispatch_cannot_reach_broker_for_test_row(self):
        private=self.prepare(); calls=[]
        self.insert_inbox("TEST-V5-RELAY-AUTO",entry_timestamp=3000,decision_timestamp=3000)
        with patch("services.deriv_binary_execution_service.private_selected_account",return_value=private):
            result=execute_signal_candidates("TEST-V5-RELAY-AUTO",engine=self.engine,broker=lambda *_:calls.append(1))
        self.assertEqual(result[0]["reason"],"NON_EXECUTABLE_V5_SIGNAL")
        self.assertEqual(calls,[])


if __name__ == "__main__": unittest.main()
