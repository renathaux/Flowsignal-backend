import asyncio
import importlib.util
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.modules.setdefault("websockets", types.SimpleNamespace(connect=None))
RUNNER_PATH = BACKEND_DIR / "scripts" / "run_deriv_binary_v5_forward_validator.py"
SPEC = importlib.util.spec_from_file_location("v5_worker_runner", RUNNER_PATH)
runner = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(runner)


class V5WorkerRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "forward.sqlite3"
        runner.initialize_database(self.db, collection_start_timestamp=3900)

    def tearDown(self):
        self.temp.cleanup()

    def test_invalid_boundary_is_skipped_and_checkpointed(self):
        invalid = AsyncMock(side_effect=ValueError("Entry quote is more than two seconds before the boundary"))
        with patch.object(runner, "process_boundary", invalid):
            result = asyncio.run(runner._process_boundary_safely(4300, self.db))

        self.assertTrue(result["skipped"])
        self.assertFalse(result["qualified"])
        self.assertEqual(runner._boundary_checkpoint(self.db), 4300)

    def test_checkpoint_advances_past_invalid_boundary_on_restart(self):
        runner._save_boundary_checkpoint(4300, self.db)
        with sqlite3.connect(str(self.db)) as connection:
            row = connection.execute(
                "SELECT value FROM metadata WHERE key=?", (runner.BOUNDARY_CHECKPOINT_KEY,)
            ).fetchone()

        self.assertEqual(int(row[0]) + runner.GRANULARITY_SECONDS, 4600)

    def test_non_validation_failure_is_not_hidden(self):
        failure = AsyncMock(side_effect=RuntimeError("database unavailable"))
        with patch.object(runner, "process_boundary", failure):
            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                asyncio.run(runner._process_boundary_safely(4300, self.db))
        self.assertIsNone(runner._boundary_checkpoint(self.db))


if __name__ == "__main__":
    unittest.main()
