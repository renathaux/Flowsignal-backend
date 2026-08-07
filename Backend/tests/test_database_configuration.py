import ast
import sqlite3
import unittest
from pathlib import Path

from sqlalchemy import inspect

import db
from models import Base


BACKEND_DIR = Path(__file__).resolve().parents[1]


class DatabaseConfigurationTests(unittest.TestCase):
    def test_render_postgres_url_is_normalized_for_sqlalchemy(self):
        self.assertEqual(
            db.normalize_database_url("postgres://user:pass@example/db"),
            "postgresql://user:pass@example/db",
        )

    def test_database_url_redaction_removes_password_and_query(self):
        redacted = db.redact_database_url(
            "postgresql://flow:secret@example.neon.tech/db?sslmode=require"
        )
        self.assertNotIn("secret", redacted)
        self.assertNotIn("sslmode", redacted)
        self.assertIn("flow:***@example.neon.tech", redacted)

    def test_models_match_existing_sqlite_tables(self):
        expected = {
            "users",
            "runtime_settings",
            "news_trading_mode_audit",
            "auto_trade_state_audit",
        }
        self.assertEqual(set(Base.metadata.tables), expected)
        self.assertTrue(expected.issubset(set(inspect(db.engine).get_table_names())))

    def test_migration_revision_and_tool_are_valid_python(self):
        files = [
            BACKEND_DIR / "migrations" / "env.py",
            BACKEND_DIR / "migrations" / "versions" / "20260807_0001_initial_schema.py",
            BACKEND_DIR / "scripts" / "migrate_sqlite_to_neon.py",
        ]
        for path in files:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    def test_initial_revision_contains_every_model_table(self):
        source = (
            BACKEND_DIR
            / "migrations"
            / "versions"
            / "20260807_0001_initial_schema.py"
        ).read_text(encoding="utf-8")
        for table_name in Base.metadata.tables:
            self.assertIn(f'"{table_name}"', source)

    def test_services_do_not_create_schema_at_import_time(self):
        for relative in (
            "services/auto_trade_state_service.py",
            "services/news_mode_service.py",
            "services/broker_account_state_service.py",
        ):
            source = (BACKEND_DIR / relative).read_text(encoding="utf-8")
            self.assertNotIn("metadata.create_all", source)

    def test_local_sqlite_source_is_readable_for_one_time_copy(self):
        connection = sqlite3.connect(BACKEND_DIR / "database" / "flowsignal.db")
        try:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        finally:
            connection.close()
        self.assertTrue(set(Base.metadata.tables).issubset(tables))


if __name__ == "__main__":
    unittest.main()
