# FlowSignal: Render + Neon PostgreSQL

FlowSignal remains hosted on Render. Neon replaces only the process-local
SQLite database used for users, runtime settings, the selected broker account,
and settings audit records.

## Required Render environment variables

Add these as **Secret** environment variables on the existing Render backend:

- `DATABASE_URL`: Neon pooled connection string (`-pooler` hostname), including
  `sslmode=require`.
- `MIGRATION_DATABASE_URL`: Neon direct/unpooled connection string, including
  `sslmode=require`.

Optional pool controls already have safe defaults:

- `DB_POOL_SIZE=5`
- `DB_MAX_OVERFLOW=5`
- `DB_POOL_RECYCLE_SECONDS=300`
- `DB_POOL_TIMEOUT_SECONDS=30`
- `DB_CONNECT_TIMEOUT_SECONDS=10`

Do not put either real URL in Git, `.env.example`, screenshots, chat messages,
or frontend code. PostgreSQL passwords containing reserved URL characters must
remain URL-encoded exactly as Neon provides them.

## Render commands

When Render's Root Directory is the repository root:

```text
Build Command: pip install -r Backend/requirements.txt
Start Command: bash Backend/start_render.sh
```

When Render's Root Directory is `Backend`:

```text
Build Command: pip install -r requirements.txt
Start Command: bash start_render.sh
```

The start script refuses to boot without `DATABASE_URL`, runs
`alembic upgrade head`, and then starts the existing FastAPI application.

## One-time SQLite data copy

Use a trusted terminal with the direct Neon URL in
`MIGRATION_DATABASE_URL`, install `Backend/requirements.txt`, then run from
`Backend`:

```text
python scripts/migrate_sqlite_to_neon.py --yes
```

The tool:

1. creates/updates the Neon schema with Alembic;
2. refuses a non-empty target by default;
3. copies `users`, `runtime_settings`, `news_trading_mode_audit`, and
   `auto_trade_state_audit` in one target transaction;
4. resets PostgreSQL identity sequences;
5. prints only row counts and redacted host information.

Use `--merge` only after reviewing an already-populated Neon database. It uses
`ON CONFLICT DO NOTHING` and will not overwrite existing target rows.

## Safe rollout order

1. Keep LIVE Auto OFF during migration.
2. Run the one-time SQLite data copy.
3. Verify Neon contains the expected row counts.
4. Add both secret URLs to Render.
5. update the Render start command.
6. deploy and confirm `/database-status` reports PostgreSQL, durable storage,
   a successful connection, and revision `20260807_0001`.
7. Verify Paper/LIVE Auto and News Trading Mode values in the UI.
8. Enable LIVE Auto manually only after the broker and account are verified.

Rollback is configuration-only: restore the previous Render start command and
remove `DATABASE_URL`/`MIGRATION_DATABASE_URL`. The local SQLite file is not
deleted by the migration tool.
