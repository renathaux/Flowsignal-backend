import time

from sqlalchemy import create_engine, update

from services.deriv_connection_recovery import latest_connection_id
from services.deriv_user_connection_store import (
    deriv_connections,
    disconnect_connection,
    save_connection,
)


def memory_engine():
    return create_engine("sqlite:///:memory:")


def test_latest_connection_rebind_is_user_scoped_and_active_only(monkeypatch):
    monkeypatch.setenv("FLOWSIGNAL_DERIV_TOKEN_KEY", "test-only-key-material-which-is-not-production")
    engine = memory_engine()
    expires = time.time() + 3600

    old_id = save_connection(
        "user-a", "old-token", [{"account_id": "DOT1", "currency": "USD"}],
        expires, selected_account_id="DOT1", connection_id="old-connection", engine=engine,
    )
    latest_id = save_connection(
        "user-a", "latest-token", [{"account_id": "DOT1", "currency": "USD"}],
        expires, selected_account_id="DOT1", connection_id="latest-connection", engine=engine,
    )
    save_connection(
        "user-b", "other-token", [{"account_id": "DOT2", "currency": "USD"}],
        expires, selected_account_id="DOT2", connection_id="other-connection", engine=engine,
    )

    assert latest_connection_id("user-a", engine=engine) == latest_id
    assert latest_connection_id("user-b", engine=engine) == "other-connection"

    disconnect_connection(latest_id, "user-a", engine=engine)
    assert latest_connection_id("user-a", engine=engine) == old_id

    with engine.begin() as connection:
        connection.execute(
            update(deriv_connections)
            .where(deriv_connections.c.connection_id == old_id)
            .values(expires_at=time.time() - 1)
        )
    assert latest_connection_id("user-a", engine=engine) is None
