import os
import time

from sqlalchemy import create_engine, select, update

from services.deriv_user_connection_store import (
    consume_oauth_state,
    disconnect_connection,
    load_connection,
    register_oauth_state,
    save_connection,
    set_selected,
)
from services.user_auth_service import (
    authenticate,
    change_password,
    create_session,
    hash_password,
    session_snapshot,
    signup,
    verify_password,
    sessions,
    users,
)


def memory_engine():
    return create_engine("sqlite:///:memory:")


def test_password_hash_is_salted_and_pbkdf2():
    first = hash_password("StrongPassword123!")
    second = hash_password("StrongPassword123!")
    assert first.startswith("pbkdf2_sha256$600000$")
    assert second.startswith("pbkdf2_sha256$600000$")
    assert first != second
    assert verify_password("StrongPassword123!", first)
    assert not verify_password("wrong-password", first)


def test_signup_login_and_duplicate_email():
    engine = memory_engine()
    user = signup("User@Example.com", "StrongPassword123!", engine=engine)
    assert user["email"] == "user@example.com"
    authenticated = authenticate("user@example.com", "StrongPassword123!", engine=engine)
    assert authenticated["id"] == user["id"]
    try:
        signup("USER@example.com", "AnotherPassword123!", engine=engine)
        assert False, "duplicate email should fail"
    except RuntimeError as exc:
        assert str(exc) == "EMAIL_ALREADY_REGISTERED"


def test_sessions_are_opaque_and_bound_to_user():
    engine = memory_engine()
    user = signup("a@example.com", "StrongPassword123!", engine=engine)
    with engine.begin() as connection:
        connection.execute(update(users).where(users.c.id == user["id"]).values(email_verified=True))
    token, csrf, expires = create_session(user["id"], engine=engine)
    assert user["id"] not in token
    snapshot = session_snapshot(token, engine=engine)
    assert snapshot is not None
    assert snapshot[0].id == user["id"]
    assert snapshot[1] == csrf
    assert expires > time.time()


def test_deriv_oauth_state_is_user_bound_single_use(monkeypatch):
    engine = memory_engine()
    state = "state-123"
    verifier = "v" * 64
    register_oauth_state("user-a", state, verifier, engine=engine)
    try:
        consume_oauth_state("user-b", state, verifier, engine=engine)
        assert False, "cross-user OAuth state should fail"
    except RuntimeError as exc:
        assert str(exc) == "DERIV_OAUTH_USER_MISMATCH"
    consume_oauth_state("user-a", state, verifier, engine=engine)
    try:
        consume_oauth_state("user-a", state, verifier, engine=engine)
        assert False, "OAuth state replay should fail"
    except RuntimeError as exc:
        assert str(exc) == "DERIV_OAUTH_STATE_REPLAYED"


def test_deriv_oauth_state_expires_and_wrong_verifier_does_not_consume(monkeypatch):
    engine = memory_engine()
    monkeypatch.setattr("services.deriv_user_connection_store.time.time", lambda: 1000)
    register_oauth_state("user-a", "state-expiry", "v" * 64, engine=engine)
    try:
        consume_oauth_state("user-a", "state-expiry", "x" * 64, engine=engine)
        assert False, "wrong verifier must fail"
    except RuntimeError as exc:
        assert str(exc) == "DERIV_OAUTH_VERIFIER_MISMATCH"
    monkeypatch.setattr("services.deriv_user_connection_store.time.time", lambda: 1601)
    try:
        consume_oauth_state("user-a", "state-expiry", "v" * 64, engine=engine)
        assert False, "expired state must fail"
    except RuntimeError as exc:
        assert str(exc) == "DERIV_OAUTH_STATE_EXPIRED"


def test_deriv_credentials_are_encrypted_and_user_isolated(monkeypatch):
    monkeypatch.setenv("FLOWSIGNAL_DERIV_TOKEN_KEY", "test-only-key-material-which-is-not-production")
    engine = memory_engine()
    connection_id = save_connection(
        "user-a", "secret-access-token", [{"account_id": "DOT1", "currency": "USD"}],
        time.time() + 3600, selected_account_id="DOT1", engine=engine,
    )
    loaded = load_connection(connection_id, user_id="user-a", engine=engine)
    assert loaded["access_token"] == "secret-access-token"
    try:
        load_connection(connection_id, user_id="user-b", engine=engine)
        assert False, "another user must not load the connection"
    except RuntimeError as exc:
        assert str(exc) == "DERIV_CONNECTION_USER_MISMATCH"

    set_selected(connection_id, "user-a", "DOT1", engine=engine)
    assert load_connection(connection_id, user_id="user-a", engine=engine)["selected_account_id"] == "DOT1"
    disconnect_connection(connection_id, "user-a", engine=engine)
    assert load_connection(connection_id, user_id="user-a", engine=engine) is None


def test_expired_connection_and_invalid_remote_token_are_detected(monkeypatch):
    monkeypatch.setenv("FLOWSIGNAL_DERIV_TOKEN_KEY", "test-only-key-material-which-is-not-production")
    engine = memory_engine()
    expired = save_connection("user-a", "expired", [], time.time() - 1, engine=engine)
    assert load_connection(expired, user_id="user-a", engine=engine) is None
    class Unauthorized:
        status_code = 401
        ok = False
    monkeypatch.setattr("services.deriv_service.requests.get", lambda *args, **kwargs: Unauthorized())
    from services.deriv_service import fetch_options_accounts
    try:
        fetch_options_accounts("revoked-token")
        assert False, "revoked token must fail"
    except RuntimeError as exc:
        assert str(exc) == "DERIV_TOKEN_INVALID"


def test_change_password_keeps_current_session_and_revokes_others():
    engine = memory_engine()
    user = signup("password@example.com", "StrongPassword123!", engine=engine)
    with engine.begin() as connection:
        connection.execute(update(users).where(users.c.id == user["id"]).values(email_verified=True))
    current, _csrf, _ = create_session(user["id"], engine=engine)
    other, _csrf2, _ = create_session(user["id"], engine=engine)
    result = change_password(user["id"], "StrongPassword123!", "NewStrongPassword456!", current, engine=engine)
    assert result["other_sessions_revoked"] is True
    assert session_snapshot(current, engine=engine) is not None
    assert session_snapshot(other, engine=engine) is None
    assert authenticate("password@example.com", "NewStrongPassword456!", engine=engine)["id"] == user["id"]
