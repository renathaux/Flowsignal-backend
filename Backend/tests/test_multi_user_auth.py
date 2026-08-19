import os
import time

from sqlalchemy import create_engine

from services.deriv_user_connection_store import (
    consume_oauth_state,
    load_connection,
    register_oauth_state,
    save_connection,
)
from services.user_auth_service import (
    authenticate,
    create_session,
    hash_password,
    session_snapshot,
    signup,
    verify_password,
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
