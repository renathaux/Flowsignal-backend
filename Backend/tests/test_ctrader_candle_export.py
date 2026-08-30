import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from fastapi import HTTPException
from starlette.requests import Request

import ctrader_connector
from routes import ctrader


def _request(token="owner-token"):
    return Request({
        "type": "http",
        "method": "GET",
        "path": "/admin/ctrader/candles",
        "headers": [(b"authorization", f"Bearer {token}".encode())],
    })


def _frame():
    index = pd.to_datetime([
        "2026-08-22T00:30:00Z",
        "2026-08-22T00:00:00Z",
        "2026-08-22T00:15:00Z",
        "2026-08-22T00:15:00Z",
        "2026-08-22T00:45:00Z",
    ])
    return pd.DataFrame({
        "Open": [3.0, 1.0, 2.0, 2.1, 4.0],
        "High": [3.5, 1.5, 2.5, 2.6, 4.5],
        "Low": [2.5, 0.5, 1.5, 1.6, 3.5],
        "Close": [3.2, 1.2, 2.2, 2.3, 4.2],
        "Volume": [30, 10, 20, 21, 40],
    }, index=index)


def test_export_is_sorted_deduplicated_bounded_and_numeric():
    fetcher = Mock(return_value=_frame())
    with patch.object(ctrader, "_require_candle_export_admin"), patch.object(
        ctrader, "fetch_ctrader_historical_candles", fetcher
    ):
        result = ctrader.export_closed_ctrader_candles(
            _request(),
            symbol="XAUUSD",
            timeframe="M15",
            start=datetime(2026, 8, 22, 0, 0, tzinfo=timezone.utc),
            end=datetime(2026, 8, 22, 0, 44, tzinfo=timezone.utc),
        )

    assert result["read_only"] is True
    assert result["closed_only"] is True
    assert result["count"] == 3
    assert [row["timestamp"] for row in result["candles"]] == [
        "2026-08-22T00:00:00Z",
        "2026-08-22T00:15:00Z",
        "2026-08-22T00:30:00Z",
    ]
    assert result["candles"][1] == {
        "timestamp": "2026-08-22T00:15:00Z",
        "open": 2.1,
        "high": 2.6,
        "low": 1.6,
        "close": 2.3,
        "volume": 21.0,
    }
    fetcher.assert_called_once()


def test_export_excludes_the_current_forming_candle():
    now = datetime.now(timezone.utc)
    forming_open = now.replace(
        minute=(now.minute // 15) * 15, second=0, microsecond=0
    )
    closed_open = forming_open - timedelta(minutes=15)
    frame = pd.DataFrame({
        "Open": [1.1, 1.2],
        "High": [1.2, 1.3],
        "Low": [1.0, 1.1],
        "Close": [1.15, 1.25],
    }, index=pd.to_datetime([closed_open, forming_open], utc=True))

    with patch.object(ctrader, "_require_candle_export_admin"), patch.object(
        ctrader, "fetch_ctrader_historical_candles", return_value=frame
    ):
        result = ctrader.export_closed_ctrader_candles(
            _request(), symbol="EURUSD", timeframe="15m",
            start=closed_open,
            end=now,
        )

    assert result["count"] == 1
    assert result["candles"][0]["timestamp"] == closed_open.isoformat().replace(
        "+00:00", "Z"
    )


@pytest.mark.parametrize("symbol,timeframe", [("BTCUSD", "M15"), ("XAUUSD", "M5")])
def test_invalid_symbol_or_timeframe_is_rejected_without_fetch(symbol, timeframe):
    fetcher = Mock()
    with patch.object(ctrader, "_require_candle_export_admin"), patch.object(
        ctrader, "fetch_ctrader_historical_candles", fetcher
    ), pytest.raises(HTTPException) as exc:
        ctrader.export_closed_ctrader_candles(
            _request(), symbol=symbol, timeframe=timeframe,
            start=datetime(2026, 8, 22, tzinfo=timezone.utc),
            end=datetime(2026, 8, 23, tzinfo=timezone.utc),
        )
    assert exc.value.status_code == 422
    fetcher.assert_not_called()


def test_range_over_fourteen_days_is_rejected():
    with patch.object(ctrader, "_require_candle_export_admin"), pytest.raises(HTTPException) as exc:
        ctrader.export_closed_ctrader_candles(
            _request(), symbol="EURUSD", timeframe="15m",
            start=datetime(2026, 8, 1, tzinfo=timezone.utc),
            end=datetime(2026, 8, 16, tzinfo=timezone.utc),
        )
    assert exc.value.status_code == 422


def test_legacy_non_admin_is_rejected():
    denied = HTTPException(status_code=401, detail="AUTHENTICATION_REQUIRED")
    fake_api = SimpleNamespace(SESSIONS={"user-token": {"role": "user"}})
    with patch.object(ctrader, "require_admin", side_effect=denied), patch.dict(
        sys.modules, {"api": fake_api}
    ), pytest.raises(HTTPException) as exc:
        ctrader._require_candle_export_admin(_request("user-token"))
    assert exc.value.status_code == 403


def test_legacy_admin_is_accepted():
    denied = HTTPException(status_code=401, detail="AUTHENTICATION_REQUIRED")
    session = {"role": "admin"}
    fake_api = SimpleNamespace(SESSIONS={"owner-token": session})
    with patch.object(ctrader, "require_admin", side_effect=denied), patch.dict(
        sys.modules, {"api": fake_api}
    ):
        assert ctrader._require_candle_export_admin(_request()) is session


def test_export_route_never_calls_broker_or_strategy_mutations():
    frame = _frame().sort_index()
    forbidden = [
        "place_market_order",
        "modify_position_sltp",
        "modify_position_stop_loss",
        "close_position",
    ]
    mocks = {name: Mock() for name in forbidden}
    with patch.object(ctrader, "_require_candle_export_admin"), patch.object(
        ctrader, "fetch_ctrader_historical_candles", return_value=frame
    ), patch.multiple("ctrader_connector", **mocks):
        ctrader.export_closed_ctrader_candles(
            _request(), symbol="EURUSD", timeframe="15m",
            start=datetime(2026, 8, 22, tzinfo=timezone.utc),
            end=datetime(2026, 8, 23, tzinfo=timezone.utc),
        )
    assert all(mock.call_count == 0 for mock in mocks.values())


def test_historical_connector_uses_only_trendbar_read_request():
    socket = Mock()
    response = {"payload": {"trendbar": [{
        "utcTimestampInMinutes": 29773440,
        "low": 459000,
        "deltaOpen": 100,
        "deltaHigh": 300,
        "deltaClose": 200,
        "volume": 7,
    }]}}
    with patch.object(ctrader_connector, "get_ctrader_config", return_value={
        "env": "demo", "account_id": "123"
    }), patch.object(
        ctrader_connector, "open_ctrader_json_socket", return_value=socket
    ), patch.object(
        ctrader_connector, "authorize_ctrader_socket"
    ) as authorize, patch.object(
        ctrader_connector, "fetch_ctrader_symbol_details", return_value=[]
    ), patch.object(
        ctrader_connector, "resolve_ctrader_symbol", return_value={
            "symbol_id": 41, "digits": 2
        }
    ), patch.object(
        ctrader_connector, "send_ctrader_request", return_value=response
    ) as send:
        frame = ctrader_connector.fetch_ctrader_historical_candles(
            "XAUUSD", "15m",
            datetime(2026, 8, 22, tzinfo=timezone.utc),
            datetime(2026, 8, 23, tzinfo=timezone.utc),
        )

    authorize.assert_called_once()
    payload = send.call_args.args[2]
    assert send.call_args.args[1] == ctrader_connector.PAYLOAD_GET_TRENDBARS_REQ
    assert send.call_args.args[3] == ctrader_connector.PAYLOAD_GET_TRENDBARS_RES
    assert payload["period"] == ctrader_connector.CTRADER_TRENDBAR_PERIODS["15m"]
    assert payload["fromTimestamp"] == 1787356800000
    assert payload["toTimestamp"] == 1787443200000
    assert list(frame.columns) == ["Open", "High", "Low", "Close", "Volume"]
    assert socket.close.call_count == 1
