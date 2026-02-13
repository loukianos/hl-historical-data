from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import sys

import polars as pl
import pytest
from google.protobuf.timestamp_pb2 import Timestamp

PACKAGE_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))

from hl_historical_client import client as client_module


class _Request:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class _FakePb2:
    SIDE_BUY = 1
    SIDE_SELL = 2

    INTERVAL_1S = 1
    INTERVAL_5S = 2
    INTERVAL_30S = 3
    INTERVAL_1M = 4
    INTERVAL_5M = 5
    INTERVAL_15M = 6
    INTERVAL_30M = 7
    INTERVAL_1H = 8
    INTERVAL_4H = 9
    INTERVAL_1D = 10

    class GetFillsRequest(_Request):
        pass

    class GetFillByHashRequest(_Request):
        pass

    class GetFillsByOidRequest(_Request):
        pass

    class GetVWAPRequest(_Request):
        pass

    class GetTimeBarsRequest(_Request):
        pass

    class GetWalletSummaryRequest(_Request):
        pass

    class ListCoinsRequest(_Request):
        pass


class _FakeMessage:
    def __init__(self, present_fields=None, **kwargs):
        self._present_fields = set(present_fields or [])
        for key, value in kwargs.items():
            setattr(self, key, value)

    def HasField(self, name: str) -> bool:
        return name in self._present_fields


def _ts(seconds: int, nanos: int = 0) -> Timestamp:
    return Timestamp(seconds=seconds, nanos=nanos)


def _fill_message() -> _FakeMessage:
    return _FakeMessage(
        present_fields={"time", "block_time", "local_time", "cloid", "builder_fee", "builder"},
        time=_ts(1_700_000_000, 100),
        block_time=_ts(1_700_000_001, 200),
        block_number=99,
        address="0xabc",
        coin="BTC",
        type="trade",
        px=100.0,
        sz=0.5,
        is_buy=True,
        start_position=0.0,
        is_gaining_inventory=False,
        closed_pnl=1.25,
        hash="0xhash",
        oid=42,
        crossed=False,
        fee=0.01,
        tid=7,
        fee_token="USDC",
        cloid="client-order",
        builder_fee="0.0001",
        builder="0xbuilder",
        local_time=_ts(1_700_000_002, 300),
    )


class _FakeStub:
    def __init__(self):
        self.last_requests = {}

    def GetFills(self, request):
        self.last_requests["GetFills"] = request
        return iter([SimpleNamespace(fill=_fill_message())])

    def GetFillByHash(self, request):
        self.last_requests["GetFillByHash"] = request
        return SimpleNamespace(fills=[_fill_message()])

    def GetFillsByOid(self, request):
        self.last_requests["GetFillsByOid"] = request
        return iter([SimpleNamespace(fill=_fill_message())])

    def GetVWAP(self, request):
        self.last_requests["GetVWAP"] = request
        point = _FakeMessage(
            present_fields={"time"},
            time=_ts(1_700_000_000, 1),
            vwap=101.0,
            volume=12.0,
            notional=1212.0,
            fill_count=3,
        )
        return iter([SimpleNamespace(point=point)])

    def GetTimeBars(self, request):
        self.last_requests["GetTimeBars"] = request
        bar = _FakeMessage(
            present_fields={"time"},
            time=_ts(1_700_000_000, 2),
            open=100.0,
            high=102.0,
            low=99.5,
            close=101.2,
            base_volume=15.0,
            notional_volume=1518.0,
            trade_count=4,
            buy_volume=8.0,
            sell_volume=7.0,
        )
        return iter([SimpleNamespace(bar=bar)])

    def GetWalletSummary(self, request):
        self.last_requests["GetWalletSummary"] = request
        per_coin = [
            _FakeMessage(
                coin="BTC",
                volume=10.0,
                notional_volume=1000.0,
                fill_count=2,
                maker_count=1,
                taker_count=1,
                maker_taker_ratio=1.0,
            )
        ]
        return _FakeMessage(
            present_fields={"first_fill_time", "last_fill_time"},
            fill_count=2,
            unique_coins=1,
            total_volume=10.0,
            total_notional=1000.0,
            maker_count=1,
            taker_count=1,
            maker_taker_ratio=1.0,
            first_fill_time=_ts(1_700_000_000, 0),
            last_fill_time=_ts(1_700_000_100, 0),
            per_coin=per_coin,
        )

    def ListCoins(self, request):
        self.last_requests["ListCoins"] = request
        coins = [SimpleNamespace(coin="BTC", fill_count=10)]
        return SimpleNamespace(coins=coins)


def _make_client() -> tuple[client_module.HistoricalDataClient, _FakeStub]:
    fake_stub = _FakeStub()
    client = client_module.HistoricalDataClient()
    client._pb2 = _FakePb2
    client._stub = fake_stub
    return client, fake_stub


def _assert_datetime_dtype(schema: dict[str, pl.DataType], column: str) -> None:
    assert schema[column] == pl.Datetime("ns", time_zone="UTC")


def test_parse_helpers_and_utc_timestamp_conversion():
    assert client_module._parse_interval("1m", _FakePb2) == _FakePb2.INTERVAL_1M
    assert client_module._parse_side("BUY", _FakePb2) == _FakePb2.SIDE_BUY

    with pytest.raises(ValueError):
        client_module._parse_interval("2m", _FakePb2)
    with pytest.raises(ValueError):
        client_module._parse_side("other", _FakePb2)

    pacific = timezone(timedelta(hours=-8))
    dt = datetime(2025, 1, 1, 12, 0, 0, 123456, tzinfo=pacific)
    ts = client_module._datetime_to_timestamp(dt)
    assert ts.seconds == 1_735_761_600
    assert ts.nanos == 123_456_000


def test_empty_dataframe_schemas_are_stable():
    fills = client_module._df_from_rows(
        [], client_module._FILLS_SCHEMA, ts_columns=client_module._FILLS_TS_COLUMNS
    )
    assert fills.columns == list(client_module._FILLS_SCHEMA.keys())
    _assert_datetime_dtype(fills.schema, "time")

    vwap = client_module._df_from_rows(
        [], client_module._VWAP_SCHEMA, ts_columns=client_module._VWAP_TS_COLUMNS
    )
    assert vwap.columns == list(client_module._VWAP_SCHEMA.keys())
    _assert_datetime_dtype(vwap.schema, "time")

    bars = client_module._df_from_rows(
        [], client_module._TIME_BARS_SCHEMA, ts_columns=client_module._TIME_BARS_TS_COLUMNS
    )
    assert bars.columns == list(client_module._TIME_BARS_SCHEMA.keys())
    _assert_datetime_dtype(bars.schema, "time")


def test_get_fills_stream_returns_dataframe_and_maps_request_fields():
    client, fake_stub = _make_client()

    frame = client.get_fills(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 1, 0, 0),
        coin=" BTC ",
        wallet=" 0xabc ",
        side="sell",
        crossed_only=True,
    )

    req = fake_stub.last_requests["GetFills"]
    assert req.coin == "BTC"
    assert req.wallet == "0xabc"
    assert req.side == _FakePb2.SIDE_SELL
    assert req.crossed_only is True

    assert frame.columns == list(client_module._FILLS_SCHEMA.keys())
    _assert_datetime_dtype(frame.schema, "time")
    assert frame.height == 1


def test_other_query_methods_return_expected_dataframe_shapes():
    client, fake_stub = _make_client()

    hash_df = client.get_fill_by_hash(" 0xhash ")
    oid_df = client.get_fills_by_oid(42)
    vwap_df = client.get_vwap(
        coin="BTC",
        interval="1m",
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 1, 0, 0),
    )
    bars_df = client.get_time_bars(
        coin="BTC",
        interval="5m",
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 1, 0, 0),
    )
    coins_df = client.list_coins()

    assert hash_df.columns == list(client_module._FILLS_SCHEMA.keys())
    assert oid_df.columns == list(client_module._FILLS_SCHEMA.keys())
    assert vwap_df.columns == list(client_module._VWAP_SCHEMA.keys())
    assert bars_df.columns == list(client_module._TIME_BARS_SCHEMA.keys())
    assert coins_df.columns == list(client_module._LIST_COINS_SCHEMA.keys())

    _assert_datetime_dtype(vwap_df.schema, "time")
    _assert_datetime_dtype(bars_df.schema, "time")

    assert fake_stub.last_requests["GetFillByHash"].hash == "0xhash"


def test_get_wallet_summary_returns_summary_and_per_coin_dataframes():
    client, _ = _make_client()

    result = client.get_wallet_summary(
        wallet=" 0xabc ",
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 1, 0, 0),
    )

    assert set(result.keys()) == {"summary", "per_coin"}
    summary = result["summary"]
    per_coin = result["per_coin"]

    assert summary.columns == list(client_module._WALLET_SUMMARY_SCHEMA.keys())
    assert per_coin.columns == list(client_module._PER_COIN_SCHEMA.keys())
    _assert_datetime_dtype(summary.schema, "first_fill_time")
    _assert_datetime_dtype(summary.schema, "last_fill_time")
