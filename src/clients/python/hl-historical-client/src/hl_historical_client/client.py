"""Polars-native gRPC client for the Hyperliquid historical data service."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import grpc
import polars as pl
from google.protobuf.timestamp_pb2 import Timestamp

_DATETIME_DTYPE = pl.Datetime("ns", time_zone="UTC")
_ALLOWED_INTERVALS = ("1s", "5s", "30s", "1m", "5m", "15m", "30m", "1h", "4h", "1d")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_FILLS_SCHEMA: dict[str, pl.DataType] = {
    "time": pl.Int64,
    "block_time": pl.Int64,
    "block_number": pl.Int64,
    "address": pl.Utf8,
    "coin": pl.Utf8,
    "type": pl.Utf8,
    "px": pl.Float64,
    "sz": pl.Float64,
    "is_buy": pl.Boolean,
    "start_position": pl.Float64,
    "is_gaining_inventory": pl.Boolean,
    "closed_pnl": pl.Float64,
    "hash": pl.Utf8,
    "oid": pl.Int64,
    "crossed": pl.Boolean,
    "fee": pl.Float64,
    "tid": pl.Int64,
    "fee_token": pl.Utf8,
    "cloid": pl.Utf8,
    "builder_fee": pl.Utf8,
    "builder": pl.Utf8,
    "local_time": pl.Int64,
}
_FILLS_TS_COLUMNS = ("time", "block_time", "local_time")

_VWAP_SCHEMA: dict[str, pl.DataType] = {
    "time": pl.Int64,
    "vwap": pl.Float64,
    "volume": pl.Float64,
    "notional": pl.Float64,
    "fill_count": pl.Int64,
}
_VWAP_TS_COLUMNS = ("time",)

_TIME_BARS_SCHEMA: dict[str, pl.DataType] = {
    "time": pl.Int64,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "base_volume": pl.Float64,
    "notional_volume": pl.Float64,
    "trade_count": pl.Int64,
    "buy_volume": pl.Float64,
    "sell_volume": pl.Float64,
}
_TIME_BARS_TS_COLUMNS = ("time",)

_WALLET_SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "fill_count": pl.Int64,
    "unique_coins": pl.Int64,
    "total_volume": pl.Float64,
    "total_notional": pl.Float64,
    "maker_count": pl.Int64,
    "taker_count": pl.Int64,
    "maker_taker_ratio": pl.Float64,
    "first_fill_time": pl.Int64,
    "last_fill_time": pl.Int64,
}
_WALLET_SUMMARY_TS_COLUMNS = ("first_fill_time", "last_fill_time")

_PER_COIN_SCHEMA: dict[str, pl.DataType] = {
    "coin": pl.Utf8,
    "volume": pl.Float64,
    "notional_volume": pl.Float64,
    "fill_count": pl.Int64,
    "maker_count": pl.Int64,
    "taker_count": pl.Int64,
    "maker_taker_ratio": pl.Float64,
}

_LIST_COINS_SCHEMA: dict[str, pl.DataType] = {
    "coin": pl.Utf8,
    "fill_count": pl.Int64,
}


class HistoricalDataClientError(RuntimeError):
    """Raised when a gRPC query fails."""


def _normalize_required_str(value: str, field: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"{field} cannot be empty")
    return trimmed


def _normalize_optional_str(value: Optional[str], field: str) -> Optional[str]:
    if value is None:
        return None

    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"{field} cannot be empty when provided")
    return trimmed


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _validate_time_range(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    start_utc = _as_utc(start)
    end_utc = _as_utc(end)
    if start_utc >= end_utc:
        raise ValueError("start must be strictly earlier than end")
    return start_utc, end_utc


def _datetime_to_timestamp(value: datetime) -> Timestamp:
    utc_value = _as_utc(value)
    delta = utc_value - _EPOCH
    seconds = (delta.days * 86_400) + delta.seconds

    timestamp = Timestamp()
    timestamp.seconds = seconds
    timestamp.nanos = delta.microseconds * 1_000
    return timestamp


def _timestamp_range(start: datetime, end: datetime) -> tuple[Timestamp, Timestamp]:
    start_utc, end_utc = _validate_time_range(start, end)
    start_ts = _datetime_to_timestamp(start_utc)
    end_ts = _datetime_to_timestamp(end_utc)

    if start_ts.seconds == 0 and start_ts.nanos == 0:
        raise ValueError("start must be non-zero")
    if end_ts.seconds == 0 and end_ts.nanos == 0:
        raise ValueError("end must be non-zero")

    return start_ts, end_ts


def _parse_side(side: Optional[str], pb2: Any) -> Optional[int]:
    if side is None:
        return None

    normalized = _normalize_required_str(side, "side").lower()
    mapping = {
        "buy": pb2.SIDE_BUY,
        "sell": pb2.SIDE_SELL,
    }

    if normalized not in mapping:
        raise ValueError("side must be one of: buy, sell")
    return mapping[normalized]


def _parse_interval(interval: str, pb2: Any) -> int:
    normalized = _normalize_required_str(interval, "interval").lower()
    mapping = {
        "1s": pb2.INTERVAL_1S,
        "5s": pb2.INTERVAL_5S,
        "30s": pb2.INTERVAL_30S,
        "1m": pb2.INTERVAL_1M,
        "5m": pb2.INTERVAL_5M,
        "15m": pb2.INTERVAL_15M,
        "30m": pb2.INTERVAL_30M,
        "1h": pb2.INTERVAL_1H,
        "4h": pb2.INTERVAL_4H,
        "1d": pb2.INTERVAL_1D,
    }

    if normalized not in mapping:
        allowed = ", ".join(_ALLOWED_INTERVALS)
        raise ValueError(f"interval must be one of: {allowed}")
    return mapping[normalized]


def _field_is_present(message: Any, field: str) -> bool:
    has_field = getattr(message, "HasField", None)
    if callable(has_field):
        try:
            return bool(has_field(field))
        except ValueError:
            return False

    return getattr(message, field, None) is not None


def _ts_to_ns(timestamp: Timestamp) -> int:
    return (timestamp.seconds * 1_000_000_000) + timestamp.nanos


def _maybe_ts_to_ns(message: Any, field: str) -> Optional[int]:
    timestamp = getattr(message, field, None)
    if timestamp is None:
        return None

    if not _field_is_present(message, field):
        return None

    return _ts_to_ns(timestamp)


def _maybe_opt_str(message: Any, field: str) -> Optional[str]:
    if not _field_is_present(message, field):
        return None
    return getattr(message, field)


def _fill_to_row(fill: Any) -> dict[str, object]:
    return {
        "time": _maybe_ts_to_ns(fill, "time"),
        "block_time": _maybe_ts_to_ns(fill, "block_time"),
        "block_number": fill.block_number,
        "address": fill.address,
        "coin": fill.coin,
        "type": fill.type,
        "px": fill.px,
        "sz": fill.sz,
        "is_buy": fill.is_buy,
        "start_position": fill.start_position,
        "is_gaining_inventory": fill.is_gaining_inventory,
        "closed_pnl": fill.closed_pnl,
        "hash": fill.hash,
        "oid": fill.oid,
        "crossed": fill.crossed,
        "fee": fill.fee,
        "tid": fill.tid,
        "fee_token": fill.fee_token,
        "cloid": _maybe_opt_str(fill, "cloid"),
        "builder_fee": _maybe_opt_str(fill, "builder_fee"),
        "builder": _maybe_opt_str(fill, "builder"),
        "local_time": _maybe_ts_to_ns(fill, "local_time"),
    }


def _vwap_point_to_row(point: Any) -> dict[str, object]:
    return {
        "time": _maybe_ts_to_ns(point, "time"),
        "vwap": point.vwap,
        "volume": point.volume,
        "notional": point.notional,
        "fill_count": point.fill_count,
    }


def _time_bar_to_row(bar: Any) -> dict[str, object]:
    return {
        "time": _maybe_ts_to_ns(bar, "time"),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "base_volume": bar.base_volume,
        "notional_volume": bar.notional_volume,
        "trade_count": bar.trade_count,
        "buy_volume": bar.buy_volume,
        "sell_volume": bar.sell_volume,
    }


def _wallet_summary_to_row(response: Any) -> dict[str, object]:
    return {
        "fill_count": response.fill_count,
        "unique_coins": response.unique_coins,
        "total_volume": response.total_volume,
        "total_notional": response.total_notional,
        "maker_count": response.maker_count,
        "taker_count": response.taker_count,
        "maker_taker_ratio": response.maker_taker_ratio,
        "first_fill_time": _maybe_ts_to_ns(response, "first_fill_time"),
        "last_fill_time": _maybe_ts_to_ns(response, "last_fill_time"),
    }


def _coin_summary_to_row(summary: Any) -> dict[str, object]:
    return {
        "coin": summary.coin,
        "volume": summary.volume,
        "notional_volume": summary.notional_volume,
        "fill_count": summary.fill_count,
        "maker_count": summary.maker_count,
        "taker_count": summary.taker_count,
        "maker_taker_ratio": summary.maker_taker_ratio,
    }


def _coin_info_to_row(info: Any) -> dict[str, object]:
    return {
        "coin": info.coin,
        "fill_count": info.fill_count,
    }


def _df_from_rows(
    rows: Iterable[dict[str, object]],
    schema: dict[str, pl.DataType],
    *,
    ts_columns: tuple[str, ...] = (),
) -> pl.DataFrame:
    frame = pl.DataFrame(list(rows), schema=schema)
    if not ts_columns:
        return frame

    casts = [pl.col(column).cast(_DATETIME_DTYPE) for column in ts_columns]
    return frame.with_columns(casts)


class HistoricalDataClient:
    """Client for querying the hl-historical-data gRPC service.

    All query methods return Polars DataFrames.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 50051):
        self._address = f"{host}:{port}"
        self._channel: Optional[grpc.Channel] = None
        self._stub: Optional[Any] = None
        self._pb2: Optional[Any] = None
        self._pb2_grpc: Optional[Any] = None

    def connect(self) -> "HistoricalDataClient":
        if self._stub is not None and self._pb2 is not None:
            return self

        try:
            from hl_historical_client.proto import hl_historical_pb2, hl_historical_pb2_grpc
        except ImportError as err:  # pragma: no cover - depends on local install/setup
            raise RuntimeError(
                "Generated gRPC stubs are unavailable. "
                "Run `pip install -e src/clients/python/hl-historical-client` "
                "or `make proto-python` first."
            ) from err

        self._channel = grpc.insecure_channel(self._address)
        self._pb2 = hl_historical_pb2
        self._pb2_grpc = hl_historical_pb2_grpc
        self._stub = hl_historical_pb2_grpc.HistoricalDataServiceStub(self._channel)
        return self

    def close(self) -> None:
        if self._channel:
            self._channel.close()
        self._channel = None
        self._stub = None
        self._pb2 = None
        self._pb2_grpc = None

    def __enter__(self) -> "HistoricalDataClient":
        return self.connect()

    def __exit__(self, *args) -> None:
        self.close()

    def _ensure_connected(self) -> tuple[Any, Any]:
        if self._stub is None or self._pb2 is None:
            self.connect()

        assert self._stub is not None
        assert self._pb2 is not None
        return self._stub, self._pb2

    def _raise_rpc_error(self, operation: str, err: grpc.RpcError) -> None:
        try:
            code = err.code()
        except Exception:  # pragma: no cover - defensive
            code = "UNKNOWN"

        try:
            details = err.details()
        except Exception:  # pragma: no cover - defensive
            details = str(err)

        raise HistoricalDataClientError(f"{operation} failed ({code}): {details}") from err

    def get_fills(
        self,
        start: datetime,
        end: datetime,
        coin: Optional[str] = None,
        wallet: Optional[str] = None,
        side: Optional[str] = None,
        crossed_only: bool = False,
    ) -> pl.DataFrame:
        """Stream fills matching filters into a Polars DataFrame."""
        stub, pb2 = self._ensure_connected()

        start_ts, end_ts = _timestamp_range(start, end)
        parsed_coin = _normalize_optional_str(coin, "coin")
        parsed_wallet = _normalize_optional_str(wallet, "wallet")
        parsed_side = _parse_side(side, pb2)

        request = pb2.GetFillsRequest(start_time=start_ts, end_time=end_ts)
        if parsed_coin is not None:
            request.coin = parsed_coin
        if parsed_wallet is not None:
            request.wallet = parsed_wallet
        if parsed_side is not None:
            request.side = parsed_side
        if crossed_only:
            request.crossed_only = True

        rows: list[dict[str, object]] = []
        try:
            for response in stub.GetFills(request):
                rows.append(_fill_to_row(response.fill))
        except grpc.RpcError as err:
            self._raise_rpc_error("GetFills", err)

        return _df_from_rows(rows, _FILLS_SCHEMA, ts_columns=_FILLS_TS_COLUMNS)

    def get_fill_by_hash(self, hash: str) -> pl.DataFrame:
        """Look up fills by transaction hash."""
        stub, pb2 = self._ensure_connected()

        request = pb2.GetFillByHashRequest(hash=_normalize_required_str(hash, "hash"))

        try:
            response = stub.GetFillByHash(request)
        except grpc.RpcError as err:
            self._raise_rpc_error("GetFillByHash", err)

        rows = [_fill_to_row(fill) for fill in response.fills]
        return _df_from_rows(rows, _FILLS_SCHEMA, ts_columns=_FILLS_TS_COLUMNS)

    def get_fills_by_oid(self, oid: int) -> pl.DataFrame:
        """Look up fills by order ID."""
        stub, pb2 = self._ensure_connected()

        if oid <= 0:
            raise ValueError("oid must be a positive integer")

        request = pb2.GetFillsByOidRequest(oid=oid)

        rows: list[dict[str, object]] = []
        try:
            for response in stub.GetFillsByOid(request):
                rows.append(_fill_to_row(response.fill))
        except grpc.RpcError as err:
            self._raise_rpc_error("GetFillsByOid", err)

        return _df_from_rows(rows, _FILLS_SCHEMA, ts_columns=_FILLS_TS_COLUMNS)

    def get_vwap(
        self,
        coin: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> pl.DataFrame:
        """Get VWAP data points."""
        stub, pb2 = self._ensure_connected()

        start_ts, end_ts = _timestamp_range(start, end)
        parsed_coin = _normalize_required_str(coin, "coin")
        parsed_interval = _parse_interval(interval, pb2)

        request = pb2.GetVWAPRequest(
            coin=parsed_coin,
            interval=parsed_interval,
            start_time=start_ts,
            end_time=end_ts,
        )

        rows: list[dict[str, object]] = []
        try:
            for response in stub.GetVWAP(request):
                rows.append(_vwap_point_to_row(response.point))
        except grpc.RpcError as err:
            self._raise_rpc_error("GetVWAP", err)

        return _df_from_rows(rows, _VWAP_SCHEMA, ts_columns=_VWAP_TS_COLUMNS)

    def get_time_bars(
        self,
        coin: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> pl.DataFrame:
        """Get OHLCV bars with metadata."""
        stub, pb2 = self._ensure_connected()

        start_ts, end_ts = _timestamp_range(start, end)
        parsed_coin = _normalize_required_str(coin, "coin")
        parsed_interval = _parse_interval(interval, pb2)

        request = pb2.GetTimeBarsRequest(
            coin=parsed_coin,
            interval=parsed_interval,
            start_time=start_ts,
            end_time=end_ts,
        )

        rows: list[dict[str, object]] = []
        try:
            for response in stub.GetTimeBars(request):
                rows.append(_time_bar_to_row(response.bar))
        except grpc.RpcError as err:
            self._raise_rpc_error("GetTimeBars", err)

        return _df_from_rows(rows, _TIME_BARS_SCHEMA, ts_columns=_TIME_BARS_TS_COLUMNS)

    def get_wallet_summary(
        self,
        wallet: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, pl.DataFrame]:
        """Get wallet activity summary.

        Returns a dict with 'summary' and 'per_coin' DataFrames.
        """
        stub, pb2 = self._ensure_connected()

        start_ts, end_ts = _timestamp_range(start, end)
        request = pb2.GetWalletSummaryRequest(
            wallet=_normalize_required_str(wallet, "wallet"),
            start_time=start_ts,
            end_time=end_ts,
        )

        try:
            response = stub.GetWalletSummary(request)
        except grpc.RpcError as err:
            self._raise_rpc_error("GetWalletSummary", err)

        summary_df = _df_from_rows(
            [_wallet_summary_to_row(response)],
            _WALLET_SUMMARY_SCHEMA,
            ts_columns=_WALLET_SUMMARY_TS_COLUMNS,
        )
        per_coin_df = _df_from_rows(
            [_coin_summary_to_row(summary) for summary in response.per_coin],
            _PER_COIN_SCHEMA,
        )

        return {"summary": summary_df, "per_coin": per_coin_df}

    def list_coins(self) -> pl.DataFrame:
        """List all coins with fill counts."""
        stub, pb2 = self._ensure_connected()

        request = pb2.ListCoinsRequest()

        try:
            response = stub.ListCoins(request)
        except grpc.RpcError as err:
            self._raise_rpc_error("ListCoins", err)

        rows = [_coin_info_to_row(info) for info in response.coins]
        return _df_from_rows(rows, _LIST_COINS_SCHEMA)
