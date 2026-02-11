"""Polars-native gRPC client for the Hyperliquid historical data service."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import grpc
import polars as pl


class HistoricalDataClient:
    """Client for querying the hl-historical-data gRPC service.

    All query methods return Polars DataFrames.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 50051):
        self._address = f"{host}:{port}"
        self._channel: Optional[grpc.Channel] = None

    def connect(self) -> "HistoricalDataClient":
        self._channel = grpc.insecure_channel(self._address)
        # TODO: create stubs from generated proto code
        return self

    def close(self) -> None:
        if self._channel:
            self._channel.close()
            self._channel = None

    def __enter__(self) -> "HistoricalDataClient":
        return self.connect()

    def __exit__(self, *args) -> None:
        self.close()

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
        # TODO: implement gRPC streaming call -> DataFrame
        raise NotImplementedError

    def get_fill_by_hash(self, hash: str) -> pl.DataFrame:
        """Look up fills by transaction hash."""
        # TODO: implement
        raise NotImplementedError

    def get_fills_by_oid(self, oid: int) -> pl.DataFrame:
        """Look up fills by order ID."""
        # TODO: implement
        raise NotImplementedError

    def get_vwap(
        self,
        coin: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> pl.DataFrame:
        """Get VWAP data points."""
        # TODO: implement
        raise NotImplementedError

    def get_time_bars(
        self,
        coin: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> pl.DataFrame:
        """Get OHLCV bars with metadata."""
        # TODO: implement
        raise NotImplementedError

    def get_wallet_summary(
        self,
        wallet: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, pl.DataFrame]:
        """Get wallet activity summary.

        Returns a dict with 'summary' and 'per_coin' DataFrames.
        """
        # TODO: implement
        raise NotImplementedError

    def list_coins(self) -> pl.DataFrame:
        """List all coins with fill counts."""
        # TODO: implement
        raise NotImplementedError
