from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

import polars as pl

from hl_historical_client import HistoricalDataClient

_ISO_TIMESTAMP_RE = re.compile(
    r"^(?P<prefix>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?P<fraction>\.\d+)?(?P<offset>Z|[+-]\d{2}:\d{2})?$"
)
_ALLOWED_INTERVALS = "1s, 5s, 30s, 1m, 5m, 15m, 30m, 1h, 4h, 1d"


def parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    match = _ISO_TIMESTAMP_RE.match(normalized)
    if match and match.group("fraction"):
        fraction_digits = match.group("fraction")[1:]
        if len(fraction_digits) > 6:
            normalized = (
                f"{match.group('prefix')}.{fraction_digits[:6]}"
                f"{match.group('offset') or ''}"
            )

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as err:
        raise ValueError(
            f"invalid datetime '{value}'. Use ISO-8601 like 2026-01-01T00:00:00Z"
        ) from err

    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def resolve_time_range(*, start: Optional[str], end: Optional[str], hours: int) -> Tuple[datetime, datetime]:
    if (start is None) != (end is None):
        raise ValueError("provide both --start and --end, or neither")

    if start is not None and end is not None:
        start_dt = parse_datetime(start)
        end_dt = parse_datetime(end)
    else:
        if hours <= 0:
            raise ValueError("--hours must be > 0")
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=hours)

    if start_dt >= end_dt:
        raise ValueError("start must be earlier than end")

    return start_dt, end_dt


def maybe_write_frame(frame: pl.DataFrame, out: Optional[str]) -> None:
    if out is None:
        return

    suffix = Path(out).suffix.lower()
    if suffix == ".parquet":
        frame.write_parquet(out)
    elif suffix == ".csv":
        frame.write_csv(out)
    else:
        raise ValueError("output path must end with .parquet or .csv")

    print(f"Wrote {frame.height} rows to {out}")


def pick_coin(client: HistoricalDataClient, explicit_coin: Optional[str]) -> str:
    if explicit_coin:
        return explicit_coin

    coins = client.list_coins()
    if coins.is_empty():
        raise RuntimeError("no coins available in database; run backfill first")

    top_coin = coins.sort("fill_count", descending=True).row(0)[0]
    print(f"No --coin provided. Using most active coin from ListCoins: {top_coin}")
    return top_coin


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query VWAP and time bars from a local hl-historical-data server")
    parser.add_argument("--host", default="127.0.0.1", help="gRPC host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=50051, help="gRPC port (default: 50051)")

    parser.add_argument("--coin", help="coin symbol to query (default: most active coin from ListCoins)")

    parser.add_argument("--hours", type=int, default=6, help="lookback window in hours when --start/--end are not provided")
    parser.add_argument("--start", help="start timestamp (ISO-8601, e.g. 2026-01-01T00:00:00Z)")
    parser.add_argument("--end", help="end timestamp (ISO-8601, e.g. 2026-01-01T06:00:00Z)")

    parser.add_argument(
        "--vwap-interval",
        default="1m",
        help=f"VWAP interval (allowed: {_ALLOWED_INTERVALS})",
    )
    parser.add_argument(
        "--bars-interval",
        default="5m",
        help=f"time bars interval (allowed: {_ALLOWED_INTERVALS})",
    )

    parser.add_argument("--vwap-out", help="optional output path for VWAP frame (.parquet or .csv)")
    parser.add_argument("--bars-out", help="optional output path for bars frame (.parquet or .csv)")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        start_dt, end_dt = resolve_time_range(start=args.start, end=args.end, hours=args.hours)

        print(f"Connecting to {args.host}:{args.port}")
        print(f"Query window: {start_dt.isoformat()} -> {end_dt.isoformat()}")

        with HistoricalDataClient(host=args.host, port=args.port) as client:
            coin = pick_coin(client, args.coin)

            vwap = client.get_vwap(
                coin=coin,
                interval=args.vwap_interval,
                start=start_dt,
                end=end_dt,
            )
            bars = client.get_time_bars(
                coin=coin,
                interval=args.bars_interval,
                start=start_dt,
                end=end_dt,
            )

        print(f"Coin: {coin}")
        print(f"VWAP rows: {vwap.height}")
        if not vwap.is_empty():
            print(vwap.head(10))

        print(f"Bars rows: {bars.height}")
        if not bars.is_empty():
            print(bars.head(10))

        maybe_write_frame(vwap, args.vwap_out)
        maybe_write_frame(bars, args.bars_out)
    except ValueError as err:
        parser.error(str(err))
    except RuntimeError as err:
        parser.exit(status=1, message=f"error: {err}\n")


if __name__ == "__main__":
    main()
