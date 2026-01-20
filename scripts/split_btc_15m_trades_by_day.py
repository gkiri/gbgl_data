#!/usr/bin/env python3
import argparse
import gzip
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import ijson


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Split BTC 15m event trades into per-day JSON files using a "
            "streaming parser."
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to gabagool_history_btc_30d.json",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write per-day JSON files.",
    )
    parser.add_argument(
        "--event-substring",
        default="-15m-",
        help="Substring that identifies 15m events in the slug.",
    )
    parser.add_argument(
        "--no-gzip",
        dest="gzip",
        action="store_false",
        help="Write daily files as uncompressed .json (default writes .json.gz).",
    )
    return parser.parse_args()


def get_slug(trade: dict) -> str:
    for key in ("slug", "eventSlug", "_market_slug"):
        value = trade.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def get_event_ts(trade: dict, slug: str) -> Optional[int]:
    number_types = (int, float, Decimal)
    ts = trade.get("_market_ts")
    if isinstance(ts, number_types):
        return int(ts)

    match = re.search(r"-(\d{10})$", slug)
    if match:
        return int(match.group(1))

    ts = trade.get("timestamp")
    if isinstance(ts, number_types):
        return int(ts)

    return None


def utc_date(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def open_writer(output_dir: str, date_key: str, gzip_enabled: bool):
    os.makedirs(output_dir, exist_ok=True)
    extension = "json.gz" if gzip_enabled else "json"
    path = os.path.join(output_dir, f"btc_15m_trades_{date_key}.{extension}")
    if gzip_enabled:
        handle = gzip.open(path, "wt", encoding="utf-8")
    else:
        handle = open(path, "w", encoding="utf-8")
    handle.write(
        json.dumps(
            {
                "date": date_key,
                "timezone": "UTC",
                "event_type": "btc-15m",
                "trades": [],
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )[:-2]
    )
    return handle, path


def main() -> None:
    args = parse_args()
    input_path = args.input
    output_dir = args.output_dir

    writers: Dict[str, Dict[str, Any]] = {}
    counts = defaultdict(int)
    total = 0

    def json_default(value):
        if isinstance(value, Decimal):
            if value == value.to_integral_value():
                return int(value)
            return float(value)
        raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")

    with open(input_path, "rb") as input_file:
        for trade in ijson.items(input_file, "trades.item"):
            slug = get_slug(trade)
            if args.event_substring not in slug:
                continue

            ts = get_event_ts(trade, slug)
            if ts is None:
                continue

            date_key = utc_date(ts)

            if date_key not in writers:
                handle, path = open_writer(output_dir, date_key, args.gzip)
                writers[date_key] = {
                    "handle": handle,
                    "path": path,
                    "first": True,
                }

            writer = writers[date_key]
            if not writer["first"]:
                writer["handle"].write(",")
            else:
                writer["first"] = False

            json.dump(
                trade,
                writer["handle"],
                ensure_ascii=True,
                separators=(",", ":"),
                default=json_default,
            )
            counts[date_key] += 1
            total += 1

    for writer in writers.values():
        writer["handle"].write("]}")
        writer["handle"].close()

    summary_path = os.path.join(output_dir, "btc_15m_trades_summary.json")
    with open(summary_path, "w", encoding="utf-8") as summary_file:
        summary = {
            "total_trades": total,
            "compressed": args.gzip,
            "daily_extension": "json.gz" if args.gzip else "json",
            "dates": dict(sorted(counts.items())),
        }
        json.dump(summary, summary_file, ensure_ascii=True, indent=2)

    extension_label = ".json.gz" if args.gzip else ".json"
    print(f"Wrote {len(writers)} daily files ({extension_label}) to {output_dir}")
    print(f"Summary: {summary_path}")


if __name__ == "__main__":
    main()

