#!/usr/bin/env python3
import argparse
import gzip
import json
import os
import re
from collections import defaultdict, OrderedDict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

import ijson


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Split wallet trade dump into per-day folders, with one file per eventSlug "
            "(streaming; supports .json or .json.gz input)."
        )
    )
    parser.add_argument("--input", required=True, help="Path to the scraped JSON (.json or .json.gz). Must contain top-level key: trades")
    parser.add_argument("--output-dir", required=True, help="Directory to write output folders/files.")
    parser.add_argument(
        "--no-gzip",
        dest="gzip",
        action="store_false",
        help="Write output as uncompressed .json (default writes .json.gz).",
    )
    parser.add_argument(
        "--max-open-files",
        dest="max_open",
        type=int,
        default=64,
        help="Max number of open output writers (LRU cache). Prevents 'too many open files'. Default: 64",
    )
    parser.add_argument(
        "--day-source",
        choices=["trade_timestamp", "slug_date"],
        default="trade_timestamp",
        help=(
            "How to decide the day folder. "
            "trade_timestamp = use trade['timestamp'] (recommended). "
            "slug_date = try YYYY-MM-DD from eventSlug/slug first, fallback to timestamp."
        ),
    )
    return parser.parse_args()


# ----------------------------
# Helpers
# ----------------------------
def json_default(value):
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def open_input(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rb")
    return open(path, "rb")


def utc_date_from_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def safe_slug(s: str, max_len: int = 140) -> str:
    s = (s or "").strip()
    if not s:
        return "unknown"
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    if len(s) > max_len:
        s = f"{s[:90]}__{s[-40:]}"
    return s or "unknown"


def get_event_slug(trade: dict) -> str:
    for key in ("eventSlug", "_event_slug", "event_slug"):
        v = trade.get(key)
        if isinstance(v, str) and v:
            return v
    return ""


def get_market_slug(trade: dict) -> str:
    for key in ("slug", "_market_slug", "marketSlug", "market_slug"):
        v = trade.get(key)
        if isinstance(v, str) and v:
            return v
    cid = trade.get("conditionId") or trade.get("_condition_id") or trade.get("condition_id")
    if isinstance(cid, str) and cid:
        return f"condition_{cid}"
    return ""


def get_trade_ts(trade: dict) -> Optional[int]:
    number_types = (int, float, Decimal)
    ts = trade.get("timestamp")
    if isinstance(ts, number_types):
        return int(ts)
    return None


DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")


def date_from_slug_or_ts(trade: dict) -> Optional[str]:
    """
    Try to extract YYYY-MM-DD from eventSlug or slug (common in sports markets),
    otherwise fallback to trade timestamp.
    """
    for key in ("eventSlug", "_event_slug", "slug", "_market_slug"):
        v = trade.get(key)
        if isinstance(v, str) and v:
            m = DATE_RE.search(v)
            if m:
                return m.group(1)
    ts = get_trade_ts(trade)
    if ts is None:
        return None
    return utc_date_from_ts(ts)


def pick_day_key(trade: dict, mode: str) -> Optional[str]:
    if mode == "slug_date":
        return date_from_slug_or_ts(trade)
    # default: trade timestamp (UTC)
    ts = get_trade_ts(trade)
    if ts is None:
        return None
    return utc_date_from_ts(ts)


# ----------------------------
# Writer Manager (LRU)
# Layout: output_dir/YYYY-MM-DD/<eventSlug>/trades.json(.gz)
# ----------------------------
class WriterManager:
    def __init__(self, output_dir: str, gzip_enabled: bool, max_open: int):
        self.output_dir = output_dir
        self.gzip_enabled = gzip_enabled
        self.max_open = max_open
        self._writers: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    def _make_path(self, day_key: str, event_slug: str) -> str:
        ext = "json.gz" if self.gzip_enabled else "json"
        day_dir = os.path.join(self.output_dir, day_key)
        event_dir = os.path.join(day_dir, safe_slug(event_slug or "unknown_event"))
        os.makedirs(event_dir, exist_ok=True)
        return os.path.join(event_dir, f"trades.{ext}")

    def _write_header(self, handle, day_key: str, event_slug: str):
        header = {
            "date": day_key,
            "timezone": "UTC",
            "event_slug": event_slug or None,
            "trades": [],
        }
        dumped = json.dumps(header, ensure_ascii=True, separators=(",", ":"))
        handle.write(dumped[:-2])  # turns ... "trades":[] into ... "trades":[

    def _open_writer(self, path: str, day_key: str, event_slug: str) -> Dict[str, Any]:
        if self.gzip_enabled:
            handle = gzip.open(path, "wt", encoding="utf-8")
        else:
            handle = open(path, "w", encoding="utf-8")

        self._write_header(handle, day_key, event_slug)
        return {"handle": handle, "path": path, "first": True, "day": day_key, "event": event_slug}

    def get(self, day_key: str, event_slug: str) -> Dict[str, Any]:
        path = self._make_path(day_key, event_slug)

        if path in self._writers:
            self._writers.move_to_end(path)
            return self._writers[path]

        while len(self._writers) >= self.max_open:
            _, victim = self._writers.popitem(last=False)
            self.close_one(victim)

        writer = self._open_writer(path, day_key, event_slug)
        self._writers[path] = writer
        return writer

    def close_one(self, writer: Dict[str, Any]):
        try:
            writer["handle"].write("]}")
        finally:
            writer["handle"].close()

    def close_all(self):
        for _, writer in list(self._writers.items()):
            self.close_one(writer)
        self._writers.clear()


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    wm = WriterManager(args.output_dir, args.gzip, args.max_open)

    # counts[day_key][event_slug] = count
    counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    total_written = 0
    skipped_no_day = 0

    with open_input(args.input) as input_file:
        for trade in ijson.items(input_file, "trades.item"):
            # We only want trades (your dataset is already type=TRADE, but keep this safe)
            ttype = trade.get("type")
            if isinstance(ttype, str) and ttype and ttype.upper() != "TRADE":
                continue

            day_key = pick_day_key(trade, args.day_source)
            if not day_key:
                skipped_no_day += 1
                continue

            event_slug = get_event_slug(trade) or "unknown_event"

            writer = wm.get(day_key, event_slug)
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

            counts[day_key][event_slug] += 1
            total_written += 1

    wm.close_all()

    # Summary
    summary_path = os.path.join(args.output_dir, "trades_split_summary.json")
    summary = {
        "input": os.path.basename(args.input),
        "output_dir": args.output_dir,
        "total_trades_written": total_written,
        "skipped_missing_day_key": skipped_no_day,
        "compressed": args.gzip,
        "extension": "json.gz" if args.gzip else "json",
        "layout": "output-dir/YYYY-MM-DD/<eventSlug>/trades.<ext>",
        "day_source": args.day_source,
        "days": {day: {"total": sum(ev.values()), "events": dict(sorted(ev.items(), key=lambda x: x[0]))}
                 for day, ev in sorted(counts.items(), key=lambda x: x[0])},
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=True, indent=2)

    print(f"Wrote per-day folders and per-event files under: {args.output_dir}")
    print(f"Summary: {summary_path}")


if __name__ == "__main__":
    main()
