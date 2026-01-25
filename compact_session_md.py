#!/usr/bin/env python3
"""
Generate compact, timestamp-grouped markdown summaries for Up/Down trades.
"""

import argparse
import json
import os
from collections import defaultdict, OrderedDict
from datetime import datetime
from typing import Dict, Iterable, List, Tuple


def load_trades(path: str) -> List[Dict]:
    with open(path, "r") as f:
        raw = json.load(f)
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and isinstance(raw.get("trades"), list):
        return raw["trades"]
    raise ValueError("JSON must be a list of trades or contain a 'trades' list.")


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def group_by_timestamp(trades: Iterable[Dict]) -> "OrderedDict[int, List[Dict]]":
    grouped: "OrderedDict[int, List[Dict]]" = OrderedDict()
    for trade in sorted(trades, key=lambda t: int(t.get("timestamp", 0))):
        ts = int(trade.get("timestamp", 0))
        grouped.setdefault(ts, []).append(trade)
    return grouped


def summarize_trades(trades: List[Dict]) -> Dict:
    total_shares = sum(float(t.get("size", 0)) for t in trades)
    total_cost = sum(float(t.get("size", 0)) * float(t.get("price", 0)) for t in trades)
    prices = [float(t.get("price", 0)) for t in trades] if trades else []
    min_price = min(prices) if prices else 0
    max_price = max(prices) if prices else 0
    vwap = (total_cost / total_shares) if total_shares else 0
    return {
        "total_shares": total_shares,
        "total_cost": total_cost,
        "vwap": vwap,
        "min_price": min_price,
        "max_price": max_price,
    }


def aggregate_legs(trades: List[Dict], precision: int) -> str:
    price_map: Dict[float, Tuple[int, float]] = defaultdict(lambda: (0, 0.0))
    for trade in trades:
        price = float(trade.get("price", 0))
        size = float(trade.get("size", 0))
        count, total_size = price_map[price]
        price_map[price] = (count + 1, total_size + size)

    legs = []
    for price in sorted(price_map.keys(), reverse=True):
        count, total_size = price_map[price]
        legs.append(f"p{price:.{precision}f}*{count}={total_size:.{precision}f}")
    return " ".join(legs)


def pick_metadata(trades: List[Dict]) -> Dict:
    first = trades[0] if trades else {}
    return {
        "title": first.get("title", "Unknown Market"),
        "slug": first.get("slug") or first.get("eventSlug") or first.get("_market_slug", ""),
        "condition_id": first.get("conditionId") or first.get("_condition_id") or first.get("condition_id", ""),
        "asset": first.get("asset", ""),
        "market_ts": first.get("_market_ts", ""),
    }


def render_markdown(
    trades: List[Dict],
    outcome_label: str,
    include_sells: bool,
    precision: int,
) -> str:
    if not trades:
        return f"# {outcome_label} Trades\n\nNo trades found.\n"

    meta = pick_metadata(trades)
    grouped = group_by_timestamp(trades)
    first_ts = next(iter(grouped.keys()))
    last_ts = next(reversed(grouped.keys()))

    summary = summarize_trades(trades)

    # Count any non-buy trades
    sell_count = sum(1 for t in trades if str(t.get("side", "")).upper() != "BUY")

    lines = []
    lines.append(f"# {outcome_label} Trades")
    lines.append("")
    lines.append(f"- Market: {meta['title']}")
    lines.append(f"- Slug: {meta['slug']}")
    lines.append(f"- Condition ID: {meta['condition_id']}")
    if meta["asset"]:
        lines.append(f"- Asset: {meta['asset']}")
    if meta["market_ts"]:
        lines.append(f"- Market timestamp: {meta['market_ts']}")
    lines.append(f"- Trade count: {len(trades)}")
    lines.append(f"- Time range: {format_ts(first_ts)} → {format_ts(last_ts)}")
    lines.append(f"- Prices are probabilities (0-1), sizes are shares")
    if sell_count and not include_sells:
        lines.append(f"- Note: {sell_count} non-BUY trades excluded")
    lines.append("")

    lines.append("## Overall Summary")
    lines.append("")
    lines.append(f"- Total shares: {summary['total_shares']:.{precision}f}")
    lines.append(f"- Total cost: {summary['total_cost']:.{precision}f}")
    lines.append(f"- VWAP: {summary['vwap']:.{precision}f}")
    lines.append(f"- Min price: {summary['min_price']:.{precision}f}")
    lines.append(f"- Max price: {summary['max_price']:.{precision}f}")
    lines.append("")

    lines.append("## Timestamp Groups")
    lines.append("")
    lines.append(
        "| ts | time | trades | shares | vwap | min | max | cum_shares | cum_vwap | legs (price*count=size) |"
    )
    lines.append(
        "|---:|:-----|------:|------:|-----:|----:|----:|----------:|--------:|:------------------------|"
    )

    cum_shares = 0.0
    cum_cost = 0.0

    for ts, bucket in grouped.items():
        bucket_summary = summarize_trades(bucket)
        cum_shares += bucket_summary["total_shares"]
        cum_cost += bucket_summary["total_cost"]
        cum_vwap = (cum_cost / cum_shares) if cum_shares else 0
        legs = aggregate_legs(bucket, precision)
        lines.append(
            f"| {ts} | {format_ts(ts)} | {len(bucket)} | "
            f"{bucket_summary['total_shares']:.{precision}f} | "
            f"{bucket_summary['vwap']:.{precision}f} | "
            f"{bucket_summary['min_price']:.{precision}f} | "
            f"{bucket_summary['max_price']:.{precision}f} | "
            f"{cum_shares:.{precision}f} | {cum_vwap:.{precision}f} | {legs} |"
        )

    lines.append("")
    return "\n".join(lines)


def render_detailed_markdown(
    trades: List[Dict],
    outcome_label: str,
    include_sells: bool,
    precision: int,
) -> str:
    if not trades:
        return f"# {outcome_label} Trades (Detailed)\n\nNo trades found.\n"

    meta = pick_metadata(trades)
    sorted_trades = sorted(trades, key=lambda t: int(t.get("timestamp", 0)))
    first_ts = int(sorted_trades[0].get("timestamp", 0))
    last_ts = int(sorted_trades[-1].get("timestamp", 0))

    # Count any non-buy trades
    sell_count = sum(1 for t in trades if str(t.get("side", "")).upper() != "BUY")

    lines = []
    lines.append(f"# {outcome_label} Trades (Detailed)")
    lines.append("")
    lines.append(f"- Market: {meta['title']}")
    lines.append(f"- Slug: {meta['slug']}")
    lines.append(f"- Condition ID: {meta['condition_id']}")
    if meta["asset"]:
        lines.append(f"- Asset: {meta['asset']}")
    if meta["market_ts"]:
        lines.append(f"- Market timestamp: {meta['market_ts']}")
    lines.append(f"- Trade count: {len(sorted_trades)}")
    lines.append(f"- Time range: {format_ts(first_ts)} → {format_ts(last_ts)}")
    lines.append("- Price is probability (0-1), shares are sizes")
    lines.append("- cum_vwap is share-weighted average up to row")
    if sell_count and not include_sells:
        lines.append(f"- Note: {sell_count} non-BUY trades excluded")
    lines.append("")

    lines.append("## Trades (Sequential)")
    lines.append("")
    lines.append("| i | ts | price | shares | cost | cum_vwap | cum_shares |")
    lines.append("|--:|---:|-----:|------:|-----:|--------:|----------:|")
    cum_cost = 0.0
    cum_shares = 0.0

    for i, trade in enumerate(sorted_trades, start=1):
        price = float(trade.get("price", 0))
        shares = float(trade.get("size", 0))
        cost = price * shares

        cum_shares += shares
        cum_cost += cost
        cum_vwap = (cum_cost / cum_shares) if cum_shares else 0

        ts = int(trade.get("timestamp", 0))
        lines.append(
            f"| {i} | {ts} | {price:.{precision}f} | {shares:.{precision}f} | "
            f"{cost:.{precision}f} | {cum_vwap:.{precision}f} | "
            f"{cum_shares:.{precision}f} |"
        )

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create compact markdown summaries for Up/Down trades."
    )
    parser.add_argument("--input", required=True, help="Input JSON trades file")
    parser.add_argument("--output-dir", default=None, help="Output directory for md files")
    parser.add_argument("--precision", type=int, default=4, help="Decimal precision")
    parser.add_argument("--include-sells", action="store_true", help="Include non-BUY trades")
    parser.add_argument("--detailed", action="store_true", help="Write detailed per-trade tables")
    args = parser.parse_args()

    trades = load_trades(args.input)

    if not args.include_sells:
        trades = [t for t in trades if str(t.get("side", "")).upper() == "BUY"]

    up_trades = [t for t in trades if str(t.get("outcome", "")).lower() == "up"]
    down_trades = [t for t in trades if str(t.get("outcome", "")).lower() == "down"]

    input_dir = os.path.dirname(args.input)
    output_dir = args.output_dir or input_dir or "."
    os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.splitext(os.path.basename(args.input))[0]
    up_path = os.path.join(output_dir, f"{base_name}_up.md")
    down_path = os.path.join(output_dir, f"{base_name}_down.md")

    with open(up_path, "w") as f:
        f.write(render_markdown(up_trades, "UP", args.include_sells, args.precision))
    with open(down_path, "w") as f:
        f.write(render_markdown(down_trades, "DOWN", args.include_sells, args.precision))

    print(f"Wrote UP markdown: {up_path}")
    print(f"Wrote DOWN markdown: {down_path}")

    if args.detailed:
        up_detail_path = os.path.join(output_dir, f"{base_name}_up_detailed.md")
        down_detail_path = os.path.join(output_dir, f"{base_name}_down_detailed.md")
        with open(up_detail_path, "w") as f:
            f.write(
                render_detailed_markdown(up_trades, "UP", args.include_sells, args.precision)
            )
        with open(down_detail_path, "w") as f:
            f.write(
                render_detailed_markdown(down_trades, "DOWN", args.include_sells, args.precision)
            )
        print(f"Wrote UP detailed markdown: {up_detail_path}")
        print(f"Wrote DOWN detailed markdown: {down_detail_path}")


if __name__ == "__main__":
    main()

