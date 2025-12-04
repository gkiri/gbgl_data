import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp

from config import REST_URL


async def fetch_trades(session, market: str, since: Optional[str] = None):
    params = {"username": "shadow-bot", "limit": 100, "market": market}
    if since:
        params["since"] = since
    url = f"{REST_URL}/trades"
    async with session.get(url, params=params) as resp:
        resp.raise_for_status()
        return await resp.json()


async def shadow_loop(market_slug: str, outfile: Path, poll_seconds: float = 5.0):
    outfile.parent.mkdir(parents=True, exist_ok=True)
    last_trade_id: Optional[str] = None
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                payload = await fetch_trades(session, market_slug, last_trade_id)
                trades = payload if isinstance(payload, list) else payload.get("data", [])
                trades = trades or []
                if trades:
                    trades.sort(key=lambda x: x.get("createdAt", ""))
                    with outfile.open("a", encoding="utf-8") as handle:
                        for trade in trades:
                            record = {
                                "ts": trade.get("createdAt")
                                or datetime.now(timezone.utc).isoformat(),
                                "market": market_slug,
                                "side": trade.get("side"),
                                "price": trade.get("price"),
                                "size": trade.get("size"),
                                "maker": trade.get("maker"),
                                "taker": trade.get("taker"),
                            }
                            handle.write(json.dumps(record) + "\n")
                    last_trade_id = trades[-1].get("id") or last_trade_id
                await asyncio.sleep(poll_seconds)
            except Exception as exc:
                print(f"[Shadow] Error while fetching trades: {exc}")
                await asyncio.sleep(poll_seconds * 2)


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Shadow trade logger")
    parser.add_argument("--market", required=True, help="Market slug to monitor")
    parser.add_argument(
        "--output",
        default="data/shadow_trades.log",
        help="Path to append newline-delimited trade records",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Polling interval in seconds (default: 5s)",
    )
    args = parser.parse_args()

    outfile = Path(args.output)
    await shadow_loop(args.market, outfile, poll_seconds=args.interval)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[Shadow] shutting down")

