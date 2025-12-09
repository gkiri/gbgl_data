import asyncio
import json
from typing import Dict, Optional

import aiohttp


class BinanceFeed:
    """
    Streams BTC/ETH best bid/ask (mid) from Binance Futures bookTicker.
    """

    STREAM_URL = "wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker"

    def __init__(self):
        self.prices: Dict[str, Optional[float]] = {"BTC": None, "ETH": None}
        self.running: bool = False

    async def start(self, reconnect_delay: float = 1.0):
        self.running = True
        while self.running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.STREAM_URL) as ws:
                        print("[Binance] Connected to BTC & ETH Futures Stream")
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                except Exception:
                                    continue
                                tick = data.get("data") or {}
                                symbol = tick.get("s")
                                if not symbol:
                                    continue
                                bid_price = float(tick.get("b", 0) or 0)
                                ask_price = float(tick.get("a", 0) or 0)
                                if bid_price == 0 or ask_price == 0:
                                    continue
                                mid = (bid_price + ask_price) / 2
                                if "BTC" in symbol:
                                    self.prices["BTC"] = mid
                                elif "ETH" in symbol:
                                    self.prices["ETH"] = mid
            except Exception as e:
                print(f"[Binance] Error: {e}")
                await asyncio.sleep(reconnect_delay)

    def stop(self):
        self.running = False

    def get_price(self, asset: str) -> Optional[float]:
        return self.prices.get(asset)

