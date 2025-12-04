import asyncio
from contextlib import suppress
import time
import aiohttp
import ujson
import uvloop
from config import WS_URL, REST_URL, API_KEY, API_PASSPHRASE, API_SECRET

# Install uvloop for maximum speed
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

class Sniper:
    def __init__(self, target_orders_map, asset_ids=None, stop_event=None, price_cache=None):
        self.target_orders = target_orders_map  # Shared memory for "Loaded" orders
        self.seed_assets = set(asset_ids or [])
        self.active_assets = set()
        self.session = None
        self.ws = None
        self.stop_event = stop_event or asyncio.Event()
        self.subscription_task = None
        self.keep_alive_task = None
        self.price_cache = price_cache

    async def setup_session(self):
        # TCP_NODELAY is crucial for latency (disable Nagle's algorithm)
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        
        # NO HEADERS for public market data WebSocket
        # Auth headers are only needed for REST API calls
        self.session = aiohttp.ClientSession(
            connector=connector, 
            json_serialize=ujson.dumps
        )

    async def fire_order(self, payload):
        """
        The Trigger. This must be as fast as possible.
        Uses REST API with auth headers (separate from WebSocket session).
        """
        try:
            # Create a separate session WITH auth headers for REST API
            headers = {
                "Content-Type": "application/json",
                "Poly-Api-Key": API_KEY,
                "Poly-Api-Secret": API_SECRET,
                "Poly-Api-Passphrase": API_PASSPHRASE
            }
            
            async with aiohttp.ClientSession(headers=headers) as rest_session:
                url = f"{REST_URL}/order"
                async with rest_session.post(url, json=payload) as resp:
                    data = await resp.text()
                    print(f"!!! SHOT FIRED !!! Status: {resp.status} | Response: {data}")
        except Exception as e:
            print(f"Fire Error: {e}")

    async def keep_alive(self):
        """Sends a ping every 30 seconds to prevent disconnection."""
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                if self.ws and not self.ws.closed:
                    await self.ws.ping()
                continue
            break

    async def update_subscriptions(self):
        """
        Checks if the Target Memory has new assets we aren't watching.
        If so, sends a new subscription message.
        """
        while not self.stop_event.is_set():
            # Combine pre-seeded assets (from Strategist) with dynamic targets
            current_targets = set(self.target_orders.keys())
            if self.seed_assets:
                current_targets |= self.seed_assets
            
            # If there's a difference between what we need and what we are watching
            if current_targets and (current_targets != self.active_assets):
                print(f"[Hot] Updating Subscriptions. New Targets: {current_targets}")
                
                # Update local state
                self.active_assets = current_targets
                
                # Send subscription in the correct format (per Market Channel docs)
                if self.ws and not self.ws.closed:
                    sub_msg = {
                        "type": "market",
                        "assets_ids": list(self.active_assets)  # Array of token IDs
                    }
                    await self.ws.send_json(sub_msg)
            
            # Check frequently (every 1s) because Strategist updates somewhat slowly
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=1)
            except asyncio.TimeoutError:
                continue
            break

    async def run(self):
        await self.setup_session()
        
        print(f"[Hot] Connecting to Websocket...")
        
        # Start subscription updater task
        self.subscription_task = asyncio.create_task(self.update_subscriptions())
        
        try:
            while not self.stop_event.is_set():
                try:
                    async with self.session.ws_connect(WS_URL) as ws:
                        self.ws = ws
                        
                        # Start Keep-Alive task
                        self.keep_alive_task = asyncio.create_task(self.keep_alive())
                        
                        async for msg in ws:
                            if self.stop_event.is_set():
                                break
                            # FAST PARSING
                            try:
                                data = ujson.loads(msg.data)
                                
                                # Look for price_change events
                                if data.get("event_type") == "price_change":
                                    for change in data.get("price_changes", []):
                                        asset_id = change.get("asset_id")

                                        if self.price_cache is not None and asset_id:
                                            self._update_price_cache(
                                                asset_id,
                                                change.get("best_bid"),
                                                change.get("best_ask"),
                                            )
                                        
                                        # CHECK: Is this asset in our Target Map?
                                        if asset_id in self.target_orders:
                                            target = self.target_orders[asset_id]
                                            
                                            # CHECK: Is the best ASK cheap enough?
                                            market_ask = float(change.get("best_ask", 999))
                                            
                                            if market_ask <= target['trigger_price']:
                                                print(
                                                    f"[Hot] Trigger! {asset_id} Price "
                                                    f"{market_ask} <= {target['trigger_price']}"
                                                )
                                                
                                                # FIRE !!!
                                                asyncio.create_task(self.fire_order(target['payload']))
                                                
                                                # Remove to prevent machine-gunning
                                                del self.target_orders[asset_id]
                            except Exception:
                                continue
                        
                        if self.keep_alive_task:
                            self.keep_alive_task.cancel()
                            with suppress(asyncio.CancelledError):
                                await self.keep_alive_task
                            self.keep_alive_task = None
            
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    if self.stop_event.is_set():
                        break
                    print(f"[Hot] WS Error: {e}. Reconnecting...")
                    await asyncio.sleep(1)
        finally:
            self.stop_event.set()
            if self.subscription_task:
                self.subscription_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self.subscription_task
            if self.keep_alive_task:
                self.keep_alive_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self.keep_alive_task
            if self.session:
                await self.session.close()
            print("[Hot] Sniper shutdown complete.")

    def _update_price_cache(self, asset_id, best_bid, best_ask):
        entry = self.price_cache.get(asset_id)
        first_update = False
        if entry is None:
            entry = {}
            self.price_cache[asset_id] = entry
            first_update = True
        entry["ts"] = time.time()
        try:
            if best_bid is not None:
                entry["best_bid"] = float(best_bid)
        except (TypeError, ValueError):
            pass
        try:
            if best_ask is not None:
                entry["best_ask"] = float(best_ask)
        except (TypeError, ValueError):
            pass
        if first_update and entry.get("best_ask") is not None:
            bid_display = entry.get("best_bid")
            ask_display = entry.get("best_ask")
            bid_str = f"{bid_display:.4f}" if isinstance(bid_display, (int, float)) else "n/a"
            print(
                f"[Hot] 📊 Primed book for {asset_id[:10]}… "
                f"ask {ask_display:.4f} bid {bid_str}"
            )
