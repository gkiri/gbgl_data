import asyncio
import aiohttp
import ujson
from config import WS_URL, REST_URL

async def test_websocket_market_subscription():
    """
    Test WebSocket connection and subscription to a real Ethereum market.
    """
    print("=" * 60)
    print("Testing WebSocket Market Subscription")
    print("=" * 60)
    
    # Step 1: Find a real Ethereum market
    print("\n[1/4] Fetching active Ethereum markets...")
    import requests
    try:
        resp = requests.get(f"{REST_URL}/markets?active=true&limit=50")
        markets = resp.json()
        if isinstance(markets, dict) and 'data' in markets:
            markets = markets['data']
        
        eth_market = None
        for market in markets:
            question = str(market.get("question", "")).lower()
            if "ethereum" in question or "eth" in question:
                eth_market = market
                break
        
        if not eth_market:
            print("❌ No Ethereum markets found in sample")
            return False
        
        print(f"✅ Found market: {eth_market.get('question', 'Unknown')[:50]}...")
        
        # Get token IDs
        tokens = eth_market.get('tokens', [])
        if len(tokens) < 2:
            print("❌ Market doesn't have YES/NO tokens")
            return False
        
        yes_token_id = tokens[0]['token_id']
        no_token_id = tokens[1]['token_id']
        
        print(f"✅ YES Token ID: {yes_token_id[:20]}...")
        print(f"✅ NO Token ID: {no_token_id[:20]}...")
        
    except Exception as e:
        print(f"❌ Failed to fetch markets: {e}")
        return False
    
    # Step 2: Connect to WebSocket
    print(f"\n[2/4] Connecting to WebSocket: {WS_URL}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(WS_URL) as ws:
                print("✅ WebSocket connected")
                
                # Step 3: Subscribe to market
                print(f"\n[3/4] Subscribing to market channel...")
                sub_msg = {
                    "action": "subscribe",
                    "channel": "market",
                    "filters": [yes_token_id, no_token_id]
                }
                await ws.send_json(sub_msg)
                print(f"✅ Subscription sent: {sub_msg}")
                
                # Step 4: Listen for messages
                print(f"\n[4/4] Listening for price updates (10 seconds)...")
                print("-" * 60)
                
                message_count = 0
                price_changes_received = 0
                
                try:
                    async with asyncio.timeout(10):  # Listen for 10 seconds
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = ujson.loads(msg.data)
                                message_count += 1
                                
                                event_type = data.get("event_type", "unknown")
                                
                                if event_type == "price_change":
                                    price_changes_received += 1
                                    price_changes = data.get("price_changes", [])
                                    print(f"📊 Price Change #{price_changes_received}:")
                                    for change in price_changes:
                                        asset_id = change.get("asset_id", "")[:20]
                                        best_bid = change.get("best_bid", "N/A")
                                        best_ask = change.get("best_ask", "N/A")
                                        print(f"   Asset {asset_id}... | Bid: {best_bid} | Ask: {best_ask}")
                                
                                elif event_type == "book":
                                    print(f"📚 Book snapshot received")
                                
                                elif event_type == "last_trade_price":
                                    print(f"💰 Last trade: {data.get('price')} @ {data.get('size')}")
                                
                                else:
                                    print(f"📨 Message type: {event_type}")
                                
                                # Limit output
                                if message_count >= 5:
                                    print("... (continuing to listen)")
                                    break
                                    
                except asyncio.TimeoutError:
                    pass
                
                print("-" * 60)
                print(f"\n📈 Summary:")
                print(f"   Total messages received: {message_count}")
                print(f"   Price changes: {price_changes_received}")
                
                if price_changes_received > 0:
                    print("\n✅ SUCCESS: WebSocket is receiving live market data!")
                    return True
                elif message_count > 0:
                    print("\n⚠️  Partial: Connected but no price_change events yet")
                    return True
                else:
                    print("\n❌ No messages received (market might be inactive)")
                    return False
                    
    except Exception as e:
        print(f"❌ WebSocket error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = asyncio.run(test_websocket_market_subscription())
    exit(0 if result else 1)

