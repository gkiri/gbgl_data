import asyncio
import aiohttp
import ujson
from config import WS_URL, REST_URL

async def test_format2_thoroughly():
    """
    Test Format 2 (type/assets_ids) thoroughly to see if it receives data.
    """
    print("=" * 60)
    print("Testing Format 2: {'type': 'market', 'assets_ids': [...]}")
    print("=" * 60)
    
    # Get token IDs
    import requests
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
        print("❌ No Ethereum markets found")
        return False
    
    tokens = eth_market.get('tokens', [])
    yes_token_id = tokens[0]['token_id']
    no_token_id = tokens[1]['token_id']
    
    print(f"✅ Market: {eth_market.get('question', 'Unknown')[:60]}...")
    print(f"✅ Token IDs: {yes_token_id[:20]}..., {no_token_id[:20]}...\n")
    
    # Use Format 2
    sub_msg = {
        "type": "market",
        "assets_ids": [yes_token_id, no_token_id]
    }
    
    print(f"📤 Sending subscription: {ujson.dumps(sub_msg)}\n")
    
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(WS_URL) as ws:
            print("✅ Connected to WebSocket")
            
            await ws.send_json(sub_msg)
            print("✅ Subscription sent\n")
            
            print("📥 Listening for messages (20 seconds)...")
            print("-" * 60)
            
            message_count = 0
            price_changes = 0
            
            try:
                async with asyncio.timeout(20):
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = ujson.loads(msg.data)
                            message_count += 1
                            
                            event_type = data.get("event_type", "unknown")
                            
                            if event_type == "price_change":
                                price_changes += 1
                                print(f"📊 Price Change #{price_changes}:")
                                for change in data.get("price_changes", []):
                                    asset_id = change.get("asset_id", "")[:20]
                                    best_bid = change.get("best_bid", "N/A")
                                    best_ask = change.get("best_ask", "N/A")
                                    print(f"   Asset {asset_id}... | Bid: {best_bid} | Ask: {best_ask}")
                            
                            elif event_type == "book":
                                print(f"📚 Book snapshot received")
                            
                            elif event_type == "last_trade_price":
                                print(f"💰 Trade: {data.get('price')} @ {data.get('size')}")
                            
                            elif "message" in data:
                                print(f"⚠️  Server message: {data.get('message')}")
                            
                            else:
                                print(f"📨 Event: {event_type}")
                                if message_count == 1:
                                    print(f"   Full data: {ujson.dumps(data, indent=2)}")
                            
                            if message_count >= 10:
                                print("\n... (stopping after 10 messages)")
                                break
                                
            except asyncio.TimeoutError:
                pass
            
            print("-" * 60)
            print(f"\n📈 Summary:")
            print(f"   Total messages: {message_count}")
            print(f"   Price changes: {price_changes}")
            
            if price_changes > 0:
                print("\n✅ SUCCESS! Receiving live price updates!")
                return True
            elif message_count > 0:
                print("\n⚠️  Connected but no price_change events (market might be inactive)")
                return True
            else:
                print("\n❌ No messages received")
                return False

if __name__ == "__main__":
    result = asyncio.run(test_format2_thoroughly())
    exit(0 if result else 1)

