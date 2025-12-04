import asyncio
import aiohttp
import ujson
from config import WS_URL, REST_URL

async def test_websocket_detailed():
    """
    Test WebSocket with detailed message inspection.
    """
    print("=" * 60)
    print("Testing WebSocket with Detailed Message Inspection")
    print("=" * 60)
    
    # Find a real Ethereum market
    print("\n[1] Fetching active Ethereum markets...")
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
            print("❌ No Ethereum markets found")
            return False
        
        print(f"✅ Found: {eth_market.get('question', 'Unknown')[:60]}...")
        
        tokens = eth_market.get('tokens', [])
        if len(tokens) < 2:
            print("❌ Market doesn't have YES/NO tokens")
            return False
        
        yes_token_id = tokens[0]['token_id']
        no_token_id = tokens[1]['token_id']
        
        print(f"✅ Token IDs ready")
        
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False
    
    # Connect and subscribe
    print(f"\n[2] Connecting to WebSocket...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(WS_URL) as ws:
                print("✅ Connected")
                
                # Try subscription
                print(f"\n[3] Sending subscription...")
                sub_msg = {
                    "action": "subscribe",
                    "channel": "market",
                    "filters": [yes_token_id, no_token_id]
                }
                await ws.send_json(sub_msg)
                print(f"✅ Sent: {sub_msg}")
                
                # Listen and print ALL messages
                print(f"\n[4] Listening for messages (15 seconds)...")
                print("-" * 60)
                
                message_count = 0
                
                try:
                    async with asyncio.timeout(15):
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = ujson.loads(msg.data)
                                message_count += 1
                                
                                print(f"\n📨 Message #{message_count}:")
                                print(f"   Full JSON: {ujson.dumps(data, indent=2)}")
                                
                                # Check for common fields
                                if "event_type" in data:
                                    print(f"   Event Type: {data['event_type']}")
                                if "market" in data:
                                    print(f"   Market: {data['market']}")
                                if "price_changes" in data:
                                    print(f"   Price Changes: {len(data['price_changes'])} items")
                                
                                if message_count >= 3:
                                    print("\n... (stopping after 3 messages)")
                                    break
                                    
                except asyncio.TimeoutError:
                    pass
                
                print("-" * 60)
                print(f"\n📈 Received {message_count} messages")
                
                if message_count > 0:
                    print("✅ WebSocket is working and receiving data!")
                    return True
                else:
                    print("❌ No messages received")
                    return False
                    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = asyncio.run(test_websocket_detailed())
    exit(0 if result else 1)

