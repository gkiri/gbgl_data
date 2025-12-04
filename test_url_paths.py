import asyncio
import aiohttp
import ujson
from config import REST_URL

async def test_url_paths():
    """
    Test if the channel should be in the URL path instead of the message.
    """
    print("=" * 60)
    print("Testing URL Path Variations")
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
    
    tokens = eth_market.get('tokens', [])
    yes_token_id = tokens[0]['token_id']
    no_token_id = tokens[1]['token_id']
    
    # Test different URL paths
    urls_to_try = [
        "wss://ws-live-data.polymarket.com",
        "wss://ws-live-data.polymarket.com/ws",
        "wss://ws-live-data.polymarket.com/ws/market",
        "wss://ws-live-data.polymarket.com/market"
    ]
    
    for url in urls_to_try:
        print(f"\n🧪 Testing URL: {url}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url) as ws:
                    print(f"   ✅ Connected")
                    
                    # Try simple subscription
                    sub_msg = {
                        "type": "market",
                        "assets_ids": [yes_token_id, no_token_id]
                    }
                    await ws.send_json(sub_msg)
                    print(f"   ✅ Sent subscription")
                    
                    # Wait for response
                    try:
                        async with asyncio.timeout(3):
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = ujson.loads(msg.data)
                                if "message" in data and "Invalid" in data["message"]:
                                    print(f"   ❌ Rejected: {data.get('message')}")
                                else:
                                    print(f"   ✅ Accepted! Response type: {data.get('event_type', 'unknown')}")
                                    print(f"   🎉 SUCCESS! Correct URL: {url}")
                                    return url
                    except asyncio.TimeoutError:
                        print(f"   ⚠️  No immediate response (might be valid)")
                        
        except Exception as e:
            print(f"   ❌ Connection failed: {e}")
    
    print("\n❌ None of the URL paths worked")

if __name__ == "__main__":
    asyncio.run(test_url_paths())

