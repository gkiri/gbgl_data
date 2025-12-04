import asyncio
import aiohttp
import ujson
from config import WS_URL, REST_URL

async def test_subscription_formats():
    """
    Test different subscription message formats to find the correct one.
    """
    print("=" * 60)
    print("Testing Different Subscription Formats")
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
        return
    
    tokens = eth_market.get('tokens', [])
    yes_token_id = tokens[0]['token_id']
    no_token_id = tokens[1]['token_id']
    
    print(f"✅ Using tokens: {yes_token_id[:20]}..., {no_token_id[:20]}...\n")
    
    # Try different formats
    formats_to_try = [
        {
            "name": "Format 1: action/channel/filters",
            "message": {
                "action": "subscribe",
                "channel": "market",
                "filters": [yes_token_id, no_token_id]
            }
        },
        {
            "name": "Format 2: type/assets_ids",
            "message": {
                "type": "market",
                "assets_ids": [yes_token_id, no_token_id]
            }
        },
        {
            "name": "Format 3: subscribe with topic",
            "message": {
                "action": "subscribe",
                "topic": "market",
                "filters": [yes_token_id, no_token_id]
            }
        },
        {
            "name": "Format 4: subscribe with channel and asset_id",
            "message": {
                "action": "subscribe",
                "channel": "market",
                "asset_id": yes_token_id
            }
        },
        {
            "name": "Format 5: Simple subscribe",
            "message": {
                "subscribe": "market",
                "filters": [yes_token_id, no_token_id]
            }
        }
    ]
    
    async with aiohttp.ClientSession() as session:
        for fmt in formats_to_try:
            print(f"\n🧪 Testing: {fmt['name']}")
            print(f"   Message: {ujson.dumps(fmt['message'])}")
            
            try:
                async with session.ws_connect(WS_URL) as ws:
                    await ws.send_json(fmt['message'])
                    
                    # Wait for response
                    try:
                        async with asyncio.timeout(3):
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = ujson.loads(msg.data)
                                if "message" in data and "Invalid" in data["message"]:
                                    print(f"   ❌ Rejected: {data.get('message')}")
                                else:
                                    print(f"   ✅ Accepted! Response: {ujson.dumps(data, indent=2)}")
                                    print(f"\n🎉 SUCCESS! Correct format found: {fmt['name']}")
                                    return fmt['message']
                            else:
                                print(f"   ⚠️  Unexpected message type: {msg.type}")
                    except asyncio.TimeoutError:
                        print(f"   ⚠️  No response (might be valid)")
                        
            except Exception as e:
                print(f"   ❌ Error: {e}")
    
    print("\n❌ None of the formats worked. Need to check documentation.")

if __name__ == "__main__":
    asyncio.run(test_subscription_formats())

