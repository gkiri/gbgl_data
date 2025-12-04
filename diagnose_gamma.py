import requests
import json

def resolve_event_slug(slug):
    print(f"Resolving Event Slug: {slug}")
    try:
        # Gamma API is used by the frontend
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                event = data[0]
                print(f"✅ Found Event: {event.get('title')}")
                
                markets = event.get('markets', [])
                if markets:
                    market = markets[0]
                    print(f"   Market ID (Condition ID): {market.get('conditionId')}")
                    print(f"   Question: {market.get('question')}")
                    print(f"   CLOB Token IDs: {market.get('clobTokenIds')}")
                    return market
            else:
                print("❌ No event found for slug.")
        else:
            print(f"❌ Gamma API Error: {resp.status_code}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Test with one of the provided slugs
    resolve_event_slug("eth-updown-15m-1764860400")

