import requests
import json
from datetime import datetime, timezone
import dateutil.parser

REST_URL = "https://clob.polymarket.com"

def diagnose():
    print("--- DIAGNOSING MARKETS (SORTED BY END DATE) ---")
    url = f"{REST_URL}/markets"
    params = {
        "active": "true",
        "closed": "false",
        "limit": 20,
        "order": "endDate",
        "ascending": "true"
    }
    
    try:
        print("Fetching markets...")
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        markets = data.get('data', data) if isinstance(data, dict) else data
        
        print(f"Fetched {len(markets)} markets.")
        
        now = datetime.now(timezone.utc)
        
        print("\n--- SOONEST EXPIRING MARKETS ---")
        for i, m in enumerate(markets):
            q = m.get("question", "")
            end_date_str = m.get("end_date_iso")
            print(f"[{i+1}] {q}")
            print(f"    Expires: {end_date_str}")
            print("-" * 30)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    diagnose()
