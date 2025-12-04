import requests  # type: ignore
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import dateutil.parser  # type: ignore
from config import REST_URL

GAMMA_URL = "https://gamma-api.polymarket.com/events"
EASTERN = ZoneInfo("America/New_York")

def get_target_markets(
    specific_slug=None,
    exclude_slugs=None,
    slug_prefix="eth-updown-15m",
    asset_label="ETH",
    keywords=None,
):
    """
    Finds the current active '<ASSET> Up/Down 15m' market using multiple strategies:
    1. Respect a manually supplied slug override.
    2. Predict the slug by rounding current America/New_York time to 15-min windows.
    3. Fall back to scanning the legacy /markets list (best-effort).
    """
    exclude_slugs = exclude_slugs or set()
    exclude_slugs_normalized = {
        slug.lower() for slug in exclude_slugs if isinstance(slug, str)
    }
    tag = f"[Utils/{asset_label}]"
    keywords = [kw.lower() for kw in (keywords or [asset_label.lower()])]

    def filter_excluded(markets):
        if not markets or not exclude_slugs_normalized:
            return markets
        filtered = []
        for market in markets:
            slug = str(market.get("market_slug", "")).lower()
            if slug and slug in exclude_slugs_normalized:
                print(f"[Utils] ⏭️ Skipping excluded slug: {market.get('market_slug')}")
                continue
            filtered.append(market)
        return filtered

    def is_active_and_valid(market):
        if market.get("closed") is True:
            return False
        if not market.get("end_date_iso"):
            return False
        try:
            now = datetime.now(timezone.utc)
            end_date = dateutil.parser.isoparse(market["end_date_iso"])
            return end_date > now
        except Exception:
            return False

    def normalize_market_stub(slug, question, condition_id=None):
        return [{
            "market_slug": slug,
            "condition_id": condition_id,
            "question": question or slug
        }]

    def fetch_from_clob(slug):
        print(f"{tag} Checking CLOB slug: {slug}...")
        try:
            resp = requests.get(f"{REST_URL}/markets/{slug}")
            if resp.status_code == 200:
                data = resp.json()
                market = data[0] if isinstance(data, list) else data
                if is_active_and_valid(market):
                    print(f"{tag} ✅ CLOB market active: {market.get('question')}")
                    return [market]
                print(f"{tag} ⚠️ Market slug {slug} exists but is inactive.")
            else:
                print(f"{tag} ❌ CLOB slug {slug} not found (status {resp.status_code})")
        except Exception as e:
            print(f"{tag} CLOB fetch error for {slug}: {e}")
        return None

    def fetch_from_gamma(slug):
        print(f"{tag} Checking Gamma for event slug: {slug}...")
        try:
            resp = requests.get(GAMMA_URL, params={"slug": slug})
            if resp.status_code != 200:
                print(f"{tag} ❌ Gamma slug {slug} not found (status {resp.status_code})")
                return None

            events = resp.json()
            if not isinstance(events, list) or not events:
                print(f"{tag} ❌ Gamma response empty.")
                return None

            event = events[0]
            markets = event.get("markets") or []
            if not markets:
                print(f"{tag} ⚠️ Gamma event has no markets.")
                return None

            market = markets[0]
            resolved_slug = market.get("slug") or slug
            condition_id = market.get("conditionId") or market.get("condition_id")
            question = market.get("question") or event.get("title")
            print(f"{tag} 🎯 Gamma target: {question} (slug: {resolved_slug})")
            return normalize_market_stub(resolved_slug, question, condition_id)
        except Exception as e:
            print(f"{tag} Gamma fetch error for {slug}: {e}")
            return None

    # 1. Respect manual override if provided
    if specific_slug:
        result = fetch_from_clob(specific_slug) or fetch_from_gamma(specific_slug)
        if result:
            return filter_excluded(result)

    # 2. Predict slug based on America/New_York 15-min candles
    print(f"{tag} 🔮 Predicting current 15m market slug (America/New_York)...")
    try:
        now_est = datetime.now(EASTERN)
        slot_minutes = (now_est.minute // 15) * 15
        slot_start_est = now_est.replace(minute=slot_minutes, second=0, microsecond=0)
        slot_start_utc = slot_start_est.astimezone(timezone.utc)
        current_slug = f"{slug_prefix}-{int(slot_start_utc.timestamp())}"

        result = fetch_from_clob(current_slug) or fetch_from_gamma(current_slug)
        if result:
            filtered = filter_excluded(result)
            if filtered:
                return filtered

        next_start_est = slot_start_est + timedelta(minutes=15)
        next_slug = f"{slug_prefix}-{int(next_start_est.astimezone(timezone.utc).timestamp())}"
        result = fetch_from_clob(next_slug) or fetch_from_gamma(next_slug)
        if result:
            filtered = filter_excluded(result)
            if filtered:
                return filtered
    except Exception as e:
        print(f"{tag} Prediction error: {e}")

    # 3. Legacy scan fallback (often stale, but harmless to try)
    print(f"{tag} 🔄 Legacy fallback scan...")
    try:
        resp = requests.get(
            f"{REST_URL}/markets",
            params={
                "active": "true",
                "closed": "false",
                "limit": 200,
                "order": "endDate",
                "ascending": "true",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        markets = data.get("data", data) if isinstance(data, dict) else data

        candidates = []
        now = datetime.now(timezone.utc)
        lookahead = now + timedelta(hours=2)

        for m in markets:
            question = str(m.get("question", "")).lower()
            slug = str(m.get("market_slug", "")).lower()
            has_keywords = any(kw in question for kw in keywords) and (
                "up" in question or "down" in question
            )
            is_slug_match = slug_prefix in slug
            if not (has_keywords or is_slug_match):
                continue

            if is_active_and_valid(m):
                end_date = dateutil.parser.isoparse(m["end_date_iso"])
                if end_date <= lookahead:
                    candidates.append(m)

        candidates = filter_excluded(candidates)
        candidates.sort(key=lambda x: x.get("end_date_iso", ""))
        if candidates:
            best = candidates[0]
            print(f"{tag} 🎯 Legacy scan target: {best.get('question')}")
            return [best]
    except Exception as e:
        print(f"{tag} Legacy scan error: {e}")

    print(f"{tag} ❌ No active {asset_label} 15m markets found via any method.")
    return []
