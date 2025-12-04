import asyncio
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Optional

from dateutil import parser as date_parser  # type: ignore

from cold_path import Strategist
from hot_path import Sniper
from utils import get_target_markets


def _predict_next_slug(current_slug: Optional[str]) -> Optional[str]:
    if not current_slug or "-" not in current_slug:
        return None
    try:
        base_ts = int(current_slug.rsplit("-", 1)[-1])
        prefix = current_slug.rsplit("-", 1)[0]
        return f"{prefix}-{base_ts + 900}"
    except ValueError:
        return None


async def _monitor_market_window(end_iso, stop_event, buffer_seconds):
    if not end_iso:
        return
    try:
        end_dt = date_parser.isoparse(end_iso)
    except Exception:
        print("[Main] ⚠️ Unable to parse end_date_iso; skipping handoff timer.")
        return

    if not end_dt.tzinfo:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    trigger_dt = end_dt - timedelta(seconds=buffer_seconds)
    if trigger_dt <= datetime.now(timezone.utc):
        print("[Main] ♻️ Handoff window already open; signaling stop immediately.")
        stop_event.set()
        return

    print(
        f"[Main] 🕒 Handoff scheduled {buffer_seconds}s before expiry "
        f"({trigger_dt.strftime('%H:%M:%S')} UTC)."
    )

    while not stop_event.is_set():
        now = datetime.now(timezone.utc)
        seconds = (trigger_dt - now).total_seconds()
        if seconds <= 0:
            print("[Main] ♻️ Time to rotate markets.")
            stop_event.set()
            break
        await asyncio.sleep(min(30, max(seconds, 1)))


async def run_bot(
    *,
    asset_label: str,
    slug_prefix: str,
    market_slug_override: Optional[str] = None,
    handoff_buffer: int = 60,
    keywords: Optional[Iterable[str]] = None,
):
    """
    Shared runner for ETH/BTC market bots.
    """
    print(f"--- GABAGOOL BOT ({asset_label}) ---")
    if market_slug_override:
        print(f"[Main] 🛠️ Manual Market Override: {market_slug_override}")

    last_slug = None
    manual_mode = bool(market_slug_override)
    keyword_list: List[str] = [kw.lower() for kw in (keywords or [asset_label.lower()])]
    target_orders_memory = {}

    while True:
        try:
            excludes = set()
            if not manual_mode and last_slug:
                excludes.add(last_slug)

            target_markets = get_target_markets(
                specific_slug=market_slug_override if manual_mode else None,
                exclude_slugs=excludes,
                slug_prefix=slug_prefix,
                asset_label=asset_label,
                keywords=keyword_list,
            )

            if not target_markets:
                print("[Main] ❌ No markets found. Retrying in 10s...")
                await asyncio.sleep(10)
                continue

            active_market = target_markets[0]
            market_id_to_trade = (
                active_market.get("condition_id")
                or active_market.get("conditionId")
                or active_market.get("market_slug")
            )

            last_slug = active_market.get("market_slug")
            predicted_next = _predict_next_slug(last_slug)

            print(
                f"[Main] 🎯 Locking on to: {active_market.get('question')} "
                f"(ID: {market_id_to_trade})"
            )
            if predicted_next:
                print(f"[Main] 📡 Next predicted slug: {predicted_next}")

            stop_event = asyncio.Event()
            price_cache = {}

            strategist = Strategist(
                active_market,
                target_orders_memory,
                stop_event=stop_event,
                price_cache=price_cache,
            )

            print("[Main] Fetching token details...")
            if not strategist.fetch_market_details():
                print("[Main] ⚠️ Failed to fetch token IDs. Retrying in 10s...")
                await asyncio.sleep(10)
                continue

            asset_ids_to_watch = list(strategist.tokens.values())

            sniper = Sniper(
                target_orders_memory,
                asset_ids=asset_ids_to_watch,
                stop_event=stop_event,
                price_cache=price_cache,
            )

            print("[Main] 🚀 Starting parallel engines...")
            sniper_task = asyncio.create_task(sniper.run())
            strategist_task = asyncio.create_task(strategist.run_loop())
            handoff_task = asyncio.create_task(
                _monitor_market_window(
                    active_market.get("end_date_iso") or active_market.get("endDate"),
                    stop_event,
                    handoff_buffer,
                )
            )

            done, pending = await asyncio.wait(
                {sniper_task, strategist_task, handoff_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not stop_event.is_set():
                stop_event.set()

            for task in pending:
                task.cancel()

            await asyncio.gather(*pending, return_exceptions=True)

            for task in done:
                exc = task.exception()
                if exc and not isinstance(exc, asyncio.CancelledError):
                    print(f"[Main] ⚠️ Engine exit reason: {exc}")

            target_orders_memory.clear()
            await asyncio.sleep(2)

        except KeyboardInterrupt:
            print("[Main] Shutting down gracefully...")
            break
        except Exception as e:
            print(f"[Main] ❌ Critical error: {e}")
            import traceback

            traceback.print_exc()
            print("[Main] Retrying in 10s...")
            await asyncio.sleep(10)

