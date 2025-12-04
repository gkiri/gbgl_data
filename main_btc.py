import asyncio

from bot_runner import run_bot
from config import BTC_MARKET_HANDOFF_BUFFER, BTC_MARKET_SLUG, BTC_SLUG_PREFIX


async def main():
    await run_bot(
        asset_label="BTC",
        slug_prefix=BTC_SLUG_PREFIX,
        market_slug_override=BTC_MARKET_SLUG,
        handoff_buffer=BTC_MARKET_HANDOFF_BUFFER,
        keywords=["btc", "bitcoin"],
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down...")

