import asyncio

from bot_runner import run_bot
from config import ETH_SLUG_PREFIX, MARKET_HANDOFF_BUFFER, MARKET_SLUG


async def main():
    await run_bot(
        asset_label="ETH",
        slug_prefix=ETH_SLUG_PREFIX,
        market_slug_override=MARKET_SLUG,
        handoff_buffer=MARKET_HANDOFF_BUFFER,
        keywords=["eth", "ethereum"],
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down...")
