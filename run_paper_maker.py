#!/usr/bin/env python3
"""
Entry point for Paper Trading the Maker Strategy.
This runs the bot with live data but MOCKED execution.
"""

import asyncio
import os
import sys
import signal

# Ensure project root on sys.path
from pathlib import Path
ROOT = Path(__file__).resolve().parents[0]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# Set Paper Trade Flag
os.environ["PAPER_TRADE_MODE"] = "true"

from maker_strategy import MakerStrategy

async def main():
    print("="*60)
    print("📝 PAPER TRADING MODE: MAKER STRATEGY")
    print("="*60)
    print("Live Data:  Enabled (Binance + Polymarket)")
    print("Execution:  MOCKED (No real funds used)")
    print("Log File:   data/paper_logs/paper_trades_*.jsonl")
    print("="*60)
    print()

    strategy = MakerStrategy(paper_mode=True)
    
    # Handle graceful shutdown
    def signal_handler(sig, frame):
        print("\n[PaperRunner] 🛑 Stopping...")
        strategy.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    await strategy.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

