#!/bin/bash

# Paper trading launcher for maker: reads live orderbooks/prices but does NOT post.

# Activate virtual environment
if [ -f "venv/bin/activate" ]; then
    . venv/bin/activate
fi

# Enable paper mode
export PAPER_TRADING=true

# Optional maker tuning overrides (keep aligned with run_maker.sh defaults)
export MAKER_MAX_BUNDLE_COST="${MAKER_MAX_BUNDLE_COST:-0.98}"
export MAKER_ORDER_SIZE="${MAKER_ORDER_SIZE:-20}"
export MAKER_REFRESH_INTERVAL="${MAKER_REFRESH_INTERVAL:-2}"

python3 strategies/maker/run_maker.py

