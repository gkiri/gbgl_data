# Polymarket Gabagool Bot

A high-frequency volatility arbitrage bot for Polymarket 15-minute Bitcoin/Ethereum markets.

## Quick Start

1. **Install dependencies:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   - Copy `.env.example` to `.env` and fill in your keys.
   - Optional knobs (momentum thresholds, rolling guard, leash, etc.) are documented in [`docs/TUNING.md`](docs/TUNING.md).

3. **Run the bot:**
   ```bash
   # Ethereum 15m markets
   python main.py

   # Bitcoin 15m markets (optional second instance)
   python main_btc.py
   ```

4. **Shadow logger (optional):**
   ```bash
   python shadow_listener.py --market btc-updown-15m-1764871200 --output data/btc-trades.log
   ```
   Use this telemetry to tune the adaptive triggers before going live.

## AWS Deployment

See `DEPLOYMENT.md` for detailed AWS deployment instructions.

## Architecture

- **Hot Path (Sniper)**: Low-latency WebSocket listener for price updates
- **Cold Path (Strategist)**: Market discovery, order signing, position management
- **Blockchain Monitor**: Polygon RPC integration for merging positions
- **Shadow Listener** *(optional)*: Records live fills for heatmap-style analysis

## Strategy

The bot implements the "Gabagool" strategy:
- Scans for active 15-minute BTC/ETH markets
- Places pre-signed limit orders on both YES and NO sides
- Adaptive combined guard tightens/loosens based on volatility
- Optional single-side momentum traps mirror @gabagool22-style sweeps
- Merges positions to recycle capital

## Testing

Run connectivity tests before deployment:
```bash
python test_connectivity.py
python test_websocket_market.py
```
