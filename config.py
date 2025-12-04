import os
from dotenv import load_dotenv  # type: ignore

load_dotenv()

# --- AUTHENTICATION ---
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
POLYGON_ADDRESS = os.getenv("POLYGON_ADDRESS")
API_KEY = os.getenv("CLOB_API_KEY")
API_SECRET = os.getenv("CLOB_API_SECRET")
API_PASSPHRASE = os.getenv("CLOB_API_PASSPHRASE")

# --- ENDPOINTS ---
REST_URL = "https://clob.polymarket.com"
WS_URL = "wss://ws-live-data.polymarket.com"

# --- POLYGON CHAIN ---
CHAIN_ID = 137
RPC_URL = "https://polygon-mainnet.g.alchemy.com/v2/sz8XTC13sVocHnsmQq9_R"

# --- STRATEGY SETTINGS ---
MAX_PAIR_COST = float(os.getenv("MAX_PAIR_COST", "1.0"))
ORDER_SIZE = float(os.getenv("ORDER_SIZE", "50"))

def _get_float(*keys, default):
    for key in keys:
        if key and (val := os.getenv(key)) is not None:
            return float(val)
    return float(default)


# Momentum / adaptive guard knobs
MOMENTUM_NO_MIN_PRICE = _get_float(
    "MOMENTUM_NO_MIN_PRICE",
    "MOMENTUM_BUY_LOW_THRESHOLD_NO",
    default="0.85",
)
MOMENTUM_YES_MAX_PRICE = _get_float(
    "MOMENTUM_YES_MAX_PRICE",
    "MOMENTUM_BUY_LOW_THRESHOLD",
    default="0.15",
)
MOMENTUM_PRICE_BUFFER = float(os.getenv("MOMENTUM_PRICE_BUFFER", "0.01"))
MOMENTUM_TIMEOUT_SECONDS = int(os.getenv("MOMENTUM_TIMEOUT_SECONDS", "90"))
MOMENTUM_MAX_POSITIONS = int(os.getenv("MOMENTUM_MAX_POSITIONS", "3"))
MOMENTUM_SPREAD_MAX = float(os.getenv("MOMENTUM_SPREAD_MAX", "0.01"))
MOMENTUM_SPREAD_DISABLE = float(os.getenv("MOMENTUM_SPREAD_DISABLE", "0.05"))
ENABLE_MOMENTUM = os.getenv("ENABLE_MOMENTUM", "true").lower() == "true"

COMBINED_GUARD_WINDOW = int(os.getenv("COMBINED_GUARD_WINDOW", "40"))
COMBINED_GUARD_STD_MULT = float(os.getenv("COMBINED_GUARD_STD_MULT", "2.0"))
FEE_BUFFER_EXTRA = float(os.getenv("FEE_BUFFER_EXTRA", "0.0"))

MAX_UNHEDGED_EXPOSURE = float(os.getenv("MAX_UNHEDGED_EXPOSURE", "500"))
LOSS_LEASH = float(os.getenv("LOSS_LEASH", "-50"))
TRADING_MODE = os.getenv("TRADING_MODE", "ACTIVE").upper()
TRADE_LOG_PATH = os.getenv("TRADE_LOG_PATH", "data/trade_log.json")
MIN_PROBABILITY_FLOOR = float(os.getenv("MIN_PROBABILITY_FLOOR", "0.02"))

# --- MARKET OVERRIDES & SLUG PREFIXES ---
MARKET_SLUG = os.getenv("MARKET_SLUG")
MARKET_HANDOFF_BUFFER = int(os.getenv("MARKET_HANDOFF_BUFFER", "60"))
ETH_SLUG_PREFIX = os.getenv("ETH_SLUG_PREFIX", "eth-updown-15m")

BTC_MARKET_SLUG = os.getenv("BTC_MARKET_SLUG")
BTC_MARKET_HANDOFF_BUFFER = int(os.getenv("BTC_MARKET_HANDOFF_BUFFER", "60"))
BTC_SLUG_PREFIX = os.getenv("BTC_SLUG_PREFIX", "btc-updown-15m")

# --- MERGE / REDEMPTION SETTINGS ---
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC_DECIMALS = 6
CTF_CONTRACT_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
PARENT_COLLECTION_ID = "0x0000000000000000000000000000000000000000000000000000000000000000"
MERGE_PARTITION = [1, 2]
MERGE_GAS_LIMIT = 450000
