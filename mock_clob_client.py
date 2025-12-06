import os
from typing import Dict, List, Optional, Any
import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
import threading

@dataclass
class PaperOrder:
    order_id: str
    token_id: str
    side: str  # 'BUY' or 'SELL'
    price: float
    size: float
    status: str = "open"  # open, matched, cancelled
    filled_size: float = 0.0
    timestamp: float = 0.0

@dataclass
class PaperBalance:
    cash: float
    positions: Dict[str, float] = None  # token_id -> size

    def __post_init__(self):
        if self.positions is None:
            self.positions = {}

class PaperTracker:
    """
    Tracks paper trading positions and PnL.
    """
    def __init__(self, initial_balance: float = 1000.0, log_dir: str = "data/paper_logs"):
        self.balance = PaperBalance(cash=initial_balance)
        self.orders: Dict[str, PaperOrder] = {}
        self.trades: List[Dict] = []
        
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.trades_file = self.log_dir / f"paper_trades_{self.session_id}.jsonl"
        self._lock = threading.Lock()
        
        print(f"[PaperTracker] 📝 Initialized with ${initial_balance:.2f}")

    def add_order(self, order: PaperOrder):
        with self._lock:
            self.orders[order.order_id] = order
            # Lock funds for buys (simplified)
            if order.side == "BUY":
                cost = order.price * order.size
                # In a real sim we might segregate 'locked' vs 'available', 
                # here we just check if we have enough cash roughly, or allow negative for simplicity
                # self.balance.cash -= cost 
                pass

    def cancel_order(self, order_id: str) -> bool:
        with self._lock:
            if order_id in self.orders and self.orders[order_id].status == "open":
                self.orders[order_id].status = "cancelled"
                return True
            return False

    def record_fill(self, order_id: str, fill_price: float, fill_size: float):
        with self._lock:
            order = self.orders.get(order_id)
            if not order:
                return
            
            # Update order
            order.filled_size += fill_size
            if order.filled_size >= order.size:
                order.status = "matched"
            
            # Update balance
            cost = fill_price * fill_size
            if order.side == "BUY":
                self.balance.cash -= cost
                self.balance.positions[order.token_id] = self.balance.positions.get(order.token_id, 0.0) + fill_size
            else:
                self.balance.cash += cost
                self.balance.positions[order.token_id] = self.balance.positions.get(order.token_id, 0.0) - fill_size

            # Log trade
            trade_record = {
                "ts": time.time(),
                "order_id": order_id,
                "token_id": order.token_id,
                "side": order.side,
                "price": fill_price,
                "size": fill_size,
                "cash_balance": self.balance.cash,
                "positions": self.balance.positions.copy()
            }
            self.trades.append(trade_record)
            self._log_to_file(trade_record)
            
            print(f"[PaperTrade] 💰 FILLED: {order.side} {fill_size} @ {fill_price:.2f} (Cash: ${self.balance.cash:.2f})")

    def _log_to_file(self, record: Dict):
        try:
            with open(self.trades_file, "a") as f:
                f.write(json.dumps(record) + "\n")
        except Exception as e:
            print(f"[PaperTracker] Log error: {e}")

    def get_portfolio_value(self, market_prices: Dict[str, float]) -> float:
        """Calculate total portfolio value based on current market prices"""
        val = self.balance.cash
        for tid, size in self.balance.positions.items():
            price = market_prices.get(tid, 0.0)
            val += size * price
        return val

class MockClobClient:
    """
    Mocks ClobClient for paper trading.
    """
    def __init__(self, chain_id: int = 137, tracker: Optional[PaperTracker] = None):
        self.chain_id = chain_id
        self.tracker = tracker or PaperTracker()
        self.creds = type("Creds", (), {"api_key": "PAPER_KEY", "api_secret": "PAPER_SECRET", "api_passphrase": "PAPER_PASS"})()
        # Store live market data interface if needed, or let strategy handle updates
        # We will let the strategy call 'update_market_books' using real client logic?
        # Wait, if we mock the client, we can't fetch real books via THIS client.
        # The MakerStrategy uses self.clob_client for BOTH data fetching and order placement.
        # So this Mock needs to passthrough 'get_order_book' etc. to a REAL client or handle it.
        
        # Solution: We will keep a 'real_client' instance internally for reads, 
        # and mock the writes.
        from py_clob_client.client import ClobClient
        from config import PRIVATE_KEY, POLYGON_ADDRESS, SIGNATURE_TYPE
        
        print("[MockClient] 🔌 Connecting read-only real client...")
        self._real_client = ClobClient(
            "https://clob.polymarket.com",
            key=PRIVATE_KEY,
            chain_id=chain_id,
            funder=POLYGON_ADDRESS,
            signature_type=SIGNATURE_TYPE,
        )
        try:
            self._real_client.set_api_creds(self._real_client.create_or_derive_api_creds())
        except Exception as e:
            print(f"[MockClient] ⚠️ Read-only auth warning: {e}")

    def create_or_derive_api_creds(self):
        return self.creds

    def set_api_creds(self, creds):
        pass

    # --- READ METHODS (Passthrough) ---
    def get_market(self, condition_id):
        return self._real_client.get_market(condition_id)

    def get_order_book(self, token_id):
        return self._real_client.get_order_book(token_id)

    def get_last_trade_price(self, token_id):
        return self._real_client.get_last_trade_price(token_id)

    # --- WRITE METHODS (Mocked) ---
    def create_order(self, order_args):
        """Return a dict that looks like a signed order but isn't."""
        # order_args is typically OrderArgs object
        return {
            "token_id": order_args.token_id,
            "price": order_args.price,
            "size": order_args.size,
            "side": order_args.side,  # 'BUY' or 'SELL' constant (usually 0 or 1)
            "expiration": 0,
            "nonce": int(time.time() * 1000)
        }

    def post_order(self, order, orderType=None):
        """Simulate posting an order."""
        import uuid
        
        # Determine side string
        side_str = "BUY" # Assuming BUY for maker strategy logic usually
        # If 'order' is the dict we returned from create_order:
        if hasattr(order, 'side'): 
             # If it's an object
             side_str = "BUY" if order.side == 0 else "SELL" # Check constants
        elif isinstance(order, dict):
             # Start with assumption, fix if constants are imported
             # In py_clob_client, BUY=0, SELL=1 usually
             side_str = "BUY" if order.get('side') == 0 else "SELL" 
             if order.get('side') == 'BUY': side_str = 'BUY'

        price = float(order.get('price', 0.0) if isinstance(order, dict) else order.price)
        size = float(order.get('size', 0.0) if isinstance(order, dict) else order.size)
        token_id = order.get('token_id', '') if isinstance(order, dict) else order.token_id

        order_id = f"paper_{uuid.uuid4().hex[:8]}"
        
        paper_order = PaperOrder(
            order_id=order_id,
            token_id=token_id,
            side=side_str,
            price=price,
            size=size,
            timestamp=time.time()
        )
        
        self.tracker.add_order(paper_order)
        
        print(f"[MockClient] 📝 Order Posted: {side_str} {size} @ {price:.2f} (ID: {order_id})")
        
        return {"orderID": order_id, "status": "live"}

    def cancel_order(self, order_id):
        success = self.tracker.cancel_order(order_id)
        if success:
            print(f"[MockClient] ❌ Order Cancelled: {order_id}")
        return {"status": "cancelled" if success else "not_found"}

    def cancel_orders(self, order_ids):
        for oid in order_ids:
            self.cancel_order(oid)
        return {"status": "cancelled"}
    
    def cancel(self, order_id):
        return self.cancel_order(order_id)

