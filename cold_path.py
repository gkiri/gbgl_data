import asyncio
import json
import statistics
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import requests  # type: ignore
from web3 import Web3  # type: ignore

from config import (
    API_KEY,
    API_PASSPHRASE,
    API_SECRET,
    COMBINED_GUARD_STD_MULT,
    COMBINED_GUARD_WINDOW,
    ENABLE_MOMENTUM,
    FEE_BUFFER_EXTRA,
    LOSS_LEASH,
    MAX_PAIR_COST,
    MAX_UNHEDGED_EXPOSURE,
    MERGE_GAS_LIMIT,
    MERGE_PARTITION,
    MOMENTUM_MAX_POSITIONS,
    MOMENTUM_NO_MIN_PRICE,
    MOMENTUM_PRICE_BUFFER,
    MOMENTUM_SPREAD_DISABLE,
    MOMENTUM_SPREAD_MAX,
    MOMENTUM_TIMEOUT_SECONDS,
    MOMENTUM_YES_MAX_PRICE,
    ORDER_SIZE,
    PARENT_COLLECTION_ID,
    POLYGON_ADDRESS,
    PRIVATE_KEY,
    REST_URL,
    TRADE_LOG_PATH,
    TRADING_MODE,
    USDC_ADDRESS,
    USDC_DECIMALS,
    CTF_CONTRACT_ADDRESS,
    MIN_PROBABILITY_FLOOR,
)
from eip712_signer import OptimizedSigner
from blockchain_monitor import BlockchainMonitor 
from colorama import Fore, Style, init  # type: ignore

init()

# Polymarket Contract Address (Legacy CTF Exchange)
CTF_EXCHANGE_ADDRESS = Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
MIN_BID = 0.01          # Absolute floor we ever bid (protects signer)
MIN_ACTIVE_BID = 0.01   # Minimum price worth arming to cover fees
BASE_FEE_BUFFER = 0.0   # Base slippage buffer before adaptive adjustments

CTF_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "collateralToken", "type": "address"},
            {"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
            {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
            {"internalType": "uint256[]", "name": "partition", "type": "uint256[]"},
            {"internalType": "uint256", "name": "amount", "type": "uint256"},
        ],
        "name": "mergePositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

class Strategist:
    def __init__(self, market_meta, sniper_queue, stop_event=None, price_cache=None):
        """
        market_meta: dict returned by utils.get_target_markets()
        """
        self.market_meta = market_meta or {}
        self.market_slug = self.market_meta.get("market_slug")
        self.condition_id = self.market_meta.get("condition_id") or self.market_meta.get("conditionId")
        self.question = self.market_meta.get("question", "Unknown Market")

        # Primary identifier preference: condition id -> slug -> fallback
        self.market_id = self.condition_id or self.market_slug or self.market_meta.get("id")
        self.fetch_identifiers = [identifier for identifier in [self.market_id, self.market_slug, self.condition_id] if identifier]
        self.market_endpoint_id = None

        self.sniper_queue = sniper_queue
        self.stop_event = stop_event or asyncio.Event()
        self.price_cache = price_cache or {}
        self.signer = OptimizedSigner(PRIVATE_KEY, POLYGON_ADDRESS)
        self.monitor = BlockchainMonitor()
        self.tokens = {}
        self.inventory = {'YES': 0, 'NO': 0}
        self.prev_inventory = {'YES': 0, 'NO': 0}
        self.auth_headers = {
            "Poly-Api-Key": API_KEY,
            "Poly-Api-Secret": API_SECRET,
            "Poly-Api-Passphrase": API_PASSPHRASE,
        }
        self.last_fee_log = 0
        self.last_inventory_refresh = 0
        self.owner_address = Web3.to_checksum_address(self.signer.maker_address)
        self.ctf_contract = self.monitor.w3.eth.contract(
            address=Web3.to_checksum_address(CTF_CONTRACT_ADDRESS),
            abi=CTF_ABI
        )
        self.dynamic_costs = deque(maxlen=COMBINED_GUARD_WINDOW)
        self.combined_std_mult = COMBINED_GUARD_STD_MULT
        self.fee_buffer = BASE_FEE_BUFFER + FEE_BUFFER_EXTRA

        self.enable_momentum = ENABLE_MOMENTUM
        self.momentum_no_min = MOMENTUM_NO_MIN_PRICE
        self.momentum_yes_max = MOMENTUM_YES_MAX_PRICE
        self.momentum_price_buffer = MOMENTUM_PRICE_BUFFER
        self.momentum_timeout = MOMENTUM_TIMEOUT_SECONDS
        self.momentum_max_positions = MOMENTUM_MAX_POSITIONS
        self.momentum_spread_max = MOMENTUM_SPREAD_MAX
        self.momentum_spread_disable = MOMENTUM_SPREAD_DISABLE
        self.max_unhedged = MAX_UNHEDGED_EXPOSURE
        self.min_probability_floor = max(MIN_BID, MIN_PROBABILITY_FLOOR)

        self.trading_mode = TRADING_MODE.upper()
        self.loss_leash = LOSS_LEASH
        self.realized_pnl = 0.0
        self.unmatched_yes = []
        self.unmatched_no = []
        self.pending_traps = {}
        self.last_signed_prices = {}
        self.last_unhedged_ts = 0
        self.momentum_disabled_reason = ""
        self.last_price_source = "REST"

        self.trade_log_path = Path(TRADE_LOG_PATH)
        self.trade_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.pnl_state_path = self.trade_log_path.with_suffix(".state.json")
        self._load_pnl_state()

    def _load_pnl_state(self):
        try:
            if self.pnl_state_path.exists():
                data = json.loads(self.pnl_state_path.read_text())
                self.realized_pnl = float(data.get("realized_pnl", 0.0))
        except Exception:
            self.realized_pnl = 0.0

    def _persist_pnl_state(self):
        try:
            self.pnl_state_path.write_text(
                json.dumps({"realized_pnl": self.realized_pnl}, indent=2)
            )
        except Exception:
            pass

    def fetch_market_details(self):
        """Resolves Token IDs for the specific market"""
        for identifier in self.fetch_identifiers:
            print(f"[{Fore.CYAN}Cold{Style.RESET_ALL}] Fetching details using {identifier}...")
            payload = self._fetch_market_payload(identifier)
            if not payload:
                continue

            tokens = payload.get("tokens") or []
            if len(tokens) < 2:
                continue

            self.tokens['YES'] = tokens[0]['token_id']
            self.tokens['NO'] = tokens[1]['token_id']
            self.market_endpoint_id = identifier
            print(f"[{Fore.CYAN}Cold{Style.RESET_ALL}] Configured: YES={self.tokens['YES'][:12]}... NO={self.tokens['NO'][:12]}...")
            return True

        print(f"[{Fore.RED}ERROR{Style.RESET_ALL}] Unable to resolve market tokens for identifiers: {self.fetch_identifiers}")
        return False

    def _fetch_market_payload(self, identifier):
        try:
            resp = requests.get(f"{REST_URL}/markets/{identifier}")
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            return data[0] if isinstance(data, list) else data
        except Exception:
            return None

    def fetch_live_prices(self):
        """Fetches the current 'Mid' or 'Last' price to use as a baseline."""
        identifier = self.market_endpoint_id or self.market_id
        if not identifier:
            return None, None
        
        yes_token = self.tokens.get("YES")
        no_token = self.tokens.get("NO")
        yes_price = self._latest_cached_price(yes_token)
        no_price = self._latest_cached_price(no_token)

        price_source = None
        if yes_price is not None and no_price is not None:
            price_source = "WS"
            final_yes, final_no = yes_price, no_price
        else:
            fallback_yes, fallback_no = self._fetch_rest_prices(identifier)

            if yes_price is None:
                yes_price = fallback_yes
            if no_price is None:
                no_price = fallback_no

            if yes_price is not None and no_price is not None:
                price_source = "REST"
                final_yes, final_no = yes_price, no_price
            else:
                return None, None

        if price_source and price_source != self.last_price_source:
            print(
                f"[{Fore.CYAN}Cold{Style.RESET_ALL}] 📡 Price source switched to "
                f"{price_source} data."
            )
            self.last_price_source = price_source

        return final_yes, final_no

    def _fetch_rest_prices(self, identifier):
        try:
            resp = requests.get(f"{REST_URL}/markets/{identifier}")
            data = resp.json()
            if isinstance(data, list):
                data = data[0]
            tokens = data.get("tokens", [])

            if len(tokens) >= 2:
                yes_price = float(tokens[0].get("price", 0.50))
                no_price = float(tokens[1].get("price", 0.50))
                return yes_price, no_price
        except Exception as e:
            print(f"[{Fore.RED}WARN{Style.RESET_ALL}] Could not fetch fallback prices: {e}")
        return None, None

    def _latest_cached_price(self, token_id):
        if not token_id or not self.price_cache:
            return None
        entry = self.price_cache.get(token_id)
        if not entry:
            return None
        if time.time() - entry.get("ts", 0) > 5:
            return None
        price = entry.get("best_ask")
        if price is None:
            price = entry.get("best_bid")
        try:
            return float(price) if price is not None else None
        except (TypeError, ValueError):
            return None

    def refresh_inventory(self):
        """
        Fetches current user positions to see if we need to merge or
        update risk state.
        """
        if not self.tokens:
            return
        now_ts = time.time()
        if now_ts - self.last_inventory_refresh < 10:
            return
        try:
            params = {"limit": 50, "closed": "false"}
            resp = requests.get(
                f"{REST_URL}/positions", headers=self.auth_headers, params=params
            )

            if resp.status_code == 200:
                data = resp.json()
                yes_bal = 0.0
                no_bal = 0.0

                for pos in data:
                    asset_id = pos.get("asset_id")
                    size = float(pos.get("size", 0))
                    if asset_id == self.tokens.get("YES"):
                        yes_bal = size
                    elif asset_id == self.tokens.get("NO"):
                        no_bal = size

                prev_yes = self.inventory["YES"]
                prev_no = self.inventory["NO"]
                self.inventory["YES"] = yes_bal
                self.inventory["NO"] = no_bal

                delta_yes = yes_bal - prev_yes
                delta_no = no_bal - prev_no

                if delta_yes > 0:
                    self._record_fill("YES", delta_yes)
                elif delta_yes < 0:
                    self._consume_unmatched("YES", abs(delta_yes))

                if delta_no > 0:
                    self._record_fill("NO", delta_no)
                elif delta_no < 0:
                    self._consume_unmatched("NO", abs(delta_no))

                if yes_bal > 0 or no_bal > 0:
                    print(
                        f"[{Fore.MAGENTA}Inventory{Style.RESET_ALL}] YES: {yes_bal} | NO: {no_bal}"
                    )
                self.last_inventory_refresh = now_ts
                self._evaluate_unhedged_state()

        except Exception:
            pass

    def prepare_ammo(self, current_yes_price, current_no_price):
        """Calculates targets and pre-signs orders."""
        
        combined_price = current_yes_price + current_no_price
        self.dynamic_costs.append(combined_price)
        std_dev = statistics.pstdev(self.dynamic_costs) if len(self.dynamic_costs) > 2 else 0.0
        adaptive_cap = MAX_PAIR_COST - (
            self.fee_buffer + (self.combined_std_mult * std_dev)
        )
        effective_cap = max(MIN_BID * 2, min(MAX_PAIR_COST, adaptive_cap))
        raw_target_yes = effective_cap - current_no_price
        raw_target_no = effective_cap - current_yes_price

        target_price_yes = round(max(raw_target_yes, MIN_BID), 4)
        target_price_no = round(max(raw_target_no, MIN_BID), 4)

        yes_active = target_price_yes >= MIN_ACTIVE_BID
        no_active = target_price_no >= MIN_ACTIVE_BID

        yes_prob_warn = target_price_yes < self.min_probability_floor
        no_prob_warn = target_price_no < self.min_probability_floor

        status_yes = "ARMED" if yes_active else "SKIPPED"
        status_no = "ARMED" if no_active else "SKIPPED"
        print(
            f"[{Fore.CYAN}Cold{Style.RESET_ALL}] "
            f"(combo {combined_price:.3f}, cap {effective_cap:.3f}) "
            f"YES {current_yes_price:.2f}→{target_price_yes:.2f} ({status_yes}), "
            f"NO {current_no_price:.2f}→{target_price_no:.2f} ({status_no})"
        )

        if yes_prob_warn:
            print(
                f"[{Fore.YELLOW}GUARD{Style.RESET_ALL}] YES target "
                f"{target_price_yes:.2f} < probability floor "
                f"{self.min_probability_floor:.2f}; safety bid only."
            )

        if no_prob_warn:
            print(
                f"[{Fore.YELLOW}GUARD{Style.RESET_ALL}] NO target "
                f"{target_price_no:.2f} < probability floor "
                f"{self.min_probability_floor:.2f}; safety bid only."
            )

        if not yes_active and not no_active:
            print(f"[{Fore.YELLOW}WARN{Style.RESET_ALL}] Both bids below MIN_ACTIVE_BID ({MIN_ACTIVE_BID:.2f}); standing down.")
            return

        if yes_active:
            self._queue_trap(
                token_id=self.tokens["YES"],
                side="YES",
                price=target_price_yes,
                mode="PAIR",
            )
        else:
            print(
                f"[{Fore.YELLOW}SKIP{Style.RESET_ALL}] YES target {target_price_yes:.2f} < MIN_ACTIVE_BID {MIN_ACTIVE_BID:.2f}"
            )

        if no_active:
            self._queue_trap(
                token_id=self.tokens["NO"],
                side="NO",
                price=target_price_no,
                mode="PAIR",
            )
        else:
            print(
                f"[{Fore.YELLOW}SKIP{Style.RESET_ALL}] NO target {target_price_no:.2f} < MIN_ACTIVE_BID {MIN_ACTIVE_BID:.2f}"
            )

        self._maybe_queue_momentum_orders(current_yes_price, current_no_price)

    def _queue_trap(self, token_id, side, price, mode):
        if token_id in self.sniper_queue:
            return
        try:
            payload = self.signer.generate_signed_order(
                token_id=token_id, side="BUY", price=price, size=ORDER_SIZE
            )
            self.sniper_queue[token_id] = {
                "trigger_price": price,
                "payload": payload,
                "side": side,
                "mode": mode,
            }
            self.pending_traps[token_id] = {
                "price": price,
                "mode": mode,
                "side": side,
                "ts": time.time(),
            }
            self.last_signed_prices[side] = price
            print(
                f"[{Fore.GREEN}LOAD{Style.RESET_ALL}] {mode} {side} trap ≤ {price:.2f} "
                f"(asset {token_id[:10]}…)"
            )
        except Exception as exc:
            print(f"[{Fore.RED}Error{Style.RESET_ALL}] Failed signing {side}: {exc}")

    def _momentum_capacity_available(self):
        open_positions = len(self.unmatched_yes) + len(self.unmatched_no)
        return open_positions < self.momentum_max_positions

    def _maybe_queue_momentum_orders(self, yes_price, no_price):
        if not (
            self.enable_momentum
            and self.trading_mode == "ACTIVE"
            and self._momentum_capacity_available()
        ):
            return

        if yes_price is None or no_price is None:
            return

        spread = abs((yes_price + no_price) - 1.0)
        if spread >= self.momentum_spread_disable:
            print(
                f"[{Fore.YELLOW}MOMENTUM{Style.RESET_ALL}] Spread {spread:.3f} "
                f"≥ disable threshold {self.momentum_spread_disable:.3f}; "
                "deferring momentum."
            )
            return

        if spread > self.momentum_spread_max:
            print(
                f"[{Fore.YELLOW}MOMENTUM{Style.RESET_ALL}] Spread {spread:.3f} "
                f"> guard {self.momentum_spread_max:.3f}; waiting for tighter book."
            )
            return

        net_delta = abs(self.inventory["YES"] - self.inventory["NO"])
        if net_delta >= self.max_unhedged:
            return

        if yes_price is not None and yes_price <= self.momentum_yes_max:
            price = round(max(MIN_BID, yes_price - self.momentum_price_buffer), 4)
            self._queue_trap(
                token_id=self.tokens["YES"],
                side="YES",
                price=price,
                mode="MOMENTUM_YES",
            )

        if no_price is not None and no_price >= self.momentum_no_min:
            price = round(min(0.99, no_price + self.momentum_price_buffer), 4)
            self._queue_trap(
                token_id=self.tokens["NO"],
                side="NO",
                price=price,
                mode="MOMENTUM_NO",
            )

    def merge_positions(self):
        """
        Executes the merge if we have matching pairs.
        """
        merge_amount = min(self.inventory['YES'], self.inventory['NO'])
        if merge_amount <= 0.1:
            return

        self.execute_merge(merge_amount)

    def execute_merge(self, merge_amount):
        if not self.condition_id:
            print(f"[{Fore.RED}ERROR{Style.RESET_ALL}] Cannot merge without condition_id")
            return

        amount_units = int(merge_amount * (10 ** USDC_DECIMALS))
        if amount_units <= 0:
            return

        print(f"[{Fore.GREEN}ACTION{Style.RESET_ALL}] 🚀 Merging {merge_amount:.4f} YES/NO sets ({amount_units} units)")

        try:
            parent_bytes = Web3.to_bytes(hexstr=PARENT_COLLECTION_ID)
            condition_bytes = Web3.to_bytes(hexstr=self.condition_id)

            tx = self.ctf_contract.functions.mergePositions(
                Web3.to_checksum_address(USDC_ADDRESS),
                parent_bytes,
                condition_bytes,
                MERGE_PARTITION,
                amount_units
            ).build_transaction({
                "from": self.owner_address,
                "nonce": self.monitor.w3.eth.get_transaction_count(self.owner_address),
                "gas": MERGE_GAS_LIMIT,
                "gasPrice": self.monitor.w3.eth.gas_price,
                "chainId": self.monitor.w3.eth.chain_id,
            })

            signed = self.monitor.w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
            tx_hash = self.monitor.w3.eth.send_raw_transaction(signed.rawTransaction)
            print(f"[{Fore.GREEN}Merge Tx{Style.RESET_ALL}] Submitted {tx_hash.hex()}")

            receipt = self.monitor.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
            if receipt.status == 1:
                print(f"[{Fore.GREEN}Merge Success{Style.RESET_ALL}] Confirmed in block {receipt.blockNumber}")
                self.inventory['YES'] -= merge_amount
                self.inventory['NO'] -= merge_amount
            else:
                print(f"[{Fore.RED}Merge Failed{Style.RESET_ALL}] Tx reverted {tx_hash.hex()}")

        except Exception as e:
            print(f"[{Fore.RED}Merge Error{Style.RESET_ALL}] {e}")
            
    def check_blockchain_health_and_fees(self):
        """Uses RPC methods for status and fee calculation."""
        now_ts = time.time()
        if now_ts - self.last_fee_log < 30:
            return

        try:
            fee_data = self.monitor.get_fee_history(num_blocks=5, percentiles=[50])

            def safe_int(val):
                if isinstance(val, str):
                    return int(val, 16) if val.startswith("0x") else int(val)
                return int(val)

            latest_base_fee = safe_int(fee_data['baseFeePerGas'][-1])
            base_fee = latest_base_fee / 10**9
            print(f"[{Fore.BLUE}Fees{Style.RESET_ALL}] Base Fee: {base_fee:.2f} gwei")
            self.last_fee_log = now_ts
        except Exception as e:
            print(f"[{Fore.RED}WARNING{Style.RESET_ALL}] Failed to fetch fee history: {e}")

    def _record_fill(self, side, size):
        token_id = self.tokens.get(side)
        trap_meta = self.pending_traps.pop(token_id, None) if token_id else None
        price = (trap_meta or {}).get("price", self.last_signed_prices.get(side, 0.5))
        entry = {"size": size, "price": price, "ts": time.time()}
        storage = self.unmatched_yes if side == "YES" else self.unmatched_no
        storage.append(entry)
        self._log_trade(side, price, size, trap_meta["mode"] if trap_meta else "fill")
        self._match_pairs()

    def _consume_unmatched(self, side, qty):
        storage = self.unmatched_yes if side == "YES" else self.unmatched_no
        remaining = qty
        while storage and remaining > 0:
            head = storage[0]
            take = min(head["size"], remaining)
            head["size"] -= take
            remaining -= take
            if head["size"] <= 1e-9:
                storage.pop(0)
        self._evaluate_unhedged_state()

    def _log_trade(self, side, price, size, event):
        record = {
            "ts": datetime.utcnow().isoformat(),
            "market": self.market_slug,
            "side": side,
            "price": price,
            "size": size,
            "event": event,
            "pnl": self.realized_pnl,
        }
        try:
            with self.trade_log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record) + "\n")
        except Exception:
            pass

    def _match_pairs(self):
        while self.unmatched_yes and self.unmatched_no:
            yes_entry = self.unmatched_yes[0]
            no_entry = self.unmatched_no[0]
            size = min(yes_entry["size"], no_entry["size"])
            cost = yes_entry["price"] + no_entry["price"]
            pnl = (1 - cost) * size
            self.realized_pnl += pnl
            yes_entry["size"] -= size
            no_entry["size"] -= size
            if yes_entry["size"] <= 1e-9:
                self.unmatched_yes.pop(0)
            if no_entry["size"] <= 1e-9:
                self.unmatched_no.pop(0)
            self._log_trade("PAIR", cost, size, "pair-realized")

        self._persist_pnl_state()
        self._evaluate_profit_leash()
        self._evaluate_unhedged_state()

    def _evaluate_profit_leash(self):
        if self.realized_pnl <= self.loss_leash and self.trading_mode != "PASSIVE":
            self.trading_mode = "PASSIVE"
            print(
                f"[{Fore.RED}RISK{Style.RESET_ALL}] Loss leash breached "
                f"({self.realized_pnl:.2f}); switching to PASSIVE mode."
            )

    def _evaluate_unhedged_state(self):
        net_delta = abs(self.inventory["YES"] - self.inventory["NO"])
        if net_delta > 0 and self.last_unhedged_ts == 0:
            self.last_unhedged_ts = time.time()
        elif net_delta == 0:
            self.last_unhedged_ts = 0

        if net_delta >= self.max_unhedged:
            if self.enable_momentum:
                print(
                    f"[{Fore.RED}RISK{Style.RESET_ALL}] Net delta {net_delta} exceeds "
                    f"limit {self.max_unhedged}; disabling momentum."
                )
            self.enable_momentum = False
            self.momentum_disabled_reason = "max_unhedged"
        elif (
            not self.enable_momentum
            and self.momentum_disabled_reason == "max_unhedged"
            and net_delta < self.max_unhedged / 2
        ):
            print(
                f"[{Fore.GREEN}RISK{Style.RESET_ALL}] Exposure cooled; "
                "re-enabling momentum."
            )
            self.enable_momentum = True
            self.momentum_disabled_reason = ""

        if (
            self.last_unhedged_ts
            and net_delta > 0
            and time.time() - self.last_unhedged_ts > self.momentum_timeout
        ):
            if self.enable_momentum:
                print(
                    f"[{Fore.RED}RISK{Style.RESET_ALL}] Exposure idle for "
                    f"{self.momentum_timeout}s; disabling momentum."
                )
            self.enable_momentum = False
            self.momentum_disabled_reason = "timeout"

    async def run_loop(self):
        if not self.fetch_market_details():
            print(f"[{Fore.RED}ERROR{Style.RESET_ALL}] Aborting strategist loop; no tokens loaded.")
            return
        
        while not self.stop_event.is_set():
            try:
                # 1. Update Prices
                yes_price, no_price = self.fetch_live_prices()
                
                # 2. Check Inventory (Are we holding bags?)
                self.refresh_inventory()
                
                # 3. Prepare Traps (only if we have valid price data)
                if yes_price is not None and no_price is not None:
                    self.prepare_ammo(yes_price, no_price)
                
                # 4. Monitor & Merge
                self.check_blockchain_health_and_fees()
                self.merge_positions()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[{Fore.RED}CRITICAL{Style.RESET_ALL}] Error in Cold Path: {e}")
            
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=2)
            except asyncio.TimeoutError:
                continue
            break