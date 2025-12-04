import asyncio
import os
import time
from colorama import Fore, Style, init
from web3 import Web3
import aiohttp
import requests
from config import REST_URL, WS_URL, RPC_URL, PRIVATE_KEY, POLYGON_ADDRESS
from config import API_KEY, API_SECRET, API_PASSPHRASE
from eip712_signer import OptimizedSigner
from utils import get_target_markets
import ujson

# Initialize colorama
init()

def print_status(component, status, message=""):
    if status == "OK":
        print(f"[{Fore.GREEN}OK{Style.RESET_ALL}] {component:<20} {message}")
    else:
        print(f"[{Fore.RED}FAIL{Style.RESET_ALL}] {component:<20} {message}")

async def test_rpc():
    print(f"\n--- Testing Polygon RPC Connection ---")
    try:
        w3 = Web3(Web3.HTTPProvider(RPC_URL))
        if w3.is_connected():
            chain_id = w3.eth.chain_id
            block = w3.eth.block_number
            print_status("RPC Connection", "OK", f"Connected to Chain ID: {chain_id}")
            print_status("Block Height", "OK", f"Current Block: {block}")
        else:
            print_status("RPC Connection", "FAIL", "Could not connect to RPC URL")
    except Exception as e:
        print_status("RPC Connection", "FAIL", str(e))

async def test_rest_api():
    print(f"\n--- Testing Polymarket REST API ---")
    try:
        resp = requests.get(f"{REST_URL}/markets?limit=1")
        if resp.status_code == 200:
            print_status("REST Endpoint", "OK", f"Status {resp.status_code}")
        else:
            print_status("REST Endpoint", "FAIL", f"Status {resp.status_code}")
            
        # Test Utils Market Discovery
        print("Testing Market Discovery (utils.py)...")
        markets = get_target_markets()
        if markets:
            print_status("Market Discovery", "OK", f"Found {len(markets)} active ETH markets")
            print(f"   Sample: {markets[0].get('question')}")
        else:
            print_status("Market Discovery", "WARN", "No active ETH markets found (might be off-hours or filter issue)")
            
    except Exception as e:
        print_status("REST API", "FAIL", str(e))

async def test_websocket():
    print(f"\n--- Testing WebSocket Connectivity ---")
    try:
        # NO AUTH HEADERS for public market data WebSocket
        print("[Info] Attempting connection WITHOUT auth headers (public channel)...")
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(WS_URL) as ws:
                print_status("WS Connection", "OK", "Connected successfully (No Auth)")
                
                # Send a ping
                await ws.ping()
                print_status("WS Ping", "OK", "Ping sent")
                
                # Send a subscription in the correct format (per real-time-data-client)
                sub_msg = {
                    "action": "subscribe",
                    "channel": "market",
                    "filters": ["2174263314346390629056905015582624153306727273689761438222190979671286099535"]
                }
                await ws.send_json(sub_msg)
                print_status("WS Subscribe", "OK", "Subscription message sent")
                
                # Wait brief moment to see if we get any data
                await asyncio.sleep(2)
                if not ws.closed:
                    print_status("WS Stability", "OK", "Connection stable for 2s")
                else:
                    print_status("WS Stability", "FAIL", "Connection closed unexpectedly")
    except Exception as e:
        print_status("Websocket", "FAIL", str(e))

def test_signer():
    print(f"\n--- Testing EIP-712 Signer ---")
    try:
        # Check if variables are loaded
        if not PRIVATE_KEY:
            print_status("Config", "FAIL", "PRIVATE_KEY not loaded from .env")
            return
            
        # Use config address or derive it if missing
        addr = POLYGON_ADDRESS
        if not addr:
            # Optional: Derive address from private key if not in .env
            from eth_account import Account
            acct = Account.from_key(PRIVATE_KEY)
            addr = acct.address
            print_status("Config", "OK", f"Derived Address: {addr}")

        signer = OptimizedSigner(PRIVATE_KEY, addr)
        
        # Generate dummy order
        payload = signer.generate_signed_order("1", "BUY", 0.50, 10)
        
        if payload and "signature" in payload:
            print_status("Signature Gen", "OK", f"Sig: {payload['signature'][:10]}...")
            print_status("Order Structure", "OK", f"Type: {payload['orderType']}")
        else:
            print_status("Signature Gen", "FAIL", "Payload missing signature")
            
    except Exception as e:
        print_status("Signer", "FAIL", str(e))

async def main():
    print(f"Starting System Check for Gabagool Bot v2.1")
    print(f"===========================================")
    
    await test_rpc()
    await test_rest_api()
    await test_websocket()
    test_signer()
    
    print(f"\n===========================================")
    print(f"Test Complete.")

if __name__ == "__main__":
    asyncio.run(main())
