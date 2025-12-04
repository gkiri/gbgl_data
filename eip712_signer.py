import time
import random
from eth_account import Account
from eth_account.messages import encode_typed_data
from config import CHAIN_ID

class OptimizedSigner:
    def __init__(self, private_key, maker_address):
        self.private_key = private_key
        if maker_address:
            self.maker_address = maker_address
        else:
            derived = Account.from_key(private_key)
            self.maker_address = derived.address
        self.domain = {
            "name": "ClobAuthDomain",
            "version": "1",
            "chainId": CHAIN_ID,
            "verifyingContract": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
        }
        
        self.types = {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Order": [
                {"name": "salt", "type": "uint256"},
                {"name": "maker", "type": "address"},
                {"name": "signer", "type": "address"},
                {"name": "taker", "type": "address"},
                {"name": "tokenId", "type": "uint256"},
                {"name": "makerAmount", "type": "uint256"},
                {"name": "takerAmount", "type": "uint256"},
                {"name": "expiration", "type": "uint256"},
                {"name": "nonce", "type": "uint256"},
                {"name": "feeRateBps", "type": "uint256"},
                {"name": "side", "type": "uint256"},
                {"name": "signatureType", "type": "uint256"},
                {"name": "checkMakerValidity", "type": "bool"},
            ]
        }

    def generate_signed_order(self, token_id, side, price, size):
        side_int = 0 if side.upper() == "BUY" else 1
        
        if side_int == 0: # BUY
            maker_amount = int((price * size) * 1_000_000) 
            taker_amount = int(size * 1_000_000) 
        else: # SELL
            maker_amount = int(size * 1_000_000)
            taker_amount = int((price * size) * 1_000_000)

        salt = int(time.time() * 1000) + random.randint(0, 10000)
        nonce = 0 

        order_struct = {
            "salt": salt,
            "maker": self.maker_address,
            "signer": self.maker_address,
            "taker": "0x0000000000000000000000000000000000000000",
            "tokenId": int(token_id),
            "makerAmount": maker_amount,
            "takerAmount": taker_amount,
            "expiration": 0,
            "nonce": nonce,
            "feeRateBps": 0,
            "side": side_int,
            "signatureType": 0,
            "checkMakerValidity": False 
        }

        structured_data = {
            "types": self.types,
            "domain": self.domain,
            "primaryType": "Order",
            "message": order_struct
        }
        
        signable_msg = encode_typed_data(full_message=structured_data)
        signed_msg = Account.sign_message(signable_msg, self.private_key)
        
        payload = {
            "order": {
                "salt": order_struct["salt"],
                "maker": order_struct["maker"],
                "signer": order_struct["signer"],
                "taker": order_struct["taker"],
                "tokenId": str(order_struct["tokenId"]),
                "makerAmount": str(order_struct["makerAmount"]),
                "takerAmount": str(order_struct["takerAmount"]),
                "expiration": str(order_struct["expiration"]),
                "nonce": str(order_struct["nonce"]),
                "feeRateBps": str(order_struct["feeRateBps"]),
                "side": str(order_struct["side"]),
                "signatureType": order_struct["signatureType"],
                "checkMakerValidity": order_struct["checkMakerValidity"]
            },
            "owner": self.maker_address,
            "signature": signed_msg.signature.hex(),
            "orderType": "FOK"
        }
        
        return payload