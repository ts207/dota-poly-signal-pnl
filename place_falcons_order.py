import asyncio
import os
import aiohttp
import json
import time
from datetime import datetime
from dotenv import load_dotenv

# Import internal modules from the same directory
import sys
sys.path.append(os.getcwd())
from book_refresh import fetch_fresh_book

load_dotenv()

# Falcons vs Liquid Game 1 - Falcons is NO
FALCONS_TOKEN_ID = "46763590797788703854129285573729309252762804441090608023815267104714686201356"
AMOUNT_USD = 1.0

async def place_manual_order(token_id, amount):
    print(f"\n--- Placing manual order for Falcons (Map 1) ---")
    
    from py_clob_client_v2 import ApiCreds, ClobClient, MarketOrderArgs, OrderType, PartialCreateOrderOptions, Side
    
    host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com")
    chain_id = int(os.getenv("POLY_CHAIN_ID", "137"))
    private_key = os.getenv("POLY_PRIVATE_KEY")
    creds = ApiCreds(
        api_key=os.getenv("POLY_CLOB_API_KEY"),
        api_secret=os.getenv("POLY_CLOB_SECRET"),
        api_passphrase=os.getenv("POLY_CLOB_PASS_PHRASE"),
    )
    sig_type = int(os.getenv("POLY_SIGNATURE_TYPE", "2"))
    funder = os.getenv("POLY_FUNDER_ADDRESS")
    
    kwargs = {
        "host": host,
        "chain_id": chain_id,
        "key": private_key,
        "creds": creds,
        "signature_type": sig_type,
        "funder": funder
    }
    
    print(f"Initializing client with sig_type={sig_type} and funder={funder}...")
    client = ClobClient(**kwargs)
    print(f"Client Address: {client.get_address()}")
    
    async with aiohttp.ClientSession() as session:
        book = await fetch_fresh_book(session, token_id)
        best_ask = book.get("best_ask")
        if best_ask is None:
            print("No ask found for Falcons token.")
            return
        
        print(f"Current best ask for Falcons: {best_ask}")
        price_cap = round(best_ask + 0.05, 2) 
        if price_cap > 0.99:
            price_cap = 0.99
            
        print(f"Placing order at price cap: {price_cap}")
        
        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=float(amount),
            side=Side.BUY,
            price=float(price_cap),
        )
        options = PartialCreateOrderOptions(tick_size="0.01", neg_risk=False)
        
        try:
            resp = await asyncio.to_thread(
                client.create_and_post_market_order,
                order_args=order_args,
                options=options,
                order_type=OrderType.FAK,
            )
            print("Response:")
            print(json.dumps(resp, indent=2))
            
            # Log to manual_orders.jsonl
            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "token_id": token_id,
                "team": "Falcons",
                "market": "Liquid vs Falcons Map 1",
                "amount_usd": amount,
                "best_ask_at_time": best_ask,
                "price_cap": price_cap,
                "response": resp
            }
            os.makedirs("logs", exist_ok=True)
            with open("logs/manual_orders.jsonl", "a") as f:
                f.write(json.dumps(log_entry) + "\n")
                
        except Exception as e:
            print(f"Error placing order: {e}")

if __name__ == "__main__":
    asyncio.run(place_manual_order(FALCONS_TOKEN_ID, AMOUNT_USD))
