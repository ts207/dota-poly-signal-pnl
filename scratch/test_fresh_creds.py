import asyncio
import os
import aiohttp
from live_executor import LiveCLOBClient
from book_refresh import fetch_fresh_book
from dotenv import load_dotenv

load_dotenv()

YES_TOKEN_ID = "2045565837370551909989536379865102671584313967899298859459028552120513333778"
AMOUNT_USD = 1.0

async def test_order(token_id, amount, price_cap, use_funder=True):
    print(f"\n--- Testing order (use_funder={use_funder}) ---")
    
    from py_clob_client_v2 import ApiCreds, ClobClient, MarketOrderArgs, OrderType, PartialCreateOrderOptions, Side
    
    host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com")
    chain_id = int(os.getenv("POLY_CHAIN_ID", "137"))
    private_key = os.getenv("POLY_PRIVATE_KEY")
    
    # Try with sig_type=3 as specified in .env
    sig_type = int(os.getenv("POLY_SIGNATURE_TYPE", "3"))
    
    # FIRST: Derive credentials dynamically
    print(f"Deriving fresh credentials for {sig_type}...")
    temp_client = ClobClient(host, chain_id, private_key, signature_type=sig_type)
    try:
        creds = temp_client.create_or_derive_api_key()
        print(f"Derived API Key: {creds.api_key}")
    except Exception as e:
        print(f"Derivation failed: {e}")
        return

    kwargs = {
        "host": host,
        "chain_id": chain_id,
        "key": private_key,
        "creds": creds,
        "signature_type": sig_type
    }
    
    if use_funder:
        funder = os.getenv("POLY_FUNDER_ADDRESS")
        if funder:
            kwargs["funder"] = funder
            print(f"Using funder: {funder}")
    
    client = ClobClient(**kwargs)
    print(f"Client Address: {client.get_address()}")
    
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
        print(resp)
    except Exception as e:
        print(f"Error: {e}")

async def main():
    async with aiohttp.ClientSession() as session:
        book = await fetch_fresh_book(session, YES_TOKEN_ID)
        best_ask = book.get("best_ask")
        if best_ask is None:
            print("No ask found")
            return
        price_cap = round(best_ask + 0.05, 2)
        
        # Try with funder as per .env
        await test_order(YES_TOKEN_ID, AMOUNT_USD, price_cap, use_funder=True)

if __name__ == "__main__":
    asyncio.run(main())
