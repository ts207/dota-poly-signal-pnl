import asyncio
import os
from py_clob_client_v2 import ClobClient, ApiCreds
from dotenv import load_dotenv

load_dotenv()

async def main():
    host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com")
    chain_id = int(os.getenv("POLY_CHAIN_ID", "137"))
    private_key = os.getenv("POLY_PRIVATE_KEY")
    creds = ApiCreds(
        api_key=os.getenv("POLY_CLOB_API_KEY"),
        api_secret=os.getenv("POLY_CLOB_SECRET"),
        api_passphrase=os.getenv("POLY_CLOB_PASS_PHRASE"),
    )
    
    # Try with sig_type=1 and NO funder
    client = ClobClient(host, chain_id, private_key, creds, signature_type=1)
    print(f"Client Address: {client.get_address()}")
    
    try:
        # Check market info for the Liquid vs Falcons Map 1 market
        condition_id = "0x77012ddcfc76052297c01a380f4ebfe09b094be8e3b39a14f7944c058bc2dcf2"
        print(f"Fetching market info for {condition_id}...")
        resp = await asyncio.to_thread(client.get_market, condition_id)
        print(f"Market Info: {resp}")
        
        # Check my own profile or trades
        print("Fetching trades...")
        trades = await asyncio.to_thread(client.get_trades)
        print(f"Recent Trades: {trades}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
