import asyncio
import os
import aiohttp
from live_executor import LiveCLOBClient
from dotenv import load_dotenv
from eth_account import Account

load_dotenv()

async def main():
    pk = os.getenv("POLY_PRIVATE_KEY")
    addr = Account.from_key(pk).address
    print(f"Signer Address: {addr}")
    print(f"Funder Address: {os.getenv('POLY_FUNDER_ADDRESS')}")
    
    try:
        client = LiveCLOBClient()
        print("Client initialized.")
        # Try a safe read call
        print("Fetching ok status...")
        # Note: buy_fak_market uses create_and_post_market_order
        # Let's try to get balance or something safe
        # The client has a _client which is ClobClient
        balance = await asyncio.to_thread(client._client.get_balance, token_id="2045565837370551909989536379865102671584313967899298859459028552120513333778")
        print(f"Balance: {balance}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
