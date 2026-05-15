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
    
    client = ClobClient(host, chain_id, private_key, creds)
    
    try:
        # Some versions of the client have get_proxy or similar
        # Let's try to see if we can find it
        print("Attempting to find proxy address...")
        # Check if get_proxy exists
        if hasattr(client, "get_proxy"):
            proxy = await asyncio.to_thread(client.get_proxy)
            print(f"Proxy address: {proxy}")
        else:
            print("get_proxy not found on client.")
            
        # Try to call get_account
        resp = await asyncio.to_thread(client.get_account)
        print(f"Account Info: {resp}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
