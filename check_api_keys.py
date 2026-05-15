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
        api_key="734311c5-5b4e-9f30-7a67-3678dc9e6703",
        api_secret=os.getenv("POLY_CLOB_SECRET"),
        api_passphrase=os.getenv("POLY_CLOB_PASS_PHRASE"),
    )
    sig_type = 2 # POLY_1271
    
    client = ClobClient(host, chain_id, private_key, creds, signature_type=sig_type)
    print(f"Signer Address: {client.get_address()}")
    
    try:
        # Check if the API key is valid and see what address it belongs to
        # Note: there isn't a direct get_api_key but there is get_account or similar
        resp = await asyncio.to_thread(client.get_api_keys)
        print("API Keys:")
        print(resp)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
