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
    
    # Try with sig_type=2 and funder as they are in .env (well, currently sig_type=0 in .env)
    client = ClobClient(host, chain_id, private_key, creds, signature_type=2, funder=os.getenv("POLY_FUNDER_ADDRESS"))
    
    try:
        print("Fetching balance allowance...")
        # In V2, it might take a BalanceAllowanceParams object
        from py_clob_client_v2 import BalanceAllowanceParams, AssetType
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        resp = await asyncio.to_thread(client.get_balance_allowance, params)
        print(f"Balance Allowance: {resp}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
