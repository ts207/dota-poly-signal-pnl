import asyncio
import os
from live_executor import LiveCLOBClient
from py_clob_client_v2 import AssetType, SignatureTypeV2
from dotenv import load_dotenv

load_dotenv()

YES_TOKEN_ID = "2045565837370551909989536379865102671584313967899298859459028552120513333778"

async def main():
    print(f"Initializing LiveCLOBClient...")
    client = LiveCLOBClient()
    
    print("Syncing CLOB balances with signature_type=3...")
    try:
        # Note: In py-clob-client-v2, update_balance_allowance might take a params object
        # but let's try the simple call first if possible.
        # Actually, looking at the docs provided:
        # clob.update_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=SignatureTypeV2.POLY_1271))
        from py_clob_client_v2 import BalanceAllowanceParams
        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,
            signature_type=SignatureTypeV2.POLY_1271
        )
        resp = await asyncio.to_thread(client._client.update_balance_allowance, params)
        print(f"Sync result: {resp}")
    except Exception as e:
        print(f"Sync failed: {e}")
        # Continue anyway to see if order works

    print(f"Testing order with LiveCLOBClient...")
    try:
        resp = await client.buy_fak_market(
            token_id=YES_TOKEN_ID,
            amount_usd=1.0,
            price_cap=0.6,
            tick_size="0.01",
            neg_risk=False
        )
        print("Order Response:")
        print(resp)
    except Exception as e:
        print(f"Order Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
