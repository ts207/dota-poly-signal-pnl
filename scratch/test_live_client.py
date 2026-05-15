import asyncio
import os
from live_executor import LiveCLOBClient
from dotenv import load_dotenv

load_dotenv()

YES_TOKEN_ID = "2045565837370551909989536379865102671584313967899298859459028552120513333778"

async def main():
    client = LiveCLOBClient()
    print(f"Testing order with LiveCLOBClient...")
    try:
        # Note: buy_fak_market signature is:
        # async def buy_fak_market(self, *, token_id: str, amount_usd: float, price_cap: float, tick_size: str, neg_risk: bool) -> dict[str, Any]:
        resp = await client.buy_fak_market(
            token_id=YES_TOKEN_ID,
            amount_usd=1.0,
            price_cap=0.6,
            tick_size="0.01",
            neg_risk=False
        )
        print(resp)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
