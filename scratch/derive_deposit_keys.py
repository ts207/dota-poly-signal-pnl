import os
from py_clob_client_v2 import ClobClient, SignatureTypeV2
from dotenv import load_dotenv

load_dotenv()

def main():
    pk = os.getenv("POLY_PRIVATE_KEY")
    funder = os.getenv("POLY_FUNDER_ADDRESS")
    if not pk or not funder:
        print("Missing PK or Funder in .env")
        return

    host = "https://clob.polymarket.com"
    chain_id = 137
    
    print(f"Signer: 0xD071cF47CEee0fac372c3d98dc00D3532Ac9267d")
    print(f"Funder: {funder}")
    
    # Initialize client for the deposit wallet
    client = ClobClient(
        host, 
        chain_id, 
        pk, 
        funder=funder, 
        signature_type=SignatureTypeV2.POLY_1271
    )
    
    print(f"\nAttempting to derive API Key for DEPOSIT WALLET flow...")
    try:
        creds = client.create_or_derive_api_key()
        print("\nSUCCESS! Derived Keys:")
        print(f"API_KEY: {creds.api_key}")
        print(f"SECRET: {creds.api_secret}")
        print(f"PASS_PHRASE: {creds.api_passphrase}")
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    main()
