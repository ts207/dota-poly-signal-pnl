import os
from py_clob_client_v2 import ClobClient
from dotenv import load_dotenv

load_dotenv()

def main():
    pk = os.getenv("POLY_PRIVATE_KEY")
    if not pk:
        print("No PK found in .env")
        return

    host = "https://clob.polymarket.com"
    chain_id = 137
    
    # Try with different signature types if needed
    sig_type = int(os.getenv("POLY_SIGNATURE_TYPE", "1"))
    
    client = ClobClient(host, chain_id, pk, signature_type=sig_type)
    
    print(f"Attempting to derive API Key for {client.get_address()} (sig_type={sig_type})...")
    try:
        creds = client.create_or_derive_api_key()
        print("\nDerived API Key Info:")
        print(f"API_KEY: {creds.api_key}")
        print(f"SECRET: {creds.api_secret}")
        print(f"PASS_PHRASE: {creds.api_passphrase}")
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    main()
