import os
import getpass
from py_clob_client_v2 import ClobClient

def main():
    print("--- Polymarket CLOB API Key Generator ---")
    pk = getpass.getpass("Enter your Private Key (starts with 0x): ").strip()
    
    # Handle the "0z" prefix common in some encodings/typos
    if pk.startswith("0z"):
        pk = "0x" + pk[2:]
    
    if not pk.startswith("0x"):
        print("Error: Private key must start with 0x")
        return

    # Initialize client
    host = "https://clob.polymarket.com"
    chain_id = 137
    
    client = ClobClient(host, chain_id, pk)
    
    print("\nAttempting to Create or Derive API Key...")
    try:
        # Use create_or_derive_api_key which is more robust in V2
        creds = client.create_or_derive_api_key()
        print("\nSUCCESS! Add these to your .env file:\n")
        print(f"POLY_PRIVATE_KEY={pk}")
        print(f"POLY_CLOB_API_KEY={creds.api_key}")
        print(f"POLY_CLOB_SECRET={creds.api_secret}")
        print(f"POLY_CLOB_PASS_PHRASE={creds.api_passphrase}")
        print("\nIMPORTANT: Save the Secret and Passphrase now. They cannot be recovered.")
    except Exception as e:
        print(f"\nFAILED: {e}")

if __name__ == "__main__":
    main()
