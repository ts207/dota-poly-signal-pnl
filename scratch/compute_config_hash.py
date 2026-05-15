import os
import json
import hashlib
from dotenv import load_dotenv

load_dotenv()

def compute_config_hash():
    keys = [
        "LIVE_TRADING", "MAX_TOTAL_LIVE_USD", "MAX_TRADE_USD", "ORDER_TYPE",
        "TRADE_EVENTS", "ALLOW_CONFIRMATION_ONLY_LIVE_TRADES",
        "DISABLE_STRUCTURE_TRADES", "MAX_BOOK_AGE_MS", "MAX_STEAM_AGE_MS",
        "MAX_SOURCE_UPDATE_AGE_SEC", "MIN_LAG", "MIN_EXECUTABLE_EDGE",
        "MAX_SPREAD", "MIN_ASK_SIZE_USD", "LIVE_REQUIRE_CADENCE_SCHEMA",
        "LIVE_ALLOWED_CADENCE_QUALITIES", "LIVE_MIN_EVENT_QUALITY", "PAPER_EXECUTION_DELAY_MS",
        "BOOK_REFRESH_RESCUE_CSV_PATH",
    ]
    payload = {key: os.getenv(key) for key in keys}
    print("Config Payload:", json.dumps(payload, indent=2))
    config_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    print("Config Hash:", config_hash)

if __name__ == "__main__":
    compute_config_hash()
