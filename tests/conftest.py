import os


# Keep tests independent from local live/paper runtime settings in .env.
# config.py uses load_dotenv without override, so these values win during tests.
os.environ["LIVE_TRADING"] = "false"
os.environ["MIN_LAG"] = "0.08"
os.environ["MIN_EXECUTABLE_EDGE"] = "0.03"
os.environ["MAX_SPREAD"] = "0.15"
os.environ["DEFAULT_MAX_FILL_PRICE"] = "0.80"
os.environ["MAX_BOOK_AGE_MS"] = "750"
os.environ["MAX_STEAM_AGE_MS"] = "1500"
os.environ["MAX_SOURCE_UPDATE_AGE_SEC"] = "45"
os.environ["EVENT_LEAD_SWING_30S"] = "1500"
os.environ["EVENT_LEAD_SWING_60S"] = "3000"
os.environ["REALTIME_STATS_ENABLED"] = "false"
