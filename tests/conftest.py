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
os.environ["REQUIRE_TOP_LIVE_FOR_SIGNALS"] = "true"
os.environ["TRADE_EVENTS"] = (
    "BASE_PRESSURE_T3_COLLAPSE,BASE_PRESSURE_T4,OBJECTIVE_CONVERSION_T3,"
    "OBJECTIVE_CONVERSION_T4,POLL_COMEBACK_RECOVERY,POLL_FIGHT_SWING,"
    "POLL_KILL_BURST_CONFIRMED,POLL_LATE_FIGHT_FLIP,POLL_LEAD_FLIP_WITH_KILLS,"
    "POLL_MAJOR_COMEBACK_RECOVERY,POLL_STOMP_THROW_CONFIRMED,"
    "POLL_ULTRA_LATE_FIGHT_FLIP,THRONE_EXPOSED"
)
os.environ["DISABLE_STRUCTURE_TRADES"] = "false"
os.environ["REALTIME_STATS_ENABLED"] = "false"
