"""
settings.py — Central configuration module.
All constants, env vars, and schema definitions live here.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# --- API ---
# v3 is the current active API. v2 (api.coincap.io/v2) was deprecated 2025-04-05.
COINCAP_BASE_URL = "https://rest.coincap.io/v3"
COINCAP_API_KEY = os.getenv(
    "COINCAP_API_KEY", ""
)  # REQUIRED for v3 — no unauthenticated access
# NOTE: v3 auth uses ?apiKey=<key> as a query param, NOT an Authorization header
TOP_N_ASSETS = 50
HISTORY_COINS = [
    "bitcoin",
    "ethereum",
    "tether",
]  # IDs must match CoinCap's slug format
HISTORY_INTERVAL = "d1"
HISTORY_DAYS = 20

# --- Storage ---
DATA_DIR = os.getenv("DATA_DIR", "./data")
RAW_ASSETS_DIR = os.path.join(DATA_DIR, "raw", "assets")
RAW_HISTORY_DIR = os.path.join(DATA_DIR, "raw", "history")

# --- Retry ---
MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 64.0

# --- Schemas (expected field types from the CoinCap API) ---
# These are the EXPECTED types AFTER our casting, not raw API types.
# CoinCap returns most numeric fields as strings — we cast on ingest.
ASSETS_SCHEMA = {
    "id": str,
    "rank": int,
    "symbol": str,
    "name": str,
    "priceUsd": float,
    "marketCapUsd": float,
    "volumeUsd24Hr": float,
    "changePercent24Hr": float,
    "supply": float,
    "maxSupply": float,  # nullable
    "vwap24Hr": float,  # nullable
}

HISTORY_SCHEMA = {
    "priceUsd": float,  # v3 returns this as a native float (v2 returned it as a string)
    "time": int,  # epoch ms
    "date": str,  # ISO date string from API
}
