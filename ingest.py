import argparse
import sys
import logging
import time
import random
import functools
import requests
import os
import pandas as pd
from datetime import date, datetime, timedelta, timezone

# configuration
from config.settings import (
    COINCAP_BASE_URL,
    COINCAP_API_KEY,
    TOP_N_ASSETS,
    HISTORY_COINS,
    HISTORY_DAYS,
    DATA_DIR,
    MAX_RETRIES,
    BASE_BACKOFF_SECONDS,
    MAX_BACKOFF_SECONDS,
    ASSETS_SCHEMA,
    HISTORY_SCHEMA,
)

logger = logging.getLogger(__name__)


# ==============================================================================
# RetryHandler
# ==============================================================================
class RetryHandler:
    """
    Implements exponential backoff with jitter for HTTP request retries.

    Algorithm:
        wait = min(BASE_BACKOFF * 2^attempt, MAX_BACKOFF) + random(0, 1)
    """

    def __init__(
        self,
        max_retries=5,
        base_backoff=1.0,
        max_backoff=64.0,
        retryable_status_codes=(429, 500, 502, 503, 504),
    ):
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self.retryable_status_codes = retryable_status_codes

    def execute(self, func, *args, **kwargs):
        """Execute func with retry logic. func must return a requests.Response."""
        last_exception = None
        for attempt in range(self.max_retries + 1):
            try:
                response = func(*args, **kwargs)
                if response.status_code in self.retryable_status_codes:
                    raise requests.exceptions.HTTPError(
                        f"Status {response.status_code}", response=response
                    )
                return response
            except (
                requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                last_exception = e
                if attempt == self.max_retries:
                    logger.error(
                        f"All {self.max_retries} retries exhausted. Last error: {e}"
                    )
                    raise
                wait = min(self.base_backoff * (2**attempt), self.max_backoff)
                wait += random.uniform(0, 1)  # jitter
                logger.warning(
                    f"Attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                    f"Retrying in {wait:.2f}s..."
                )
                time.sleep(wait)


def with_retry(max_retries=5, base_backoff=1.0, max_backoff=64.0):
    """
    Decorator factory that applies RetryHandler to any function returning
    a requests.Response object.
    """

    def decorator(func):
        handler = RetryHandler(max_retries, base_backoff, max_backoff)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return handler.execute(func, *args, **kwargs)

        return wrapper

    return decorator


# ==============================================================================
# SchemaValidator
# ==============================================================================
class SchemaValidationError(Exception):
    """Raised when the API response does not match the expected schema."""

    pass


class SchemaValidator:
    """
    Validates and coerces a pandas DataFrame to match an expected schema.
    """

    def __init__(self, schema: dict):
        self.schema = schema

    def validate_and_cast(self, df: "pd.DataFrame") -> "pd.DataFrame":
        """
        Returns a new DataFrame with all columns cast to schema types.
        Raises SchemaValidationError for any schema violation.
        """
        import pandas as pd

        df = df.copy()
        errors = []

        for col, expected_type in self.schema.items():
            if col not in df.columns:
                errors.append(f"Missing required column: '{col}'")
                continue
            try:
                if expected_type == int:
                    # Use Int64 (nullable integer) to handle NaN gracefully
                    df[col] = pd.to_numeric(df[col], errors="raise").astype("Int64")
                elif expected_type == float:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    # Log if >20% values became NaN after coercion (suspicious)
                    nan_ratio = df[col].isna().mean()
                    if nan_ratio > 0.2:
                        logger.warning(
                            f"Column '{col}': {nan_ratio:.1%} values are NaN after cast."
                        )
                elif expected_type == str:
                    df[col] = df[col].astype(str)
            except (ValueError, TypeError) as e:
                errors.append(
                    f"Cannot cast column '{col}' to {expected_type.__name__}. "
                    f"Sample values: {df[col].head(3).tolist()}. Error: {e}"
                )

        if errors:
            raise SchemaValidationError(
                "Schema validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            )

        return df


# ==============================================================================
# CoinCapClient
# ==============================================================================
class CoinCapClient:
    """
    HTTP client for the CoinCap API v3.
    """

    def __init__(self, api_key: str, timeout: int = 30):
        if not api_key:
            raise ValueError(
                "CoinCap v3 requires an API key. "
                "Get one free at https://coincap.io/api-key"
            )
        self.base_url = COINCAP_BASE_URL
        self.api_key = api_key
        self.timeout = timeout
        self.retry_handler = RetryHandler(
            max_retries=MAX_RETRIES,
            base_backoff=BASE_BACKOFF_SECONDS,
            max_backoff=MAX_BACKOFF_SECONDS,
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            }
        )

    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Internal GET helper with retry logic. Injects apiKey into every request."""
        url = f"{self.base_url}{endpoint}"
        params = params or {}
        params["apiKey"] = self.api_key
        logger.info(
            f"GET {url} | params={{k: v for k, v in params.items() if k != 'apiKey'}}"
        )
        response = self.retry_handler.execute(
            self.session.get, url, params=params, timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def get_top_assets(self, n: int = 50) -> list[dict]:
        """
        Fetch top N cryptocurrencies by market cap.
        """
        data = self._get("/assets", params={"limit": n})
        assets = data.get("data", [])
        logger.info(f"Fetched {len(assets)} assets from /assets endpoint.")
        return assets

    def get_asset_history(
        self, coin_id: str, days: int = 20, interval: str = "d1"
    ) -> list[dict]:
        """
        Fetch daily price history for a coin.
        """
        now = datetime.now(timezone.utc)
        end_ms = int(now.timestamp() * 1000)
        start_ms = int((now - timedelta(days=days)).timestamp() * 1000)

        data = self._get(
            f"/assets/{coin_id}/history",
            params={"interval": interval, "start": start_ms, "end": end_ms},
        )
        history = data.get("data", [])
        logger.info(
            f"Fetched {len(history)} history records for '{coin_id}' "
            f"(last {days} days, interval={interval})."
        )
        return history


# ==============================================================================
# StorageManager
# ==============================================================================
class StorageManager:
    """
    Handles idempotent writing of DataFrames to partitioned Parquet files.
    """

    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def _write(self, df: pd.DataFrame, path: str) -> bool:
        """
        Write df to path as Parquet. Returns True if written, False if skipped.
        Adds _ingestion_timestamp column before writing.
        """
        if os.path.exists(path):
            logger.warning(f"IDEMPOTENCY: File already exists, skipping: {path}")
            return False

        os.makedirs(os.path.dirname(path), exist_ok=True)
        df = df.copy()
        df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
        df.to_parquet(path, engine="pyarrow", index=False)
        logger.info(f"Written {len(df)} rows to {path}")
        return True

    def write_assets(self, df: pd.DataFrame, execution_date: str) -> bool:
        """
        Persist assets snapshot.
        execution_date: 'YYYY-MM-DD' string.
        """
        path = os.path.join(
            self.base_dir, "raw", "assets", f"date={execution_date}", "assets.parquet"
        )
        return self._write(df, path)

    def write_history(
        self, df: pd.DataFrame, coin_id: str, execution_date: str
    ) -> bool:
        """
        Persist history snapshot for one coin.
        Partitioned by coin and execution date.
        """
        path = os.path.join(
            self.base_dir,
            "raw",
            "history",
            f"coin={coin_id}",
            f"date={execution_date}",
            "history.parquet",
        )
        return self._write(df, path)


# ==============================================================================
# Main Execution
# ==============================================================================
def setup_logging(level: str = "INFO"):
    # Clear existing handlers to avoid duplicates
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        level=getattr(logging, level.upper(), logging.INFO),
    )


def main():
    parser = argparse.ArgumentParser(description="CoinCap Ingestion Pipeline")
    parser.add_argument(
        "--date",
        default=str(date.today()),
        help="Execution date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Fetch data but skip writing to disk"
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    setup_logging(args.log_level)
    logger = logging.getLogger("ingest.main")
    logger.info(
        f"Starting ingestion pipeline | date={args.date} | dry_run={args.dry_run}"
    )

    client = CoinCapClient(
        api_key=COINCAP_API_KEY
    )  # v3: key is mandatory
    storage = StorageManager(base_dir=DATA_DIR)
    assets_validator = SchemaValidator(ASSETS_SCHEMA)
    history_validator = SchemaValidator(HISTORY_SCHEMA)

    written = 0
    skipped = 0
    errors = []

    # --- Assets ---
    try:
        raw_assets = client.get_top_assets(n=TOP_N_ASSETS)
        df_assets = pd.DataFrame(raw_assets)
        df_assets = assets_validator.validate_and_cast(df_assets)
        if not args.dry_run:
            result = storage.write_assets(df_assets, args.date)
            written += int(result)
            skipped += int(not result)
        else:
            logger.info(f"[DRY RUN] Would write {len(df_assets)} asset rows.")
    except SchemaValidationError as e:
        logger.error(f"Schema validation failed for /assets: {e}")
        errors.append(str(e))
    except Exception as e:
        logger.exception(f"Unexpected error fetching /assets: {e}")
        errors.append(str(e))

    # --- History for each coin ---
    for coin_id in HISTORY_COINS:
        try:
            raw_history = client.get_asset_history(coin_id, days=HISTORY_DAYS)
            df_history = pd.DataFrame(raw_history)
            df_history = history_validator.validate_and_cast(df_history)
            if not args.dry_run:
                result = storage.write_history(df_history, coin_id, args.date)
                written += int(result)
                skipped += int(not result)
            else:
                logger.info(
                    f"[DRY RUN] Would write {len(df_history)} rows for {coin_id}."
                )
        except SchemaValidationError as e:
            logger.error(f"Schema validation failed for history/{coin_id}: {e}")
            errors.append(str(e))
        except Exception as e:
            logger.exception(f"Unexpected error fetching history for {coin_id}: {e}")
            errors.append(str(e))

    # --- Summary ---
    logger.info(
        f"Pipeline complete | written={written} | skipped={skipped} | errors={len(errors)}"
    )
    if errors:
        logger.error("Pipeline finished with errors. See above for details.")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
