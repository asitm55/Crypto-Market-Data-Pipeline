import os
import glob
import pandas as pd
from datetime import datetime, timezone
import argparse
import logging
import sys
from collections import namedtuple

from config.settings import DATA_DIR, HISTORY_COINS

# logger
logger = logging.getLogger(__name__)


# ==============================================================================
# DataLoader
# ==============================================================================
class DataLoader:
    """
    Reads partitioned Parquet files from the data lake.
    Handles both single-partition and multi-partition reads.
    """

    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def load_assets(self, execution_date: str = None) -> pd.DataFrame:
        """
        Load assets data.
        If execution_date is given, load that specific partition.
        Otherwise, load all partitions and return combined DataFrame.
        """
        if execution_date:
            path = os.path.join(
                self.base_dir,
                "raw",
                "assets",
                f"date={execution_date}",
                "assets.parquet",
            )
            if not os.path.exists(path):
                raise FileNotFoundError(f"No assets data for date: {execution_date}")
            return pd.read_parquet(path)
        else:
            pattern = os.path.join(self.base_dir, "raw", "assets", "**", "*.parquet")
            files = glob.glob(pattern, recursive=True)
            if not files:
                raise FileNotFoundError("No assets parquet files found.")
            return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    def load_history(self, coin_id: str, execution_date: str = None) -> pd.DataFrame:
        """
        Load history data for a specific coin.
        """
        if execution_date:
            path = os.path.join(
                self.base_dir,
                "raw",
                "history",
                f"coin={coin_id}",
                f"date={execution_date}",
                "history.parquet",
            )
            if not os.path.exists(path):
                raise FileNotFoundError(
                    f"No history data for coin={coin_id}, date={execution_date}"
                )
            return pd.read_parquet(path)
        else:
            pattern = os.path.join(
                self.base_dir, "raw", "history", f"coin={coin_id}", "**", "*.parquet"
            )
            files = glob.glob(pattern, recursive=True)
            if not files:
                raise FileNotFoundError(f"No history files found for coin: {coin_id}")
            df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
            # Deduplicate by (coin_id, date) keeping the latest ingestion
            df = df.sort_values("_ingestion_timestamp").drop_duplicates(
                subset=["date"], keep="last"
            )
            return df


# ==============================================================================
# DataCleaner
# ==============================================================================
class DataCleaner:
    """
    Performs type casting and business-rule-based filtering on raw DataFrames.
    All methods return a new DataFrame (no in-place modification).
    """

    @staticmethod
    def clean_assets(df: pd.DataFrame) -> pd.DataFrame:
        """
        Cast numeric fields, filter invalid prices/volumes.
        Adds a 'clean_timestamp' column.
        """
        df = df.copy()

        numeric_cols = [
            "priceUsd",
            "marketCapUsd",
            "volumeUsd24Hr",
            "changePercent24Hr",
            "supply",
            "maxSupply",
            "vwap24Hr",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df["rank"] = pd.to_numeric(df["rank"], errors="coerce").astype("Int64")

        initial_count = len(df)
        df = df[df["priceUsd"].notna() & (df["priceUsd"] > 0)]
        df = df[df["volumeUsd24Hr"].isna() | (df["volumeUsd24Hr"] >= 0)]

        removed = initial_count - len(df)
        if removed > 0:
            logger.warning(f"DataCleaner: Removed {removed} invalid rows from assets.")

        df["clean_timestamp"] = datetime.now(timezone.utc).isoformat()
        return df.reset_index(drop=True)

    @staticmethod
    def clean_history(df: pd.DataFrame, coin_id: str) -> pd.DataFrame:
        """
        Cast numeric fields, parse dates, add coin_id column.
        """
        df = df.copy()
        df["priceUsd"] = pd.to_numeric(df["priceUsd"], errors="coerce")

        # Parse date from ISO string (e.g., "2024-01-15T00:00:00.000Z")
        df["date"] = pd.to_datetime(df["date"], utc=True).dt.normalize()

        initial_count = len(df)
        df = df[df["priceUsd"].notna() & (df["priceUsd"] > 0)]
        removed = initial_count - len(df)
        if removed > 0:
            logger.warning(
                f"DataCleaner: Removed {removed} invalid rows from {coin_id} history."
            )

        df["coin_id"] = coin_id
        df = df.sort_values("date").reset_index(drop=True)
        return df


# ==============================================================================
# MetricsCalculator
# ==============================================================================
class MetricsCalculator:
    """
    Calculates business metrics from cleaned DataFrames.
    """

    @staticmethod
    def calculate_sma(df: pd.DataFrame, window: int = 7) -> pd.DataFrame:
        """
        Calculates a simple moving average of priceUsd.
        The first window-1 rows will have SMA calculated on fewer data points.
        """
        df = df.copy()
        df = df.sort_values("date")
        df["sma_7d"] = df["priceUsd"].rolling(window=window, min_periods=1).mean()
        return df

    @staticmethod
    def identify_high_volatility(df: pd.DataFrame) -> pd.DataFrame:
        """
        Identifies days with high volatility (>5% change from previous day).
        Note: Exact OHLCV volatility requires High, Low, Open data, which is unavailable.
        We proxy this using the absolute daily percentage change.
        """
        df = df.copy()
        df = df.sort_values("date")

        # Calculate daily percentage change (price today - price yesterday) / price yesterday
        # Shift by 1 to get yesterday's price
        yesterday_price = df["priceUsd"].shift(1)
        df["daily_pct_change"] = (
            (df["priceUsd"] - yesterday_price) / yesterday_price
        ).fillna(0.0)

        # Flag days where absolute change > 5%
        df["is_high_volatility"] = df["daily_pct_change"].abs() > 0.05
        return df

    @staticmethod
    def get_top_winners(df_assets: pd.DataFrame, n: int = 5) -> pd.DataFrame:
        """
        Get the top N winners by 24h percentage change.
        """
        df = df_assets.copy()
        df = df.sort_values("changePercent24Hr", ascending=False).head(n)
        res = df[["rank", "name", "symbol", "priceUsd", "changePercent24Hr"]].copy()

        # Add winner_rank (1 is best)
        res["winner_rank"] = range(1, len(res) + 1)
        return res


# ==============================================================================
# DataQualityChecker
# ==============================================================================
QualityCheckResult = namedtuple(
    "QualityCheckResult", ["check_name", "passed", "message", "failing_count"]
)


class DataQualityChecker:
    """
    Programmatic data quality checks that run as part of the pipeline.
    Failures are logged but do NOT stop the pipeline unless critical=True.
    """

    @staticmethod
    def check_volume_non_negative(df_assets: pd.DataFrame) -> QualityCheckResult:
        """
        CRITICAL CHECK: volumeUsd24Hr must never be negative.
        Null/NaN values are acceptable (some coins may not report volume).
        """
        if df_assets.empty or "volumeUsd24Hr" not in df_assets.columns:
            return QualityCheckResult(
                "check_volume_non_negative", True, "No volume data to check.", 0
            )

        failing = df_assets[df_assets["volumeUsd24Hr"] < 0]
        failing_count = len(failing)
        passed = failing_count == 0
        msg = (
            f"Found {failing_count} rows with negative volume."
            if not passed
            else "All volumes non-negative."
        )

        return QualityCheckResult(
            "check_volume_non_negative", passed, msg, failing_count
        )

    @staticmethod
    def check_no_date_gaps(
        df_history: pd.DataFrame, coin_id: str
    ) -> QualityCheckResult:
        """
        Validate that the history DataFrame has exactly 1 record per calendar day
        with no missing dates between the first and last date.
        """
        if df_history.empty:
            return QualityCheckResult(
                f"check_no_date_gaps_{coin_id}", True, "Empty history df.", 0
            )

        min_date = df_history["date"].min()
        max_date = df_history["date"].max()
        expected_dates = pd.date_range(min_date, max_date, freq="D")

        actual_dates = df_history["date"]
        missing_dates = expected_dates.difference(actual_dates)

        failing_count = len(missing_dates)
        passed = failing_count == 0
        msg = (
            f"Missing dates for {coin_id}: {missing_dates.strftime('%Y-%m-%d').tolist()}"
            if not passed
            else f"No date gaps for {coin_id}."
        )

        return QualityCheckResult(
            f"check_no_date_gaps_{coin_id}", passed, msg, failing_count
        )

    @staticmethod
    def check_sma_not_null(df_with_sma: pd.DataFrame) -> QualityCheckResult:
        """Verify sma_7d was calculated for all rows."""
        if df_with_sma.empty or "sma_7d" not in df_with_sma.columns:
            return QualityCheckResult(
                "check_sma_not_null", True, "No SMA data to check.", 0
            )

        failing = df_with_sma[df_with_sma["sma_7d"].isna()]
        failing_count = len(failing)
        passed = failing_count == 0
        msg = (
            f"Found {failing_count} rows with null SMA."
            if not passed
            else "All SMA values calculated."
        )

        return QualityCheckResult("check_sma_not_null", passed, msg, failing_count)

    @classmethod
    def run_all(
        cls,
        df_assets: pd.DataFrame,
        history_dfs: dict,
        df_sma: pd.DataFrame = None,
    ) -> list[QualityCheckResult]:
        """
        Run all checks and return results.
        Logs PASS/FAIL for each check.
        Returns list of all QualityCheckResult objects.
        """
        results = []

        results.append(cls.check_volume_non_negative(df_assets))

        for coin_id, df_hist in history_dfs.items():
            results.append(cls.check_no_date_gaps(df_hist, coin_id))

        if df_sma is not None:
            results.append(cls.check_sma_not_null(df_sma))

        # Log summaries
        for res in results:
            status = "PASS" if res.passed else "FAIL"
            log_msg = f"[{status}] {res.check_name}: {res.message}"
            if res.passed:
                logger.info(log_msg)
            else:
                logger.warning(log_msg)

        return results


# ==============================================================================
# Main Execution
# ==============================================================================
def setup_logging(level: str = "INFO"):
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        level=getattr(logging, level.upper(), logging.INFO),
    )


def main():
    parser = argparse.ArgumentParser(description="CoinCap Transformation Pipeline")
    parser.add_argument(
        "--date",
        default=None,
        help="Execution date in YYYY-MM-DD format. If omitted, uses latest available partition.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    setup_logging(args.log_level)
    logger.info("Starting transformation pipeline")

    # Initialize Loader
    loader = DataLoader(base_dir=DATA_DIR)

    try:
        raw_assets = loader.load_assets(execution_date=args.date)
        logger.info(f"Loaded {len(raw_assets)} raw asset rows.")
    except Exception as e:
        logger.error(f"Failed to load assets: {e}")
        sys.exit(1)

    # Clean assets
    clean_assets = DataCleaner.clean_assets(raw_assets)

    history_dfs = {}
    clean_history_dfs_sma = []
    clean_history_dfs_vol = []

    for coin_id in HISTORY_COINS:
        try:
            raw_hist = loader.load_history(coin_id)
            clean_hist = DataCleaner.clean_history(raw_hist, coin_id)
            history_dfs[coin_id] = clean_hist

            # Calculate SMA
            hist_sma = MetricsCalculator.calculate_sma(clean_hist)
            # Identify high volatility
            hist_vol = MetricsCalculator.identify_high_volatility(
                hist_sma
            )  # Chain metrics

            clean_history_dfs_sma.append(hist_sma)
            clean_history_dfs_vol.append(hist_vol)

            # Save Processed Dataframes
            # Save SMA
            out_sma = os.path.join(
                DATA_DIR, "processed", "sma", f"coin={coin_id}", "sma.parquet"
            )
            os.makedirs(os.path.dirname(out_sma), exist_ok=True)
            hist_sma.to_parquet(out_sma, engine="pyarrow", index=False)
            logger.info(f"Saved {coin_id} SMA to {out_sma}")

            # Save Volatility
            out_vol = os.path.join(
                DATA_DIR,
                "processed",
                "volatility",
                f"coin={coin_id}",
                "volatility.parquet",
            )
            os.makedirs(os.path.dirname(out_vol), exist_ok=True)
            hist_vol.to_parquet(out_vol, engine="pyarrow", index=False)
            logger.info(f"Saved {coin_id} volatility to {out_vol}")

        except Exception as e:
            logger.warning(
                f"Skipping history processing for {coin_id} due to error: {e}"
            )

    # Combine all SMA dataframe if exists for checking
    if clean_history_dfs_sma:
        combined_sma = pd.concat(clean_history_dfs_sma, ignore_index=True)
    else:
        combined_sma = None

    # Run Data Quality Checks
    DataQualityChecker.run_all(clean_assets, history_dfs, combined_sma)

    # Get top 5 winners and save ranking
    top_winners = MetricsCalculator.get_top_winners(clean_assets)
    out_rank = os.path.join(DATA_DIR, "processed", "rankings", "top_winners.parquet")
    os.makedirs(os.path.dirname(out_rank), exist_ok=True)
    top_winners.to_parquet(out_rank, engine="pyarrow", index=False)
    logger.info(f"Saved top winners ranking to {out_rank}")

    # Print summary to stdout
    print("\n=== TOP 5 WINNERS ===")
    print(top_winners.to_string(index=False))
    print("=====================\n")

    logger.info("Transformation pipeline complete.")


if __name__ == "__main__":
    main()
