# Crypto Market Data Pipeline - Phase 1: Ingestion

## Purpose
This pipeline ingests cryptocurrency market data from the CoinCap API v3.
It fetches the top 50 assets by market cap and the last 20 days of historical data for configured coins (Bitcoin, Ethereum, Tether). The data is validated against expected schemas and stored idempotently as partitioned Parquet files for downstream processing.

## Architecture Diagram

```mermaid
flowchart TD
    CLI[CLI Argument Parser/ --date / --dry-run] --> Main
    
    Main --> Client[CoinCapClient/ Fetches from rest.coincap.io/v3]
    Client --> Retry[RetryHandler/ Exponential Backoff + Jitter]
    
    Main --> Val[SchemaValidator/ Enforces types & nullability]
    
    Main --> Storage[StorageManager/ Writes Parquet files]
    
    Storage --> Disk[(Local File System/data/raw/)]
```

## Setup Instructions

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Configure environment:
   Copy `.env.example` to `.env` and fill in the values.
   **Crucial:** `COINCAP_API_KEY` is **mandatory** for v3. You can get a free key (4000 tokens/month) at [coincap.io/api-key](https://coincap.io/api-key).

## API Version Note
This pipeline uses **CoinCap API v3** (`rest.coincap.io/v3`). The older v2 (`api.coincap.io/v2`) was deprecated on April 5, 2025. In v3, the API key is passed as an `?apiKey=` query parameter on every request, not as an Authorization header.

## How to Run

1. **Standard run (defaults to today's date):**
   ```bash
   python ingest.py
   ```
2. **Run for a specific date:**
   ```bash
   python ingest.py --date 2024-01-15
   ```
3. **Dry-run (fetch, validate, but skip writing to disk):**
   ```bash
   python ingest.py --dry-run
   ```

## Output Structure
The pipeline organizes raw data into partitioned Parquet files:
```text
data/raw/
├── assets/
│   └── date=YYYY-MM-DD/
│       └── assets.parquet
└── history/
    ├── coin=bitcoin/
    │   └── date=YYYY-MM-DD/
    │       └── history.parquet
    ├── coin=ethereum/
    │   └── ...
    └── coin=tether/
        └── ...
```

## Idempotency Guarantee
The storage layer is idempotent based on file existence. If a file already exists at the target path for a specific partition (e.g., `date=2024-01-15`), the `StorageManager` will skip the file overwrite and log a warning. To force a re-ingestion, manually delete the destination file before running the script.

## Rate Limit Handling
CoinCap applies rate limits (HTTP 429). The `RetryHandler` elegantly manages these using exponential backoff with jitter to prevent a thundering herd. It also retries on transient connection errors and timeouts (up to `MAX_RETRIES`), pausing between attempts.

## Schema Enforcement
The `SchemaValidator` guarantees that data fits the expected format before persisting it to disk. 
- Numeric fields returned as strings by the API are explicitly cast to numeric types (`float` or nullable `Int64`).
- If a required column is entirely missing, or if a particular value cannot be cast to the specified type, a `SchemaValidationError` is raised, forcing the pipeline to fail early and loudly.

## Sample Output Files
Representative outputs are located in the `samples/` directory:
- `samples/sample_assets.parquet`
- `samples/sample_history_bitcoin.parquet`

# Crypto Market Data Pipeline - Phase 2: Transformation

## Purpose
This phase takes the raw, partitioned Parquet data generated in Phase 1 and applies business transformations. It cleans the data, calculates Simple Moving Averages (SMA), identifies high-volatility days, ranks the top 5 assets by 24-hour performance, and runs automated programmatic data quality checks before persisting the processed data.

## Architecture & Flow

```mermaid
flowchart TD
    Raw[(Raw Parquet Data)] --> Loader[DataLoader]
    Loader --> Cleaner["DataCleaner<br/>- Type casting<br/>- Null filtering"]
    Cleaner --> Metrics["MetricsCalculator<br/>- SMA_7d<br/>- Volatility (> 5%)<br/>- Top Winners"]
    Metrics --> DQ["DataQualityChecker<br/>- Run business logic checks"]
    DQ --> Processed[(Processed Parquet Data)]
```

## How to Run

1. Ensure you have run the Phase 1 Ingestion first to populate `data/raw/`.
2. Run standard transformation (processes latest available assets, all history):
   ```bash
   python transform.py
   ```
3. Run transformation for a specific ingestion date:
   ```bash
   python transform.py --date 2024-01-15
   ```

## Output Structure
The processed data is persisted into `data/processed/` using the following structure:
```text
data/processed/
├── sma/
│   └── coin={coin_id}/
│       └── sma.parquet
├── volatility/
│   └── coin={coin_id}/
│       └── volatility.parquet
└── rankings/
    └── top_winners.parquet
```

## Programmatic Data Quality Checks
`DataQualityChecker` runs several checks inline during transformation:
- **Volume check**: Ensures `volumeUsd24Hr` is never negative.
- **Date gap check**: Asserts there's exactly 1 record per calendar day between min and max dates.
- **SMA completeness**: Validates that SMA is calculated for every row.

Failed checks are logged as `WARNING` but do not currently stop execution.

## Entity-Relationship Diagram

```mermaid
erDiagram
    dim_asset {
        string asset_id PK "e.g., 'bitcoin'"
        string name
        string symbol
        int rank
    }
    
    dim_date {
        date date_id PK
        int year
        int month
        int day
        int day_of_week
    }
    
    fact_asset_daily_price {
        string asset_id FK
        date date_id FK
        float price_usd
        float sma_7d
        float daily_pct_change
        bool is_high_volatility
    }
    
    fact_asset_snapshot {
        string asset_id FK
        date snapshot_date FK
        float price_usd
        float market_cap_usd
        float volume_usd_24hr
        float change_pct_24hr
    }
    
    fact_daily_rankings {
        date snapshot_date FK
        string asset_id FK
        int winner_rank
        float change_pct_24hr
    }
    
    dim_asset ||--o{ fact_asset_daily_price : "has history in"
    dim_date ||--o{ fact_asset_daily_price : "records"
    
    dim_asset ||--o{ fact_asset_snapshot : "has snapshot"
    dim_date ||--o{ fact_asset_snapshot : "at date"
    
    dim_asset ||--o{ fact_daily_rankings : "ranked"
    dim_date ||--o{ fact_daily_rankings : "on date"
```
