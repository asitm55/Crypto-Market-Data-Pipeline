import pytest
import pandas as pd
import requests
from unittest.mock import patch, MagicMock

from ingest import (
    RetryHandler,
    SchemaValidator,
    SchemaValidationError,
    StorageManager,
    main,
)
from config.settings import ASSETS_SCHEMA


def test_retry_on_429():
    handler = RetryHandler(max_retries=3, base_backoff=0.01)
    mock_func = MagicMock()

    resp_429 = requests.Response()
    resp_429.status_code = 429

    resp_200 = requests.Response()
    resp_200.status_code = 200

    mock_func.side_effect = [resp_429, resp_429, resp_200]

    res = handler.execute(mock_func)
    assert res.status_code == 200
    assert mock_func.call_count == 3


def test_retry_exhausted_raises():
    handler = RetryHandler(max_retries=2, base_backoff=0.01)
    mock_func = MagicMock()

    resp_429 = requests.Response()
    resp_429.status_code = 429
    mock_func.return_value = resp_429

    with pytest.raises(requests.exceptions.HTTPError):
        handler.execute(mock_func)

    assert mock_func.call_count == 3


def test_schema_validator_missing_column():
    validator = SchemaValidator(ASSETS_SCHEMA)
    df = pd.DataFrame({"id": ["bitcoin"]})  # Missing everything else
    with pytest.raises(SchemaValidationError) as exc:
        validator.validate_and_cast(df)
    assert "Missing required column" in str(exc.value)


def test_schema_validator_bad_type():
    validator = SchemaValidator({"priceUsd": float})
    # Simulating bad data that becomes NaN
    df = pd.DataFrame({"priceUsd": ["not_a_number"]})
    df_res = validator.validate_and_cast(df)

    assert pd.isna(df_res.iloc[0]["priceUsd"])  # errors="coerce" makes it NaN
    pass


def test_schema_validator_string_to_float():
    validator = SchemaValidator({"priceUsd": float, "rank": int, "id": str})
    df = pd.DataFrame({"priceUsd": ["42000.5"], "rank": ["1"], "id": ["bitcoin"]})
    df_res = validator.validate_and_cast(df)
    assert df_res.iloc[0]["priceUsd"] == 42000.5
    assert df_res.iloc[0]["rank"] == 1
    assert isinstance(df_res.iloc[0]["priceUsd"], float)


def test_storage_idempotency(tmp_path):
    storage = StorageManager(base_dir=str(tmp_path))
    df = pd.DataFrame({"id": ["bitcoin"]})

    # First write should succeed
    res1 = storage.write_assets(df, "2024-01-01")
    assert res1 is True

    # Second write should be skipped
    res2 = storage.write_assets(df, "2024-01-01")
    assert res2 is False


@patch("ingest.StorageManager.write_assets")
@patch("ingest.StorageManager.write_history")
@patch("ingest.CoinCapClient.get_top_assets")
@patch("ingest.CoinCapClient.get_asset_history")
def test_dry_run_no_files_created(
    mock_history, mock_assets, mock_write_history, mock_write_assets
):
    mock_assets.return_value = [
        {
            "id": "bitcoin",
            "rank": 1,
            "symbol": "BTC",
            "name": "Bitcoin",
            "priceUsd": "42000",
            "marketCapUsd": "800000000",
            "volumeUsd24Hr": "30000000",
            "changePercent24Hr": "5.0",
            "supply": "19000000",
            "maxSupply": "21000000",
            "vwap24Hr": "41500"
        }
    ]
    mock_history.return_value = [
        {"priceUsd": 42000.0, "time": 1234567890, "date": "2024-01-01"}
    ]

    import sys

    test_args = ["ingest.py", "--dry-run"]
    with patch.object(sys, "argv", test_args):
        try:
            main()
        except SystemExit as e:
            assert e.code == 0

    # Writes should NOT be called
    mock_write_assets.assert_not_called()
    mock_write_history.assert_not_called()
