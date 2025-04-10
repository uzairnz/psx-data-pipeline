# PSX Data Pipeline Scripts Guide

This document provides an overview of the scripts included in the PSX Data Pipeline project and how to use them.

## Main Entry Point

### `main.py`

The main entry point for the pipeline with various command-line options:

```bash
# View help and available options
python -m psx_data_automation.main --help

# Run the full pipeline
python -m psx_data_automation.main --full-run

# Just synchronize tickers
python -m psx_data_automation.main --sync-tickers

# View version information
python -m psx_data_automation.main --version
```

## Core Scripts

### `scrape_tickers.py`

Responsible for fetching the current list of ticker symbols from the PSX website.

- **Function**: `fetch_tickers_from_psx()`
- **Output**: List of ticker symbols with their basic information
- **Mock Data**: Contains mock data for testing purposes

```python
# Example usage within code
from psx_data_automation.scripts.scrape_tickers import fetch_tickers_from_psx
tickers = fetch_tickers_from_psx(use_mock=False)
```

### `update_ticker_info.py`

Updates ticker information including names, sectors, and URLs.

- **Function**: `update_ticker_info(tickers)`
- **Input**: List of ticker objects
- **Output**: Updated ticker information with names, sectors, and URLs

```python
# Example usage within code
from psx_data_automation.scripts.update_ticker_info import update_ticker_info
updated_tickers, stats = update_ticker_info(tickers)
```

## Testing Scripts

### `test_ticker_update.py`

Tests the ticker update functionality with a small sample of tickers.

```bash
# Run the test
python -m psx_data_automation.scripts.test_ticker_update
```

- Creates a test file with a subset of tickers
- Runs the ticker update process on these test tickers
- Logs the results and verifies successful updates

## Configuration

### `config.py`

Contains configuration settings for the pipeline:

- `COMPANY_URL_TEMPLATE`: Template for ticker information URLs
- `DATA_DIR`: Directory for storing data files
- `LOG_DIR`: Directory for storing log files
- Various other configuration settings

Example:
```python
from psx_data_automation.config import COMPANY_URL_TEMPLATE, DATA_DIR

# Use the URL template
url = COMPANY_URL_TEMPLATE.format("HBL")
```

## Future Scripts

The following scripts are planned but not yet implemented:

### `download_data.py`

Will be responsible for downloading historical OHLC data for all tickers.

### `update_data.py`

Will handle daily updates of OHLC data after market close.

## Error Handling and Logging

All scripts include robust error handling and logging. Logs are saved to the configured log directory and also output to the console during execution.

## Best Practices

1. Always use the `COMPANY_URL_TEMPLATE` from config for URL construction
2. Handle network errors with appropriate retry logic
3. Use mock data during development/testing by setting `use_mock=True`
4. Check the logs directory after each run to verify success/identify issues 