# URL Pattern Management

## Overview
This document explains how URL patterns for PSX company information pages are managed in the project.

## Current Implementation

The URL pattern for company information pages is defined in `config.py` as `COMPANY_URL_TEMPLATE`. This centralizes the URL pattern in one place, making it easy to update if the PSX website changes its structure.

```python
# Current URL pattern
COMPANY_URL_TEMPLATE = "https://dps.psx.com.pk/company/{}"
```

The placeholder `{}` is replaced with the ticker symbol to create the full URL, e.g., `https://dps.psx.com.pk/company/HBL`.

## Usage in Code

Whenever you need to construct a URL for a company page, use the template from config:

```python
from psx_data_automation.config import COMPANY_URL_TEMPLATE

def get_company_url(ticker_symbol):
    """Generate the URL for a company's information page."""
    return COMPANY_URL_TEMPLATE.format(ticker_symbol)

# Example usage
hbl_url = get_company_url("HBL")  # Returns: https://dps.psx.com.pk/company/HBL
```

## Historical Changes

Previously, the project used hardcoded URLs without a central configuration, such as:
- `https://www.psx.com.pk/company-information/{symbol}`

We've updated the code to use the `COMPANY_URL_TEMPLATE` from the config file instead.

## Handling Future Changes

If the PSX website changes its URL structure:

1. Update only the `COMPANY_URL_TEMPLATE` in `config.py`
2. All components will automatically use the new pattern
3. No need to search and replace URLs throughout the codebase

## Error Handling

Some tickers might have non-standard URLs or return errors when accessed. The code should include appropriate error handling:

```python
import requests
from psx_data_automation.config import COMPANY_URL_TEMPLATE

def get_company_data(ticker_symbol, max_retries=3):
    """Fetch company data with retry logic."""
    url = COMPANY_URL_TEMPLATE.format(ticker_symbol)
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                logging.warning(f"Failed to fetch company details for {ticker_symbol}: {str(e)}")
                return None
            time.sleep(1)  # Wait before retry
```

## Testing URL Patterns

When testing changes to the URL pattern, use the `test_ticker_update.py` script:

```bash
python -m psx_data_automation.scripts.test_ticker_update
```

This will create test data with the updated URL patterns and verify that they work correctly. 