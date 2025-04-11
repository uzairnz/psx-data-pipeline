#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Configuration settings for the PSX Data Pipeline project.
"""

import os
from pathlib import Path

# Project version
__version__ = "0.1.0"

# Base directory - use absolute path for reliability
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

# Directory paths
DATA_DIR = BASE_DIR / "data"
METADATA_DIR = BASE_DIR / "metadata"
LOG_DIR = BASE_DIR / "logs"

# Date settings
START_DATE = "2014-01-01"  # Start collecting data from this date
MARKET_CLOSE_HOUR = 18  # 6:00 PM Pakistan Time

# URL settings
# Update to the correct PSX website URLs
PSX_BASE_URL = "https://dps.psx.com.pk"  # Base URL for PSX Official Website
PSX_DATA_PORTAL_URL = "https://dps.psx.com.pk"  # Data Portal URL
COMPANY_URL_TEMPLATE = "https://dps.psx.com.pk/company/"  # Updated URL for company pages

# Alternative data sources
SCSTRADE_BASE_URL = "https://www.scstrade.com"
SCSTRADE_SNAPSHOT_URL = "https://www.scstrade.com/stockscreening/SS_CompanySnapShotHP.aspx?symbol="
SCSTRADE_HISTORICAL_URL = "https://www.scstrade.com/stockscreening/SS_HistoricalCloseHP.aspx?symbol="

# Investing.com data source
INVESTING_BASE_URL = "https://www.investing.com"
INVESTING_SEARCH_URL = "https://www.investing.com/search/?q="
INVESTING_HISTORICAL_URL = "https://www.investing.com/equities/{ticker}-historical-data"

# For direct access to known stocks
MILLAT_TRACTOR_URL = "https://www.investing.com/equities/millat-tractor-historical-data"

# Ticker name mapping from PSX to Investing.com format
# Examples: 
# "LUCK" -> "lucky-cement"
# "HBL" -> "habib-bank"
TICKER_TO_INVESTING_MAP = {
    # Common Pakistan stock tickers
    "LUCK": "lucky-cement",
    "ENGRO": "engro-corporation-limited",
    "HBL": "habib-bank-limited",
    "PSO": "pakistan-state-oil-co",
    "OGDC": "oil-and-gas-development-company-limited",
    "UBL": "united-bank-limited",
    "EFERT": "engro-fertilizer-ltd",
    "MARI": "mari-petroleum-co-limited",
    "MCB": "mcb-bank-limited",
    "PPL": "pakistan-petroleum-limited",
    "FFC": "fauji-fertilizer-company-limited",
    "HUBC": "hub-power-company-limited",
    "BAFL": "bank-alfalah-limited",
    "MEBL": "meezan-bank-limited",
    "POL": "pakistan-oilfields-limited",
    # This will be populated dynamically based on search results for other tickers
}

# File paths
TICKERS_FILE = "tickers.json"  # Default filename for storing ticker data

# Ensure directories exist
for directory in [DATA_DIR, METADATA_DIR, LOG_DIR]:
    os.makedirs(directory, exist_ok=True) 