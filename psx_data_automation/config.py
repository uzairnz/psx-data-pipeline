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
PSX_BASE_URL = "https://www.psx.com.pk"  # Base URL for PSX Official Website
PSX_DATA_PORTAL_URL = "https://dps.psx.com.pk"  # Data Portal URL

# Ensure directories exist
for directory in [DATA_DIR, METADATA_DIR, LOG_DIR]:
    os.makedirs(directory, exist_ok=True) 