#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utility functions for the PSX Data Pipeline.

This module contains shared helper functions for:
- Web scraping and HTTP requests
- Date handling
- File operations
- Data processing

Usage:
    Import: from psx_data_automation.scripts.utils import fetch_url, date_range, etc.
"""

import logging
import time
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("psx_pipeline.utils")


def retry(max_attempts=3, delay=1, backoff=2, exceptions=(Exception,)):
    """
    Retry decorator for functions that might fail due to network issues.
    
    Args:
        max_attempts (int): Maximum number of retry attempts
        delay (int): Initial delay between retries in seconds
        backoff (int): Backoff multiplier for delay
        exceptions (tuple): Exceptions to catch and retry
    
    Returns:
        function: Decorated function with retry capability
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            m_attempts, m_delay = max_attempts, delay
            while m_attempts > 0:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    m_attempts -= 1
                    if m_attempts == 0:
                        raise
                    
                    logger.warning(
                        f"Function {func.__name__} failed with {str(e)}. "
                        f"Retrying in {m_delay} seconds... "
                        f"({max_attempts - m_attempts}/{max_attempts - 1})"
                    )
                    
                    time.sleep(m_delay)
                    m_delay *= backoff
        return wrapper
    return decorator


@retry(max_attempts=3, delay=2, exceptions=(requests.RequestException,))
def fetch_url(url, params=None, headers=None, timeout=30):
    """
    Fetch content from URL with retries and error handling.
    
    Args:
        url (str): URL to fetch
        params (dict, optional): Query parameters
        headers (dict, optional): HTTP headers
        timeout (int, optional): Request timeout in seconds
    
    Returns:
        str: Response text if successful
    
    Raises:
        requests.RequestException: If request fails after retries
    """
    default_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    if headers:
        default_headers.update(headers)
    
    response = requests.get(url, params=params, headers=default_headers, timeout=timeout)
    response.raise_for_status()
    
    return response.text


def parse_html(html_content, selector=None):
    """
    Parse HTML content using BeautifulSoup.
    
    Args:
        html_content (str): HTML content to parse
        selector (str, optional): CSS selector to extract specific elements
    
    Returns:
        BeautifulSoup: Parsed HTML or selected elements
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    if selector:
        return soup.select(selector)
    
    return soup


def date_range(start_date, end_date=None, as_string=False, fmt="%Y-%m-%d"):
    """
    Generate a list of dates between start_date and end_date.
    
    Args:
        start_date (str or datetime): Start date
        end_date (str or datetime, optional): End date, defaults to today
        as_string (bool): Return dates as strings if True, datetime objects if False
        fmt (str): Date format string if using string inputs/outputs
    
    Returns:
        list: List of dates in the range
    """
    # Convert string dates to datetime objects
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, fmt)
    
    if end_date is None:
        end_date = datetime.now()
    elif isinstance(end_date, str):
        end_date = datetime.strptime(end_date, fmt)
    
    # Generate date range
    dates = []
    current_date = start_date
    
    while current_date <= end_date:
        if as_string:
            dates.append(current_date.strftime(fmt))
        else:
            dates.append(current_date)
        
        current_date += timedelta(days=1)
    
    return dates


def ensure_directory_exists(path):
    """
    Ensure that a directory exists, creating it if necessary.
    
    Args:
        path (str or Path): Directory path
    
    Returns:
        Path: Path object for the directory
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def format_ticker_symbol(symbol):
    """
    Format ticker symbol according to PSX standards.
    
    Args:
        symbol (str): Ticker symbol
    
    Returns:
        str: Formatted ticker symbol
    """
    # Strip whitespace, convert to uppercase
    symbol = symbol.strip().upper()
    
    # Remove any .PA or similar suffixes
    if '.' in symbol:
        symbol = symbol.split('.')[0]
    
    return symbol 