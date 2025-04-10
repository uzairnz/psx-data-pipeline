#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Historical Data Downloader for PSX Data Pipeline.

This module provides functionality to download historical price data for PSX stocks
using the crawl4ai library for efficient async requests. It supports:
- Downloading data for multiple tickers in parallel
- Handling rate limiting and retries
- Parsing and formatting historical price data
"""

import asyncio
import logging
import os
import re
import json
import time
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from psx_data_automation.config import (
    PSX_DATA_PORTAL_URL,
    DATA_DIR,
    COMPANY_URL_TEMPLATE
)
from psx_data_automation.scripts.crawler import (
    psx_crawler,
    fetch_page,
    fetch_url_fallback
)

# Set up logging
logger = logging.getLogger("psx_pipeline.historical_data")

# Mock historical data template for testing
MOCK_HISTORICAL_DATA_TEMPLATE = """Date,Open,High,Low,Close,Volume
{date_1},{open_1},{high_1},{low_1},{close_1},{volume_1}
{date_2},{open_2},{high_2},{low_2},{close_2},{volume_2}
{date_3},{open_3},{high_3},{low_3},{close_3},{volume_3}
{date_4},{open_4},{high_4},{low_4},{close_4},{volume_4}
{date_5},{open_5},{high_5},{low_5},{close_5},{volume_5}
"""


async def download_historical_data(
    symbol: str, 
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    save_to_file: bool = True,
    mock_mode: bool = False
) -> Optional[pd.DataFrame]:
    """
    Download historical price data for a given ticker symbol.
    
    Args:
        symbol (str): The ticker symbol
        start_date (datetime, optional): Start date for data download
        end_date (datetime, optional): End date for data download
        save_to_file (bool): Whether to save data to CSV file
        mock_mode (bool): Whether to use mock data for testing
        
    Returns:
        Optional[pd.DataFrame]: DataFrame with historical price data if successful
    """
    if mock_mode:
        return _generate_mock_data(symbol, start_date, end_date, save_to_file)
    
    logger.info(f"Downloading historical data for {symbol}")
    
    # Set default date range if not provided
    if end_date is None:
        end_date = datetime.now()
    if start_date is None:
        start_date = end_date - timedelta(days=365 * 5)  # 5 years by default
    
    # Format dates for the request
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    # Build the historical data URL
    # This template will need to be updated based on the actual PSX data portal structure
    hist_data_url = f"{PSX_DATA_PORTAL_URL}/historical-data/{symbol}?from={start_str}&to={end_str}"
    
    try:
        # Fetch the page content
        html_content = await fetch_page(hist_data_url)
        
        if not html_content:
            logger.error(f"Failed to fetch historical data for {symbol}")
            return None
        
        # Parse the historical data from the HTML
        df = _parse_historical_data(html_content, symbol)
        
        if df is None or df.empty:
            logger.warning(f"No historical data found for {symbol}")
            return None
        
        # Save to file if requested
        if save_to_file and df is not None and not df.empty:
            file_path = os.path.join(DATA_DIR, f"{symbol}.csv")
            df.to_csv(file_path, index=False)
            logger.info(f"Saved historical data for {symbol} to {file_path}")
        
        return df
    
    except Exception as e:
        logger.error(f"Error downloading historical data for {symbol}: {str(e)}")
        return None


def _parse_historical_data(html_content: str, symbol: str) -> Optional[pd.DataFrame]:
    """
    Parse historical data from HTML content.
    
    Args:
        html_content (str): HTML content of the historical data page
        symbol (str): The ticker symbol
        
    Returns:
        Optional[pd.DataFrame]: DataFrame with parsed historical data
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the historical data table
        # This selector will need to be updated based on the actual PSX data portal structure
        tables = soup.select('table.table')
        
        if not tables:
            # Try alternative selectors
            tables = soup.select('table')
        
        if not tables:
            logger.warning(f"No tables found in historical data page for {symbol}")
            return None
        
        # Find the right table with historical data
        hist_table = None
        for table in tables:
            headers = [th.text.strip().upper() for th in table.select('th')]
            # Look for key columns like Date, Open, High, Low, Close, Volume
            if any('DATE' in h for h in headers) and any('CLOSE' in h for h in headers):
                hist_table = table
                break
        
        if hist_table is None:
            logger.warning(f"Could not find historical data table for {symbol}")
            return None
        
        # Extract header positions
        headers = [th.text.strip().upper() for th in hist_table.select('th')]
        
        # Map column indices
        column_map = {}
        for i, header in enumerate(headers):
            if 'DATE' in header:
                column_map['date'] = i
            elif 'OPEN' in header:
                column_map['open'] = i
            elif 'HIGH' in header:
                column_map['high'] = i
            elif 'LOW' in header:
                column_map['low'] = i
            elif 'CLOSE' in header or 'ADJ' in header or 'LAST' in header:
                column_map['close'] = i
            elif 'VOLUME' in header or 'VOL' in header:
                column_map['volume'] = i
        
        # Required columns
        required_cols = ['date', 'close']
        if not all(col in column_map for col in required_cols):
            logger.warning(f"Missing required columns in historical data for {symbol}")
            return None
        
        # Parse table rows
        data = []
        for row in hist_table.select('tbody tr'):
            cells = row.select('td')
            if len(cells) < len(headers):
                continue
            
            row_data = {}
            for col, idx in column_map.items():
                if idx < len(cells):
                    cell_text = cells[idx].text.strip()
                    
                    # Parse date
                    if col == 'date':
                        # Try different date formats
                        try:
                            # Try to extract date using regex to handle various formats
                            date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})', cell_text)
                            if date_match:
                                cell_text = date_match.group(0)
                                
                            # Try various date formats
                            for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%y'):
                                try:
                                    dt = datetime.strptime(cell_text, fmt)
                                    row_data[col] = dt.strftime('%Y-%m-%d')
                                    break
                                except ValueError:
                                    continue
                            
                            # If no format worked, keep original text
                            if col not in row_data:
                                row_data[col] = cell_text
                        except Exception:
                            row_data[col] = cell_text
                    
                    # Parse numeric values
                    elif col in ('open', 'high', 'low', 'close'):
                        try:
                            # Extract numeric part, handle commas and currency symbols
                            clean_text = re.sub(r'[^\d.-]', '', cell_text)
                            if clean_text:
                                row_data[col] = float(clean_text)
                            else:
                                row_data[col] = None
                        except ValueError:
                            row_data[col] = None
                    
                    # Parse volume
                    elif col == 'volume':
                        try:
                            # Extract numeric part, handle commas and K/M suffixes
                            clean_text = cell_text.replace(',', '')
                            if 'K' in clean_text.upper():
                                vol = float(clean_text.upper().replace('K', '')) * 1000
                            elif 'M' in clean_text.upper():
                                vol = float(clean_text.upper().replace('M', '')) * 1000000
                            else:
                                vol = float(re.sub(r'[^\d.]', '', clean_text))
                            row_data[col] = int(vol)
                        except ValueError:
                            row_data[col] = None
            
            # Add row if we have at least date and close price
            if 'date' in row_data and 'close' in row_data and row_data['date'] and row_data['close']:
                data.append(row_data)
        
        # Create DataFrame
        if data:
            df = pd.DataFrame(data)
            
            # Add missing columns with NaN
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col not in df.columns:
                    df[col] = None
            
            # Ensure all columns have the right types
            for col in ['open', 'high', 'low', 'close']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            if 'volume' in df.columns:
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(int)
            
            # Sort by date
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.sort_values('date')
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            # Standardize column order
            cols = ['date', 'open', 'high', 'low', 'close', 'volume']
            df = df[[col for col in cols if col in df.columns]]
            
            return df
        
        return None
    
    except Exception as e:
        logger.error(f"Error parsing historical data for {symbol}: {str(e)}")
        return None


def _generate_mock_data(
    symbol: str, 
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    save_to_file: bool = True
) -> pd.DataFrame:
    """
    Generate mock historical data for testing.
    
    Args:
        symbol (str): The ticker symbol
        start_date (datetime, optional): Start date for data
        end_date (datetime, optional): End date for data
        save_to_file (bool): Whether to save data to CSV file
        
    Returns:
        pd.DataFrame: DataFrame with mock historical data
    """
    logger.info(f"Generating mock data for {symbol}")
    
    # Set default date range if not provided
    if end_date is None:
        end_date = datetime.now()
    if start_date is None:
        start_date = end_date - timedelta(days=30)  # 30 days for mock data
    
    # Generate date range
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        # Skip weekends
        if current_date.weekday() < 5:  # 0-4 are Monday to Friday
            date_range.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    # Reverse to have newest dates first
    date_range.reverse()
    
    # Generate random price data
    import random
    
    # Base price for the stock (use symbol's numerical position in alphabet for variety)
    base_price = sum([ord(c) - ord('A') + 1 for c in symbol if c.isalpha()]) * 5
    if base_price < 10:
        base_price = 50  # Minimum base price
    
    data = []
    prev_close = base_price
    
    for date in date_range:
        # Generate daily volatility (0.5% to 1.5%)
        volatility = random.uniform(0.005, 0.015)
        
        # Generate daily price movement (-1.5% to +1.5%)
        day_change_pct = random.uniform(-0.015, 0.015)
        
        # Calculate prices
        close = prev_close * (1 + day_change_pct)
        high = close * (1 + volatility * random.uniform(0.2, 1.0))
        low = close * (1 - volatility * random.uniform(0.2, 1.0))
        open_price = prev_close * (1 + random.uniform(-0.005, 0.005))
        
        # Ensure high >= open, close and low <= open, close
        high = max(high, open_price, close)
        low = min(low, open_price, close)
        
        # Generate random volume (higher for larger price movements)
        volume = int(random.uniform(100000, 1000000) * (1 + abs(day_change_pct) * 10))
        
        data.append({
            'date': date,
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': volume
        })
        
        prev_close = close
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Save to file if requested
    if save_to_file:
        file_path = os.path.join(DATA_DIR, f"{symbol}.csv")
        df.to_csv(file_path, index=False)
        logger.info(f"Saved mock historical data for {symbol} to {file_path}")
    
    return df


async def download_batch(symbols: List[str], mock_mode: bool = False) -> Dict[str, pd.DataFrame]:
    """
    Download historical data for multiple symbols in parallel.
    
    Args:
        symbols (List[str]): List of ticker symbols
        mock_mode (bool): Whether to use mock data for testing
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping symbols to their historical data
    """
    logger.info(f"Downloading historical data for {len(symbols)} symbols")
    
    tasks = []
    for symbol in symbols:
        task = download_historical_data(symbol, mock_mode=mock_mode)
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results
    data_dict = {}
    for symbol, result in zip(symbols, results):
        if isinstance(result, Exception):
            logger.error(f"Error downloading data for {symbol}: {str(result)}")
        elif result is not None:
            data_dict[symbol] = result
    
    logger.info(f"Successfully downloaded data for {len(data_dict)} out of {len(symbols)} symbols")
    return data_dict


def download_ticker_data(symbols: List[str], mock_mode: bool = False) -> Dict[str, pd.DataFrame]:
    """
    Synchronous wrapper for download_batch.
    
    Args:
        symbols (List[str]): List of ticker symbols
        mock_mode (bool): Whether to use mock data for testing
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping symbols to their historical data
    """
    try:
        # Ensure DATA_DIR exists
        os.makedirs(DATA_DIR, exist_ok=True)
        
        # Use a dummy event loop for Windows if needed
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Run the async function in the event loop
        return loop.run_until_complete(download_batch(symbols, mock_mode))
    
    except Exception as e:
        logger.error(f"Error in download_ticker_data: {str(e)}")
        return {}


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    # Test the historical data downloader
    import asyncio
    
    async def test_historical_data():
        # Test with a few symbols
        symbols = ['HBL', 'LUCK', 'PSO']
        results = await download_batch(symbols, mock_mode=True)
        
        for symbol, df in results.items():
            print(f"\nData for {symbol}:")
            print(df.head())
    
    # Run the test
    asyncio.run(test_historical_data()) 