#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script for historical data download.

This script tests the historical data download functionality using a small subset
of tickers to verify that the download process works correctly.

Usage:
    python -m psx_data_automation.scripts.test_historical_data
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from psx_data_automation.config import DATA_DIR
from psx_data_automation.scripts.download_data import (
    download_historical_data,
    generate_mock_data,
    save_historical_data
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("psx_pipeline.test_historical")

# Test settings
TEST_TICKERS = [
    {"symbol": "HBL", "name": "Habib Bank Limited", "sector": "Commercial Banks"},
    {"symbol": "ENGRO", "name": "Engro Corporation Limited", "sector": "Fertilizer"},
    {"symbol": "LUCK", "name": "Lucky Cement Limited", "sector": "Cement"},
    {"symbol": "PSO", "name": "Pakistan State Oil Company Limited", "sector": "Oil & Gas Marketing Companies"},
    {"symbol": "OGDC", "name": "Oil & Gas Development Company Limited", "sector": "Oil & Gas Exploration Companies"}
]


def test_generate_mock_data():
    """Test the mock data generation."""
    logger.info("Testing mock data generation")
    
    symbol = "TEST"
    start_date = "2020-01-01"
    end_date = "2020-12-31"
    
    # Generate mock data
    data = generate_mock_data(symbol, start_date, end_date)
    
    # Verify data
    if data.empty:
        logger.error("Failed to generate mock data")
        return False
    
    # Check date range
    min_date = data['date'].min().strftime("%Y-%m-%d")
    max_date = data['date'].max().strftime("%Y-%m-%d")
    
    logger.info(f"Generated {len(data)} rows of mock data for {symbol}")
    logger.info(f"Date range: {min_date} to {max_date}")
    
    # Check columns
    expected_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in expected_columns if col not in data.columns]
    
    if missing_columns:
        logger.error(f"Missing columns in mock data: {missing_columns}")
        return False
    
    logger.info("Mock data generation test passed")
    return True


def test_save_load_data():
    """Test saving and loading historical data."""
    logger.info("Testing data save and load functionality")
    
    # Generate test data
    symbol = "TEST_SAVE_LOAD"
    data = generate_mock_data(symbol, "2020-01-01", "2020-12-31")
    
    # Save data
    file_path = DATA_DIR / f"{symbol}.csv"
    if file_path.exists():
        os.remove(file_path)
    
    success = save_historical_data(symbol, data)
    if not success:
        logger.error("Failed to save test data")
        return False
    
    # Verify file exists
    if not file_path.exists():
        logger.error(f"File not created: {file_path}")
        return False
    
    # Load data back
    loaded_data = pd.read_csv(file_path)
    
    # Verify data integrity
    if len(loaded_data) != len(data):
        logger.error(f"Data size mismatch: {len(loaded_data)} != {len(data)}")
        return False
    
    logger.info(f"Successfully saved and loaded {len(data)} rows of data")
    logger.info("Data save/load test passed")
    
    # Clean up
    os.remove(file_path)
    return True


def test_historical_download():
    """Test downloading historical data for a small set of tickers."""
    logger.info("Testing historical data download with mock data")
    
    # Create a timestamp for test files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Download data using mock mode
    stats = download_historical_data(tickers=TEST_TICKERS, use_mock=True)
    
    if stats['status'] != 'success':
        logger.error(f"Download process failed: {stats.get('error', 'Unknown error')}")
        return False
    
    # Verify results
    if stats['successful'] != len(TEST_TICKERS):
        logger.error(f"Not all tickers were processed successfully: {stats['successful']}/{len(TEST_TICKERS)}")
        return False
    
    # Check if files were created
    for ticker in TEST_TICKERS:
        symbol = ticker['symbol']
        file_path = DATA_DIR / f"{symbol}.csv"
        
        if not file_path.exists():
            logger.error(f"Data file not created for {symbol}")
            return False
        
        # Verify file content
        try:
            data = pd.read_csv(file_path)
            if data.empty:
                logger.error(f"Empty data file for {symbol}")
                return False
            
            logger.info(f"Successfully verified data for {symbol}: {len(data)} rows")
        except Exception as e:
            logger.error(f"Error reading data file for {symbol}: {str(e)}")
            return False
    
    logger.info("Historical data download test passed")
    return True


def test_real_download():
    """Test downloading real historical data for one ticker."""
    logger.info("Testing real historical data download (limited to one ticker)")
    
    # Use just one ticker to minimize load on the server
    test_ticker = TEST_TICKERS[0]
    symbol = test_ticker['symbol']
    
    # Download actual data (not mock)
    stats = download_historical_data(tickers=[test_ticker], use_mock=False)
    
    if stats['status'] != 'success':
        logger.warning(f"Real download test failed - this may be due to network issues or website changes")
        logger.warning(f"Error: {stats.get('error', 'Unknown error')}")
        return False
    
    # Check if file was created
    file_path = DATA_DIR / f"{symbol}.csv"
    if not file_path.exists():
        logger.warning(f"Real download did not create data file for {symbol}")
        return False
    
    # Verify file content
    try:
        data = pd.read_csv(file_path)
        if data.empty:
            logger.warning(f"Empty data file from real download for {symbol}")
            return False
        
        logger.info(f"Successfully downloaded real data for {symbol}: {len(data)} rows")
        logger.info(f"Date range: {data['date'].min()} to {data['date'].max()}")
        
        # Display sample data
        logger.info("Sample data:")
        logger.info(data.head().to_string())
        
    except Exception as e:
        logger.warning(f"Error reading real data file for {symbol}: {str(e)}")
        return False
    
    logger.info("Real historical data download test passed")
    return True


def run_all_tests():
    """Run all test functions."""
    logger.info("Starting historical data download tests")
    
    tests = [
        ("Mock data generation", test_generate_mock_data),
        ("Data save/load", test_save_load_data),
        ("Mock historical download", test_historical_download),
        ("Real data download (optional)", test_real_download)
    ]
    
    results = []
    
    for name, test_func in tests:
        logger.info(f"\n{'='*50}\nTest: {name}\n{'='*50}")
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            logger.error(f"Test {name} raised an exception: {str(e)}")
            results.append((name, False))
    
    # Print summary
    logger.info("\n\n" + "="*50)
    logger.info("Test Results Summary:")
    logger.info("="*50)
    
    all_passed = True
    for name, success in results:
        status = "PASSED" if success else "FAILED"
        logger.info(f"{name}: {status}")
        all_passed = all_passed and success
    
    if all_passed:
        logger.info("\nAll tests PASSED!")
    else:
        logger.info("\nSome tests FAILED!")
    
    return all_passed


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1) 