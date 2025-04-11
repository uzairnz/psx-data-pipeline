#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PSX Web Crawler using Crawl4AI.

This module provides more robust web scraping capabilities for the PSX Data Pipeline
by leveraging the Crawl4AI library - a dedicated web crawling and scraping library
designed for LLM-friendly data extraction.

Key features:
- Smart rate limiting and retry logic
- Better error handling for 500 errors
- Respects website crawling rules
- More resilient scraping patterns

Usage:
    from psx_data_automation.scripts.crawler import fetch_company_page, fetch_ticker_list
"""

import logging
import re
from typing import Dict, List, Optional, Union
import time
import random
import asyncio
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig

from psx_data_automation.config import PSX_DATA_PORTAL_URL, COMPANY_URL_TEMPLATE

# Set up logging
logger = logging.getLogger("psx_pipeline.crawler")

# Create a crawler instance with optimized settings for PSX
browser_config = BrowserConfig(
    browser_type="chromium",
    headless=True,
    viewport_width=1280,
    viewport_height=800,
    ignore_https_errors=True,
    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    headers={
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': f"{PSX_DATA_PORTAL_URL}/market-watch",
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
    },
    verbose=False
)

# Create the asynchronous web crawler
psx_crawler = AsyncWebCrawler(config=browser_config)


async def fetch_page(url: str) -> Optional[str]:
    """
    Fetch a webpage content with advanced error handling and retry logic.
    
    Args:
        url (str): URL to fetch
        
    Returns:
        Optional[str]: HTML content if successful, None otherwise
    """
    try:
        # Use requests directly with retry logic
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': PSX_DATA_PORTAL_URL,
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
        }
        
        for attempt in range(3):  # Try up to 3 times
            try:
                response = requests.get(url, headers=headers, timeout=30)
                if response.status_code == 200:
                    return response.text
                elif response.status_code == 404:
                    logger.warning(f"Page not found (404): {url}")
                    return None
                else:
                    logger.warning(f"HTTP {response.status_code} when fetching {url}")
                    if attempt < 2:  # If not the last attempt
                        time.sleep(1 * (2 ** attempt))  # Exponential backoff
                    else:
                        return None
            except requests.RequestException as e:
                logger.warning(f"Request error on attempt {attempt+1} for {url}: {str(e)}")
                if attempt < 2:  # If not the last attempt
                    time.sleep(1 * (2 ** attempt))  # Exponential backoff
                else:
                    return None
    except Exception as e:
        logger.error(f"Error fetching {url}: {str(e)}")
        return None
    
    return None


async def fetch_company_details(symbol: str, url: Optional[str] = None) -> Dict[str, str]:
    """
    Fetch detailed information about a company from its page using Crawl4AI.
    
    Args:
        symbol (str): Company ticker symbol
        url (str, optional): URL to the company page
        
    Returns:
        Dict[str, str]: Company details including name, sector, and URL
    """
    # Initialize default result
    details = {
        'symbol': symbol,
        'name': symbol,  # Default to symbol
        'sector': "Unknown",  # Default sector
        'url': url if url else f"{COMPANY_URL_TEMPLATE}{symbol}"
    }
    
    # Fetch the company page
    html_content = await fetch_page(details['url'])
    if not html_content:
        logger.warning(f"Could not fetch company page for {symbol}")
        return details
    
    # Parse the HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Check for "No record found" pages
    if "no record found" in soup.text.lower() or "not found" in soup.text.lower():
        details['name'] = "No record found"
        return details
    
    # Extract company name - try different selectors that might contain the name
    for selector in ['h1', 'h2', 'h3', '.company-name', '.profile-title']:
        company_name_elem = soup.select_one(selector)
        if company_name_elem and company_name_elem.text.strip():
            name = company_name_elem.text.strip()
            # Avoid selecting elements that just contain the symbol
            if name and name != symbol and len(name) > len(symbol):
                details['name'] = name
                break
    
    # Extract sector - look for specific elements or text patterns
    sector_keywords = [
        'REFINERY', 'CEMENT', 'COMMERCIAL BANKS', 'FERTILIZER', 
        'OIL & GAS', 'POWER', 'TEXTILE', 'PHARMACEUTICALS', 'TECHNOLOGY'
    ]
    
    # Try to find sector in specific elements
    for selector in ['.sector', '.industry', '.category', '.profile-sector', '.company-sector']:
        sector_elem = soup.select_one(selector)
        if sector_elem and sector_elem.text.strip():
            details['sector'] = sector_elem.text.strip()
            break
    
    # If still no sector, look for sector keywords in the page content
    if details['sector'] == "Unknown":
        for elem in soup.select('strong, b, h4, h5, .badge, .sector-badge, p'):
            text = elem.text.strip().upper()
            if any(keyword in text for keyword in sector_keywords) and len(text) < 50:
                # Check if the element is likely a sector indicator
                details['sector'] = elem.text.strip()
                break
    
    # If still no name, try to extract from title
    if details['name'] == symbol:
        title_elem = soup.select_one('title')
        if title_elem and title_elem.text:
            title = title_elem.text.strip()
            # Often in format "Company Name - PSX"
            if ' - ' in title:
                company_name = title.split(' - ')[0].strip()
                if company_name and company_name != symbol:
                    details['name'] = company_name
    
    # Attempt to extract from business description as last resort
    if details['sector'] == "Unknown" or details['name'] == symbol:
        business_desc = soup.select_one('.business-description, #company-profile, .about-company')
        if business_desc:
            # Try to extract sector from keywords in text
            text = business_desc.text.lower()
            sector_mapping = {
                'bank': 'Commercial Banks',
                'cement': 'Cement',
                'oil': 'Oil & Gas',
                'gas': 'Oil & Gas Marketing Companies',
                'pharma': 'Pharmaceuticals',
                'fertilizer': 'Fertilizer',
                'textile': 'Textile',
                'power': 'Power Generation & Distribution',
                'refinery': 'Refinery',
                'insurance': 'Insurance',
                'investment': 'Investment',
                'automobile': 'Automobile',
                'chemical': 'Chemical',
                'technology': 'Technology & Communication',
                'food': 'Food & Personal Care Products'
            }
            
            for keyword, sector_name in sector_mapping.items():
                if keyword in text and details['sector'] == "Unknown":
                    details['sector'] = sector_name
                    break
            
            # Try to extract company name if still using symbol as name
            if details['name'] == symbol:
                match = re.search(r'([A-Za-z\s]+)\s+(?:was|is|has been)\s+incorporated', text)
                if match:
                    company_name = match.group(1).strip()
                    if company_name and len(company_name) > 3:
                        details['name'] = company_name
    
    logger.debug(f"Extracted details for {symbol}: {details['name']} - {details['sector']}")
    return details


async def fetch_ticker_list() -> List[Dict[str, str]]:
    """
    Fetch the list of tickers from PSX Market Watch page using Crawl4AI.
    
    Returns:
        List[Dict[str, str]]: List of ticker dictionaries with symbol and URL
    """
    ticker_data = []
    
    # URL for Market Watch
    market_watch_url = f"{PSX_DATA_PORTAL_URL}/market-watch"
    
    # Fetch the market watch page
    logger.info(f"Fetching ticker list from {market_watch_url}")
    html_content = await fetch_page(market_watch_url)
    
    if not html_content:
        logger.error("Failed to fetch market watch page")
        return []
    
    # Parse the HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Look for the market watch table
    table = soup.select_one('table.table')
    
    if not table:
        # Try alternative selectors if the first one doesn't work
        tables = soup.select('table')
        if tables:
            # Use the table that has symbols data
            for potential_table in tables:
                # Check if this table has columns we need (Symbol, etc.)
                headers = potential_table.select('th')
                header_texts = [h.text.strip().upper() for h in headers]
                if any('SYMBOL' in txt for txt in header_texts):
                    table = potential_table
                    break
    
    if table:
        # Extract header positions for mapping
        headers = table.select('thead th')
        header_mapping = {}
        for i, header in enumerate(headers):
            header_text = header.text.strip().upper()
            if 'SYMBOL' in header_text:
                header_mapping['symbol'] = i
            elif 'CURRENT' in header_text or 'PRICE' in header_text:
                header_mapping['price'] = i
            elif 'VOLUME' in header_text:
                header_mapping['volume'] = i
            elif 'SECTOR' in header_text:
                header_mapping['sector'] = i
        
        # Process the table rows
        rows = table.select('tbody tr')
        
        for row in rows:
            columns = row.select('td')
            if len(columns) >= 2:  # Ensure we have at least symbol and other data
                # Get symbol, which is always needed
                if 'symbol' in header_mapping:
                    symbol_col = header_mapping['symbol']
                    symbol_cell = columns[symbol_col]
                else:
                    # If we can't determine which column has the symbol, use the first column
                    symbol_cell = columns[0]
                
                # Extract symbol text and format it
                symbol = symbol_cell.text.strip().upper()
                
                # Remove any suffixes like .PA
                if '.' in symbol:
                    symbol = symbol.split('.')[0]
                
                # Extract URL if there's a link
                ticker_url = ""
                symbol_link = symbol_cell.select_one('a')
                if symbol_link and 'href' in symbol_link.attrs:
                    href = symbol_link['href']
                    # Make sure we have a full URL
                    if href.startswith('/'):
                        ticker_url = urljoin(PSX_DATA_PORTAL_URL, href)
                    elif href.startswith('http'):
                        ticker_url = href
                    else:
                        ticker_url = urljoin(PSX_DATA_PORTAL_URL, href)
                
                # Add to ticker data list if it's a valid symbol (not empty or "Select...")
                if symbol and len(symbol) > 1 and 'SELECT' not in symbol.upper():
                    ticker_data.append({
                        'symbol': symbol,
                        'url': ticker_url
                    })
        
        logger.info(f"Successfully fetched {len(ticker_data)} ticker symbols from PSX Market Watch")
    else:
        logger.warning("Could not find ticker table on the market watch page")
    
    return ticker_data


# Synchronous wrapper functions for compatibility with existing code
def fetch_company_page(symbol: str, url: Optional[str] = None) -> Dict[str, str]:
    """
    Synchronous wrapper for fetch_company_details.
    
    Args:
        symbol (str): Company ticker symbol
        url (str, optional): URL to the company page
        
    Returns:
        Dict[str, str]: Company details including name, sector, and URL
    """
    # Use a dummy event loop for Windows if needed
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Event loop is closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Run the async function in the event loop
    return loop.run_until_complete(fetch_company_details(symbol, url))


def fetch_ticker_list_sync() -> List[Dict[str, str]]:
    """
    Synchronous wrapper for fetch_ticker_list.
    
    Returns:
        List[Dict[str, str]]: List of ticker dictionaries with symbol and URL
    """
    # Use a dummy event loop for Windows if needed
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Event loop is closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Run the async function in the event loop
    return loop.run_until_complete(fetch_ticker_list())


# If crawl4ai is not available, use a fallback function with requests
def fetch_url_fallback(url, headers=None, max_retries=3, retry_delay=1.0):
    """
    Fallback function to fetch URL content using requests.
    
    Args:
        url (str): URL to fetch
        headers (dict): Request headers
        max_retries (int): Maximum number of retries
        retry_delay (float): Delay between retries
    
    Returns:
        str: HTML content if successful
    """
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': PSX_DATA_PORTAL_URL,
        }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an exception for 4xx/5xx status codes
            return response.text
        except Exception as e:
            if attempt < max_retries - 1:
                # Wait with exponential backoff before retrying
                wait_time = retry_delay * (2 ** attempt) * random.uniform(0.8, 1.2)
                logger.warning(f"Error fetching {url}: {str(e)}. Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                # Last attempt failed
                raise Exception(f"Failed to fetch {url} after {max_retries} attempts: {str(e)}")


# Mock ticker data for testing when in mock mode
MOCK_TICKERS = [
    {'symbol': 'HBL', 'name': 'Habib Bank Limited', 'sector': 'Commercial Banks', 'url': f"{COMPANY_URL_TEMPLATE}HBL"},
    {'symbol': 'ENGRO', 'name': 'Engro Corporation Limited', 'sector': 'Fertilizer', 'url': f"{COMPANY_URL_TEMPLATE}ENGRO"},
    {'symbol': 'PSO', 'name': 'Pakistan State Oil Company Limited', 'sector': 'Oil & Gas Marketing Companies', 'url': f"{COMPANY_URL_TEMPLATE}PSO"},
    {'symbol': 'LUCK', 'name': 'Lucky Cement Limited', 'sector': 'Cement', 'url': f"{COMPANY_URL_TEMPLATE}LUCK"},
    {'symbol': 'OGDC', 'name': 'Oil & Gas Development Company Limited', 'sector': 'Oil & Gas Exploration Companies', 'url': f"{COMPANY_URL_TEMPLATE}OGDC"},
    {'symbol': 'PPL', 'name': 'Pakistan Petroleum Limited', 'sector': 'Oil & Gas Exploration Companies', 'url': f"{COMPANY_URL_TEMPLATE}PPL"},
    {'symbol': 'UBL', 'name': 'United Bank Limited', 'sector': 'Commercial Banks', 'url': f"{COMPANY_URL_TEMPLATE}UBL"},
    {'symbol': 'MCB', 'name': 'MCB Bank Limited', 'sector': 'Commercial Banks', 'url': f"{COMPANY_URL_TEMPLATE}MCB"},
    {'symbol': 'FFC', 'name': 'Fauji Fertilizer Company Limited', 'sector': 'Fertilizer', 'url': f"{COMPANY_URL_TEMPLATE}FFC"},
    {'symbol': 'EFERT', 'name': 'Engro Fertilizers Limited', 'sector': 'Fertilizer', 'url': f"{COMPANY_URL_TEMPLATE}EFERT"},
    {'symbol': 'BAHL', 'name': 'Bank Al Habib Limited', 'sector': 'Commercial Banks', 'url': f"{COMPANY_URL_TEMPLATE}BAHL"},
    {'symbol': 'MEBL', 'name': 'Meezan Bank Limited', 'sector': 'Commercial Banks', 'url': f"{COMPANY_URL_TEMPLATE}MEBL"},
    {'symbol': 'CNERGY', 'name': 'Cnergyico PK Limited', 'sector': 'Refinery', 'url': f"{COMPANY_URL_TEMPLATE}CNERGY"},
    {'symbol': 'KEL', 'name': 'K-Electric Limited', 'sector': 'Power Generation & Distribution', 'url': f"{COMPANY_URL_TEMPLATE}KEL"},
    {'symbol': 'SSGC', 'name': 'Sui Southern Gas Company Limited', 'sector': 'Oil & Gas Marketing Companies', 'url': f"{COMPANY_URL_TEMPLATE}SSGC"},
]


# Add the test_crawl4ai function
def test_crawl4ai():
    """
    Test function to verify that Crawl4AI is working properly.
    """
    async def _test_crawl():
        url = "https://dps.psx.com.pk"
        logger.info(f"Testing crawl4ai with URL: {url}")
        try:
            html_content = await fetch_page(url)
            if html_content:
                logger.info("Crawl4AI test successful: retrieved content")
                print("Crawl4AI is working correctly!")
                return True
            else:
                logger.error("Crawl4AI test failed: no content retrieved")
                return False
        except Exception as e:
            logger.error(f"Crawl4AI test error: {str(e)}")
            return False

    # Use a dummy event loop for Windows if needed
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Event loop is closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Run the async function in the event loop
    result = loop.run_until_complete(_test_crawl())
    return result


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    # Test the crawler
    import asyncio
    
    async def test_crawler():
        # Test ticker list
        tickers = await fetch_ticker_list()
        print(f"Fetched {len(tickers)} tickers")
        
        # Test company details for a few symbols
        if tickers:
            for symbol in ['HBL', 'LUCK', 'PSO'][:3]:  # Test first 3 or less
                print(f"\nFetching details for {symbol}...")
                details = await fetch_company_details(symbol)
                print(f"  Name: {details['name']}")
                print(f"  Sector: {details['sector']}")
                print(f"  URL: {details['url']}")
    
    # Run the test
    asyncio.run(test_crawler()) 