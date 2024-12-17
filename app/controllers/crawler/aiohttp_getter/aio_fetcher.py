import re

import aiohttp
import urllib3
from urllib.parse import urlencode
import json
import time
import random
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
import asyncio

from sqlalchemy.util import await_only
from tqdm.asyncio import tqdm_asyncio

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}


MAX_RETRIES = 25
RETRY_DELAY = 3  # Initial retry delay in seconds
BACKOFF_FACTOR = 1.05  # Exponential backoff factor

# Create a single PoolManager instance to be reused across requests
http = urllib3.PoolManager()

async def fetch(url: str, params: Dict[str, str] = None) -> Dict[str, Any]:
    """Asynchronous wrapper for the synchronous _fetch function."""
    return await asyncio.to_thread(_fetch, url, params)

def _fetch(url: str, params: Dict[str, str] = None) -> Dict[str, Any]:
    """Fetch data with exponential backoff and infinite retries, also extract group ID."""
    retries = 0
    while True:
        try:
            response = http.request('GET', url, headers=headers, fields=params, timeout=5)
            if response.status >= 400:
                raise urllib3.exceptions.HTTPError(f"HTTP Error: {response.status}")
            data = json.loads(response.data.decode('utf-8'))

            # Extract group ID from the URL
            match = re.search(r'/\d{8}(?=/|$)', url)
            if match:
                data['dEven'] = match.group(0)[1:9]  # Extract year and month (YYYYMM)

            # Add the input URL to the response data
            data['url'] = url

            return data
        except Exception as e:
            retries += 1
            print(f"Error fetching {url}: {str(e)}, retrying... (attempt {retries})")
            sleep_time = RETRY_DELAY * (BACKOFF_FACTOR ** retries) + random.uniform(0, 1)
            print(sleep_time)
            time.sleep(sleep_time)

async def fetch_xls_table(url: str) -> Any:
    """Asynchronous wrapper for the synchronous _fetch_xls_table function."""
    return await asyncio.to_thread(_fetch_xls_table, url)

def _fetch_xls_table(url: str) -> Any:
    """Fetch data from the page and parse it into a BeautifulSoup table."""
    while True:
        try:
            data = {'__EVENTTARGET': 'exportbtn'}
            encoded_data = urlencode(data).encode('utf-8')
            temp_headers = headers.copy()
            temp_headers['Content-Type'] = 'application/x-www-form-urlencoded'
            response = http.request('POST', url, headers=temp_headers, body=encoded_data)
            if response.status >= 400:
                raise urllib3.exceptions.HTTPError(f"HTTP Error: {response.status}")
            html_content = response.data.decode('utf-8')
            return html_content
        except Exception as e:
            print(f"Error fetching {url}: {str(e)}, retrying...")
            time.sleep(RETRY_DELAY)

async def crawl_data_xls_parser(urls: List[str]) -> List[Any]:
    """Crawl a list of URLs and return the gathered data asynchronously."""
    tasks = [fetch_xls_table(url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # Filter out exceptions if any occurred
    return [result for result in results if not isinstance(result, Exception)]

def _fetch_js_variables(url: str) -> Dict[str, Any]:
    """
    Fetch the JavaScript variables 'ClosingPriceData' and 'BestLimitData' from the given URL using urllib3.

    :param url: The URL to fetch data from.
    :return: A dictionary containing the 'ClosingPriceData' and 'BestLimitData'.
    """
    retries = 0
    while True:
        try:
            # Make the HTTP request
            response = http.request('GET', url, headers=headers, timeout=5)

            if response.status >= 400:
                raise urllib3.exceptions.HTTPError(f"HTTP Error: {response.status}")

            html_content = response.data.decode('utf-8')

            # Parse the HTML using BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find the script tag that contains the JavaScript data
            script_tags = soup.find_all('script')

            # Initialize variables to store extracted data
            closing_price_data = None
            best_limit_data = None

            # Regular expressions to match the desired JavaScript variables
            closing_price_pattern = re.compile(r"var ClosingPriceData=\[(.*?)\];", re.DOTALL)
            best_limit_pattern = re.compile(r"var BestLimitData=\[(.*?)\];", re.DOTALL)

            # Iterate over script tags to find the JavaScript variables
            for script in script_tags:
                script_content = script.string
                if script_content:
                    # Extract 'ClosingPriceData'
                    closing_price_match = re.search(closing_price_pattern, script_content)
                    if closing_price_match:
                        closing_price_data = eval(f"[{closing_price_match.group(1)}]")

                    # Extract 'BestLimitData'
                    best_limit_match = re.search(best_limit_pattern, script_content)
                    if best_limit_match:
                        best_limit_data = eval(f"[{best_limit_match.group(1)}]")

            # If both variables are successfully extracted, return them
            if closing_price_data or best_limit_data:
                return {
                    "ClosingPriceData": closing_price_data,
                    "BestLimitData": best_limit_data
                }

        except Exception as e:
            retries += 1
            print(f"Error fetching {url}: {str(e)}, retrying... (attempt {retries})")
            sleep_time = 1 + random.uniform(0, 1)
            print(f"Sleeping for {sleep_time} seconds before retrying...")
            time.sleep(sleep_time)

async def fetch_js_variables(url: str) -> Dict[str, Any]:
    """
    Asynchronously fetch the JavaScript variables 'ClosingPriceData' and 'BestLimitData' from the given URL.
    This function runs in an infinite loop until it successfully retrieves data.

    :param url: The URL to fetch data from.
    :return: A dictionary containing the 'ClosingPriceData' and 'BestLimitData'.
    """
    return await asyncio.to_thread(_fetch_js_variables, url)

async def fetch_multiple_js_links(urls: list):
    """
    Fetch data from multiple URLs and return two dataframes (ClosingPrice and OrderBook).

    :param urls: A list of full URLs to fetch data for.
    :return: Two pandas DataFrames for ClosingPriceData and BestLimitData, including the insCode extracted from the URL.
    """
    tasks = [fetch_js_variables(url) for url in urls]

    # Use tqdm_asyncio to show progress
    results = await tqdm_asyncio.gather(*tasks, desc="Fetching data from URLs", total=len(urls))
    closing_price_list = []
    best_limit_list = []

    # Parse each result
    for url, result in zip(urls, results):
        ins_code = url.split("i=")[-1]  # Extract the insCode from the URL
        closing_price_data = result.get("ClosingPriceData")
        best_limit_data = result.get("BestLimitData")

        # Add insCode column to the data
        if closing_price_data:
            for entry in closing_price_data:
                closing_price_list.append([ins_code] + entry)

        if best_limit_data:
            for entry in best_limit_data:
                best_limit_list.append([ins_code] + entry)

    # Create DataFrames
    closing_price_df = pd.DataFrame(
        closing_price_list,
        columns=["insCode", "time_id", "Final", "close", "count", "SumOfVolume", "SumOfMarketCap", "yesterday_final", "dEven", "day_LL", "day_UL"]
    )

    best_limit_df = pd.DataFrame(
        best_limit_list,
        columns=["insCode", "time_id", "Depth", "Buy_No", "Buy_Vol", "Buy_Price", "Sell_Price", "Sell_Vol", "Sell_No"]
    )

    return closing_price_df, best_limit_df
async def crawl_data_simple_api(urls: List[str]) -> List:
    """Crawl a list of URLs and return the gathered data asynchronously."""
    tasks = [fetch(url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # Filter out exceptions if any occurred
    return [result for result in results if not isinstance(result, Exception)]


async def time_series_to_df(urls: list, response_key: str) -> pd.DataFrame:
    """
    Crawl the data and return it as a single DataFrame, handling both list and single dictionary results.
    If unique_keys are provided, they will be added as a new column to the corresponding rows.

    :param urls: List of URLs to crawl.
    :param response_key: Key in the response to extract the relevant data.
    :return: DataFrame containing all crawled data.
    """
    tasks = [fetch(url) for url in urls]
    all_data = []  # To accumulate data from all URLs

    # Use tqdm_asyncio to show progress
    for idx, coro in enumerate(tqdm_asyncio.as_completed(tasks, total=len(urls), desc="Fetching data")):
        try:
            data = await coro
            if response_key in data:
                data_response = data[response_key]
                dEven_value = data.get('dEven', None)  # Get dEven if available, otherwise None
                url_value = data.get('url', None)  # Get URL if available

                if isinstance(data_response, dict):
                    if dEven_value is not None:
                        data_response['dEven'] = dEven_value
                    if url_value is not None:
                        data_response['url'] = url_value
                    all_data.append(data_response)
                elif isinstance(data_response, list):
                    for entry in data_response:
                        if dEven_value is not None:
                            entry['dEven'] = dEven_value
                        if url_value is not None:
                            entry['url'] = url_value
                    all_data.extend(data_response)
        except KeyError as e:
            all_data.extend(data_response)
        except Exception as e:
            print(f"Error fetching data: {e}")

    # Convert the accumulated data into a single DataFrame
    df = pd.DataFrame(all_data)
    return df