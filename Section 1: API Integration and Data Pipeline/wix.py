import requests
import pandas as pd
import datetime
import logging
import os
import time
import threading
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("stock_pipeline")


class RateLimiter:
    """Handles rate limiting for API requests."""

    def __init__(self, requests_per_minute=5, burst=10):
        """
        Initialize the rate limiter.

        Args:
            requests_per_minute: Number of requests allowed per minute
            burst: Number of requests allowed in burst before throttling
        """
        self.requests_per_minute = requests_per_minute
        self.burst = burst
        self.request_times = []
        self.lock = threading.Lock()

    @staticmethod
    def calculate_backoff_time(base_wait_time, retry_count, max_wait=60):
        """
        Calculate exponential backoff time.

        Returns:
            Wait time in seconds
        """
        # Exponential backoff formula with jitter
        wait_time = min(max_wait, base_wait_time * (2 ** retry_count))
        # Add small random jitter (±10%)
        jitter = wait_time * 0.1 * (random.random() * 2 - 1)
        return max(0.1, wait_time + jitter)

    def wait_if_needed(self):
        """
        Wait if rate limit is reached.
        Returns the number of seconds waited, if any.
        """
        with self.lock:
            now = time.time()

            # Remove request timestamps older than 1 minute
            self.request_times = [t for t in self.request_times if now - t < 60]

            # If we have fewer requests than burst limit, don't wait
            if len(self.request_times) < self.burst:
                self.request_times.append(now)
                return 0

            # If we're under the per-minute limit, don't wait
            if len(self.request_times) < self.requests_per_minute:
                self.request_times.append(now)
                return 0

            # Calculate how long to wait
            oldest_request = self.request_times[0]
            wait_time = max(0, 60 - (now - oldest_request))

            # If wait time is very small, just proceed
            if wait_time < 0.1:
                self.request_times.append(now)
                return 0

            # Wait and then add new timestamp
            time.sleep(wait_time)
            self.request_times.append(time.time())
            return wait_time


class PolygonStockAPI:
    """Handles interactions with the Polygon.io Stock API."""

    def __init__(self, api_key=None, default_tickers=None, requests_per_min=5, burst=10):
        """
        Initialize the API client with optional API key and default tickers.

        Args:
            api_key: API key for Polygon.io
            default_tickers: Default list of tickers to use if none provided
            requests_per_min: Maximum number of requests allowed per minute
            burst: Number of requests allowed in burst before throttling


        Note:
            - The default API key is a public demo key with limited access.
            - To use authenticated (premium) endpoints, replace `self.api_key` with your personal API key.
        """
        self.base_url = "https://api.polygon.io"
        self.api_key = api_key or "tSOYqa6iCiDkIRpIOQ1XnurQu2VbeaoQ"  # Sample key for demo
        self.default_tickers = default_tickers or ["WIX", "GOOGL"]
        self.rate_limiter = RateLimiter(requests_per_minute=requests_per_min, burst=burst)

    @staticmethod
    def _process_ticker_data(ticker, date, data, is_prev_close=False):
        """
        Process ticker data into a standard format regardless of source.

        Returns:
            Dictionary with normalized ticker data
        """
        date_str = date.strftime("%Y-%m-%d") if isinstance(date, datetime.datetime) else date

        if is_prev_close:
            # Process data from previous close endpoint (uses abbreviations)
            return {
                "ticker": ticker,
                "date": date_str,
                "open": data.get("o", 0),
                "high": data.get("h", 0),
                "low": data.get("l", 0),
                "close": data.get("c", 0),
                "volume": data.get("v", 0)
            }


    def _make_request(self, url, params=None, retry_count=0, max_retries=3):
        """
        Make a rate-limited API request.

        Returns:
            Response JSON or None if error

        Note:
            - To access premium (real-time, historical) stock data, switch to authenticated endpoints.
            - Example: Replace `v2/aggs/ticker/{ticker}/prev` with `v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}`
        """
        # Wait if needed for rate limiting
        wait_time = self.rate_limiter.wait_if_needed()
        if wait_time > 0:
            logger.debug(f"Rate limit reached, waited {wait_time:.2f} seconds")

        try:
            response = requests.get(url, params=params)
            if response.status_code == 429 and retry_count < max_retries:  # Too Many Requests
                # Calculate wait time using static method
                backoff_time = RateLimiter.calculate_backoff_time(5, retry_count)
                logger.warning(
                    f"Rate limit exceeded, waiting {backoff_time:.2f} seconds before retry {retry_count + 1}/{max_retries}")
                time.sleep(backoff_time)

                # Retry request with incremented retry count
                return self._make_request(url, params, retry_count + 1, max_retries)

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error making request to {url}: {e}")
            return None
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error making request to {url}: {e}")
            return None
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"JSON decode error in response from {url}: {e}")
            return None

    def get_market_status(self):
        """Get current market status."""
        url = f"{self.base_url}/v1/marketstatus/now"
        params = {"apiKey": self.api_key}
        return self._make_request(url, params)

    def get_ticker_details(self, ticker):
        """Get details for a specific ticker."""
        url = f"{self.base_url}/v3/reference/tickers/{ticker}"
        params = {"apiKey": self.api_key}
        return self._make_request(url, params) or {}

    def get_previous_close(self, ticker):
        """Get the previous day's close data for a ticker."""
        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/prev"
        params = {"apiKey": self.api_key}
        result = self._make_request(url, params)
        return result or {"results": []}


    def get_stock_data_for_ticker(self, ticker, days=7):
        """
        Get stock data for a single ticker.

        Returns:
            List of daily data for the ticker
        """
        end_date = datetime.datetime.now() - datetime.timedelta(days=1)
        ticker_data = []

        # Get ticker details for additional information
        logger.info(f"Retrieved ticker info for {ticker}")

        # Get recent data from previous close endpoint
        prev_close = self.get_previous_close(ticker)
        if prev_close and "results" in prev_close and prev_close["results"]:
            latest_data = prev_close["results"][0]
            yesterday = end_date - datetime.timedelta(days=1)

            # Format the previous close data using helper method
            ticker_data.append(
                self._process_ticker_data(ticker, yesterday, latest_data, is_prev_close=True)
            )
            logger.info(f"Retrieved previous close data for {ticker}")

        return ticker_data

    def get_stock_data(self, tickers=None, days=6, use_threading=True, max_workers=10):
        """
        Get real stock data for the specified tickers using sample endpoints.

        Returns:
            Dictionary with ticker symbols as keys and lists of daily data as values
        """
        if tickers is None:
            tickers = self.default_tickers

        market_status = self.get_market_status()
        logger.info(f"Market status: {market_status}")

        all_data = {}

        if use_threading and len(tickers) > 1:
            import concurrent.futures

            # Use ThreadPoolExecutor for multi-threading
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(tickers))) as executor:
                # Submit tasks for each ticker
                future_to_ticker = {
                    executor.submit(self.get_stock_data_for_ticker, ticker, days): ticker
                    for ticker in tickers
                }

                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        ticker_data = future.result()
                        all_data[ticker] = ticker_data
                    except Exception as e:
                        logger.error(f"Error processing ticker {ticker}: {e}")
                        all_data[ticker] = []
        else:
            # Sequential processing
            for ticker in tickers:
                try:
                    all_data[ticker] = self.get_stock_data_for_ticker(ticker, days)
                except Exception as e:
                    logger.error(f"Error processing ticker {ticker}: {e}")
                    all_data[ticker] = []

        return all_data


class FrankfurterCurrencyAPI:
    """Handles interactions with the Frankfurter Currency API."""

    def __init__(self):
        """Initialize the API client."""
        self.base_url = "https://api.frankfurter.dev"

    def get_latest_rates(self, base_currency="USD"):
        """Get latest exchange rates."""
        url = f"{self.base_url}/v1/latest"
        params = {"from": base_currency}

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching exchange rates: {e}")
            return {}
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching exchange rates: {e}")
            return {}
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"JSON decode error in exchange rates response: {e}")
            return {}

    def get_historical_rates(self, date, base_currency="USD"):
        """Get historical exchange rates for a specific date."""
        url = f"{self.base_url}/v1/{date}"
        params = {"from": base_currency}

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching historical exchange rates: {e}")
            return {}
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching historical exchange rates: {e}")
            return {}
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"JSON decode error in historical exchange rates response: {e}")
            return {}

    def get_currency_data(self, days=7, base_currency="USD"):
        """Get currency data for the past days."""
        end_date = datetime.datetime.now() - datetime.timedelta(days=1)

        all_data = []
        for i in range(days):
            date = end_date - datetime.timedelta(days=i)
            date_str = date.strftime("%Y-%m-%d")

            rates = self.get_historical_rates(date_str, base_currency)
            if "rates" in rates:
                for currency, rate in rates["rates"].items():
                    all_data.append({
                        "date": date_str,
                        "from_currency": base_currency,
                        "to_currency": currency,
                        "rate": rate
                    })

        return all_data


class DataTransformer:
    """Transforms and combines data."""

    def combine_data(self, stock_data, currency_data):
        """
        Combine stock and currency data to enable analysis in different currencies.

        Returns:
            Combined DataFrame with stock prices in different currencies
        """
        # Convert stock data to DataFrame more efficiently using concat
        dataframes = [pd.DataFrame(data) for ticker, data in stock_data.items() if data]
        stock_df = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

        currency_df = pd.DataFrame(currency_data)

        # Ensure date columns are in the same format
        stock_df["date"] = pd.to_datetime(stock_df["date"])
        currency_df["date"] = pd.to_datetime(currency_df["date"])

        # Merge data on date
        result = pd.merge(
            stock_df,
            currency_df,
            on="date",
            how="inner"
        )

        # Calculate prices in different currencies
        result["open_converted"] = result["open"] * result["rate"]
        result["high_converted"] = result["high"] * result["rate"]
        result["low_converted"] = result["low"] * result["rate"]
        result["close_converted"] = result["close"] * result["rate"]

        return result

def run_pipeline(tickers=None, use_threading=True, rate_limit=5):
    """
    Run the complete data pipeline.

    Returns:
        Dict containing the extracted data, transformed data, and SQL schema
    """
    # Extract data from APIs
    logger.info("Extracting data from APIs...")

    # Get stock data
    stock_api = PolygonStockAPI(
        api_key=None,
        default_tickers=tickers,
        requests_per_min=rate_limit
    )
    stock_data = stock_api.get_stock_data(use_threading=use_threading)

    # Get currency data
    currency_api = FrankfurterCurrencyAPI()
    currency_data = currency_api.get_currency_data()

    logger.info(f"Extracted data for {len(stock_data)} stocks and {len(currency_data)} currency rates")

    # Transform and combine data
    logger.info("Transforming and combining data...")
    transformer = DataTransformer()
    combined_data = transformer.combine_data(stock_data, currency_data)

    logger.info(f"Combined data has {len(combined_data)} records")



    # In a real implementation, we would:
    # 1. Execute the SQL statements to create the schema
    # 2. Transform the combined data into the appropriate format for loading
    # 3. Load the data into the database tables

    return {
        "extracted_data": {
            "stock_data": stock_data,
            "currency_data": currency_data
        },
        "transformed_data": combined_data,
    }


if __name__ == "__main__":
    import argparse

    # Add command-line arguments
    parser = argparse.ArgumentParser(description='Run the stock data pipeline')
    parser.add_argument('--tickers', type=str, nargs='+', help='List of ticker symbols to process')
    parser.add_argument('--no-threading', action='store_true', help='Disable multi-threading')
    parser.add_argument('--rate-limit', type=int, default=5, help='API rate limit (requests per minute)')
    args = parser.parse_args()

    # Run the pipeline with command-line arguments
    result = run_pipeline(
        tickers=args.tickers,
        use_threading=not args.no_threading,
        rate_limit=args.rate_limit
    )

    # Save output to files for inspection
    os.makedirs("output", exist_ok=True)

    # Save sample of transformed data on csv file
    if not result["transformed_data"].empty:
        result["transformed_data"].head(30).to_csv("output/sample_transformed_data.csv", index=False)

    logger.info("Pipeline completed successfully. Output saved to 'output' directory.")