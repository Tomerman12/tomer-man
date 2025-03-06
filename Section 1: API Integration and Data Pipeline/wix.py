import requests
import pandas as pd
import datetime
import logging
import os
import time


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("stock_pipeline")


class PolygonStockAPI:
    """Handles interactions with the Polygon.io Stock API."""

    def __init__(self, api_key=None):
        """Initialize the API client with optional API key."""
        self.base_url = "https://api.polygon.io"
        self.api_key = api_key or "tSOYqa6iCiDkIRpIOQ1XnurQu2VbeaoQ"  # Sample key for demo

    def get_market_status(self):
        """Get current market status."""
        url = f"{self.base_url}/v1/marketstatus/now"
        params = {"apiKey": self.api_key}

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching market status: {e}")
            return {}

    def get_ticker_details(self, ticker):
        """Get details for a specific ticker."""
        url = f"{self.base_url}/v3/reference/tickers/{ticker}"
        params = {"apiKey": self.api_key}

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching ticker details for {ticker}: {e}")
            return {}

    def get_previous_close(self, ticker):
        """Get the previous day's close data for a ticker."""
        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/prev"
        params = {"apiKey": self.api_key}

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching previous close for {ticker}: {e}")
            return {"results": []}

    def get_daily_open_close(self, ticker, date):
        """Get the open, close, high, and low prices for a ticker on a certain date."""
        date_str = date.strftime("%Y-%m-%d")
        url = f"{self.base_url}/v1/open-close/{ticker}/{date_str}"
        params = {"apiKey": self.api_key, "adjusted": "true"}

        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                return response.json()
            # Handle case where market is closed on the requested date
            elif response.status_code == 404:
                logger.warning(f"No data available for {ticker} on {date_str} (market closed)")
                return None
            else:
                response.raise_for_status()
        except Exception as e:
            logger.error(f"Error fetching daily open-close for {ticker} on {date_str}: {e}")
            return None

    def get_stock_data(self, tickers=None, days=7):
        """
        Get real stock data for the specified tickers using Polygon.io's sample endpoints.

        Args:
            tickers: List of ticker symbols
            days: Number of days of historical data to retrieve

        Returns:
            Dictionary with ticker symbols as keys and lists of daily data as values
        """
        if tickers is None:
            tickers = ["AAPL", "MSFT", "GOOGL"]

        market_status = self.get_market_status()
        logger.info(f"Market status: {market_status}")

        end_date = datetime.datetime.now() - datetime.timedelta(days=1)

        all_data = {}

        for ticker in tickers:
            ticker_data = []

            # Get ticker details for additional information
            ticker_info = self.get_ticker_details(ticker)
            logger.info(f"Retrieved ticker info for {ticker}")

            # Get recent data from previous close endpoint
            prev_close = self.get_previous_close(ticker)
            if prev_close and "results" in prev_close and prev_close["results"]:
                latest_data = prev_close["results"][0]
                yesterday = end_date - datetime.timedelta(days=1)

                # Format the previous close data
                ticker_data.append({
                    "ticker": ticker,
                    "date": yesterday.strftime("%Y-%m-%d"),
                    "open": latest_data.get("o", 0),
                    "high": latest_data.get("h", 0),
                    "low": latest_data.get("l", 0),
                    "close": latest_data.get("c", 0),
                    "volume": latest_data.get("v", 0)
                })
                logger.info(f"Retrieved previous close data for {ticker}")

            # Get historical data using daily open-close endpoint
            for i in range(2, days + 1):  # Start from 2 since we already have yesterday's data
                date = end_date - datetime.timedelta(days=i)
                daily_data = self.get_daily_open_close(ticker, date)

                if daily_data and "symbol" in daily_data:
                    ticker_data.append({
                        "ticker": ticker,
                        "date": date.strftime("%Y-%m-%d"),
                        "open": daily_data.get("open", 0),
                        "high": daily_data.get("high", 0),
                        "low": daily_data.get("low", 0),
                        "close": daily_data.get("close", 0),
                        "volume": daily_data.get("volume", 0)
                    })
                    logger.info(f"Retrieved daily data for {ticker} on {date.strftime('%Y-%m-%d')}")

                # Add a small delay to avoid rate limiting
                time.sleep(0.5)

            all_data[ticker] = ticker_data

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
        except Exception as e:
            logger.error(f"Error fetching exchange rates: {e}")
            return {}

    def get_historical_rates(self, date, base_currency="USD"):
        """Get historical exchange rates for a specific date."""
        url = f"{self.base_url}/v1/{date}"
        params = {"from": base_currency}

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching historical exchange rates: {e}")
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

        Args:
            stock_data: Dictionary with ticker symbols as keys and lists of daily data as values
            currency_data: List of currency exchange rate records

        Returns:
            Combined DataFrame with stock prices in different currencies
        """
        # Convert stock data to DataFrame
        stock_records = []
        for ticker, data_list in stock_data.items():
            stock_records.extend(data_list)

        stock_df = pd.DataFrame(stock_records)
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


def create_database_schema():
    """
    Generate SQL statements for creating the data warehouse schema.

    Returns:
        Dict containing SQL statements for dimensions, facts, and views
    """
    # Dimension table SQL statements
    dim_tables = [
        """
        CREATE TABLE IF NOT EXISTS dim_stock (
            stock_id SERIAL PRIMARY KEY,
            ticker VARCHAR(10) NOT NULL,
            company_name VARCHAR(255),
            CONSTRAINT ticker_unique UNIQUE (ticker)
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS dim_currency (
            currency_id SERIAL PRIMARY KEY,
            currency_code VARCHAR(3) NOT NULL,
            currency_name VARCHAR(100),
            is_base_currency BOOLEAN DEFAULT FALSE,
            CONSTRAINT currency_code_unique UNIQUE (currency_code)
        );
        """
    ]

    # Fact table SQL statements
    fact_tables = [
        """
        CREATE TABLE IF NOT EXISTS fact_stock_price (
            price_id SERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            stock_id INTEGER NOT NULL REFERENCES dim_stock(stock_id),
            open DECIMAL(18, 6) NOT NULL,
            high DECIMAL(18, 6) NOT NULL,
            low DECIMAL(18, 6) NOT NULL,
            close DECIMAL(18, 6) NOT NULL,
            volume BIGINT NOT NULL,
            CONSTRAINT stock_date_unique UNIQUE (stock_id, trade_date)
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS fact_exchange_rate (
            rate_id SERIAL PRIMARY KEY,
            rate_date DATE NOT NULL,
            from_currency_id INTEGER NOT NULL REFERENCES dim_currency(currency_id),
            to_currency_id INTEGER NOT NULL REFERENCES dim_currency(currency_id),
            rate DECIMAL(18, 6) NOT NULL,
            CONSTRAINT exchange_rate_unique UNIQUE (rate_date, from_currency_id, to_currency_id)
        );
        """
    ]

    # View SQL statements for analysis
    views = [
        """
        CREATE OR REPLACE VIEW vw_stock_price_any_currency AS
        SELECT 
            sp.trade_date,
            s.ticker,
            fc.currency_code as base_currency,
            tc.currency_code as target_currency,
            sp.open,
            sp.high,
            sp.low,
            sp.close,
            sp.volume,
            CASE 
                WHEN fc.currency_code = tc.currency_code THEN sp.open
                ELSE sp.open * er.rate
            END as open_converted,
            CASE 
                WHEN fc.currency_code = tc.currency_code THEN sp.high
                ELSE sp.high * er.rate
            END as high_converted,
            CASE 
                WHEN fc.currency_code = tc.currency_code THEN sp.low
                ELSE sp.low * er.rate
            END as low_converted,
            CASE 
                WHEN fc.currency_code = tc.currency_code THEN sp.close
                ELSE sp.close * er.rate
            END as close_converted
        FROM 
            fact_stock_price sp
        JOIN 
            dim_stock s ON sp.stock_id = s.stock_id
        CROSS JOIN 
            dim_currency tc
        JOIN 
            dim_currency fc ON fc.is_base_currency = TRUE
        LEFT JOIN 
            fact_exchange_rate er 
            ON sp.trade_date = er.rate_date 
            AND fc.currency_id = er.from_currency_id 
            AND tc.currency_id = er.to_currency_id;
        """
    ]

    return {
        "dimensions": dim_tables,
        "facts": fact_tables,
        "views": views
    }


def run_pipeline():
    """
    Run the complete data pipeline.
    Returns:
        Dict containing the extracted data, transformed data, and SQL schema
    """
    # Extract data from APIs
    logger.info("Extracting data from APIs...")

    # Get stock data
    stock_api = PolygonStockAPI()
    stock_data = stock_api.get_stock_data()

    # Get currency data
    currency_api = FrankfurterCurrencyAPI()
    currency_data = currency_api.get_currency_data()

    logger.info(f"Extracted data for {len(stock_data)} stocks and {len(currency_data)} currency rates")

    # Transform and combine data
    logger.info("Transforming and combining data...")
    transformer = DataTransformer()
    combined_data = transformer.combine_data(stock_data, currency_data)

    logger.info(f"Combined data has {len(combined_data)} records")

    # Create database schema
    logger.info("Creating database schema...")
    schema = create_database_schema()

    # In a real implementation, we would:
    # 1. Execute the SQL statements to create the schema
    # 2. Transform the combined data into the appropriate format for loading
    # 3. Load the data into the database tables
    # 4. Create indexes for better query performance

    return {
        "extracted_data": {
            "stock_data": stock_data,
            "currency_data": currency_data
        },
        "transformed_data": combined_data,
        "database_schema": schema
    }


if __name__ == "__main__":
    # Run the pipeline
    result = run_pipeline()

    # Save output to files for inspection
    os.makedirs("output", exist_ok=True)

    # Save sample of transformed data on csv file
    if not result["transformed_data"].empty:
        result["transformed_data"].head(20).to_csv("output/sample_transformed_data.csv", index=False)

    # Save SQL schema
    with open("output/database_schema.sql", "w") as f:
        for category, statements in result["database_schema"].items():
            f.write(f"-- {category.upper()}\n")
            for statement in statements:
                f.write(f"{statement}\n\n")

    logger.info("Pipeline completed successfully. Output saved to 'output' directory.")