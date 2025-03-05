import logging
from src.api.polygon_api import PolygonStockAPI
from src.api.frankfurter_api import FrankfurterCurrencyAPI
from src.transform.data_transformer import DataTransformer
from src.load.warehouse_loader import DataWarehouseLoader
from sql_loader import SQLLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("stock_pipeline")

def run_pipeline():
    """
    Run the complete data pipeline.
    
    Returns:
        Dict containing the pipeline results and stats
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
    
    # Load SQL files
    logger.info("Loading SQL definitions...")
    sql_loader = SQLLoader()
    dimension_sql = sql_loader.get_dimension_tables_sql()
    fact_sql = sql_loader.get_fact_tables_sql()
    index_sql = sql_loader.get_indexes_sql()
    view_sql = sql_loader.get_views_sql()
    
    # Initialize data warehouse
    logger.info("Creating database schema...")
    warehouse_loader = DataWarehouseLoader()
    warehouse_loader.execute_sql(dimension_sql)
    warehouse_loader.execute_sql(fact_sql)
    warehouse_loader.execute_sql(index_sql)
    warehouse_loader.execute_sql(view_sql)
    
    # Load data
    logger.info("Loading data into warehouse...")
    loading_stats = warehouse_loader.load_data(combined_data)
    
    logger.info(f"Pipeline completed successfully. Stats: {loading_stats}")
    
    return {
        "extraction": {
            "stock_count": len(stock_data),
            "currency_count": len(currency_data)
        },
        "transformation": {
            "record_count": len(combined_data)
        },
        "loading": loading_stats
    }

if __name__ == "__main__":
    # Run the pipeline
    result = run_pipeline()
    print(f"Pipeline completed with result: {result}")
