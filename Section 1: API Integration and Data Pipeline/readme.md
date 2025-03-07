# **API Integration and Data Pipeline**  

## **Pipeline Overview**  

The pipeline follows an **ETL** process:  

1. **Extraction**:  
   - Fetches stock prices from **Polygon.io** and exchange rates from **Frankfurter API**  
   - Modular design with error handling and logging  

2. **Transformation**:  
   - Combines stock and currency data for seamless conversion  
   - Normalizes data and prepares it for analysis  

3. **Loading**:  
   - Uses a **star schema** for efficient querying  
   - Includes fact tables for stock prices and exchange rates, plus views for converted stock prices  

## **Data Model**  

- **Dimensions**:  
  - `dim_stock`: Stock details (ticker, company name)  
  - `dim_currency`: Currency codes and names  

- **Facts**:  
  - `fact_stock_price`: Daily stock metrics (open, high, low, close, volume)  
  - `fact_exchange_rate`: Exchange rates between currencies  

- **Views**:  
  - `vw_stock_price_any_currency`: Provides stock prices in any currency  

## **Stock Analysis with Currency Conversion**  

- Merges stock prices with exchange rates by date  
- Converts prices dynamically based on selected currency  
- Supports historical analysis and multi-currency comparisons  

## **Configuring the Pipeline**  

- **Stocks**: Modify `tickers` in `get_stock_data()`  
- **Currencies**: Change `base_currency` in `get_currency_data()`  
- **Date Range**: Adjust `days` parameter for historical data  
- **API Credentials**: Update the API key for production  

## **Handling Failures & Scaling**  

- **Error Handling**: API retries, logging, and NULL safeguards  
- **Rate Limits**: Introduces delays between API calls  
- **Monitoring**: Logs integrated with **CloudWatch** or **ELK**  
- **Scaling**: Docker-based deployment, Airflow scheduling, and database partitioning  
- **Backup & Recovery**: Automated backups and audit logs  

