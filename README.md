Stock price prediction analysis using airflow and snowflake:

Project Overview:
This project explains how a stock price forecasting analytics system was built with Snowflake and Airflow. Historical stock data for Apple (AAPL) and Tesla (TSLA) are retrieved with the yfinance API, processed with Airflow pipelines, and stored in Snowflake. An additional machine learning (ML) forecast pipeline is introduced to predict future stock prices. Both historical and forecast data are joined into a single unified table in Snowflake.

Technologies Used:
1. Python: Data processing and ML model implementation
2. Snowflake: Cloud-based data storage and warehousing
3. Apache Airflow: Workflow automation and task scheduling
4. yfinance API: Data extraction for stock prices
5. SQL: Data manipulation and storage operations
6. ARIMA Model: Time-series forecasting for stock price prediction

Project Workflow:
1. Data Extraction: Historical stock data is fetched from yfinance API for AAPL and TSLA (last 180 days). API credentials are securely stored in Airflow Variables.
2. Data Storage: Extracted data is stored in the Snowflake STOCK_PRICE_PRED table. Forecasted data is stored in the MARKET_DATA_FORECAST table.
3. Data Processing & ML Forecasting: ARIMA model trains on historical stock data to predict stock prices for the next 7 business days. Predictions are stored in Snowflake.
4. Automated Data Pipeline (Managed by Airflow DAGs): DAG for ETL pipeline (data extraction, transformation, and loading into Snowflake). DAG for ML pipeline (training ARIMA and predicting future stock prices

Installation & Setup:
1. Python 3.8+
2. Snowflake Account
3. Apache Airflow Setup
4. API Key

Configure Airflow to use your snowflake connection and Alpha Vantage API key by adding them as variables and connections.
Add the following variables in Airflow:
1. snowflake_username
2. snowflake_password
3. snowflake_account
4. api_key (Alpha Vantage API key)
5. Snowflake Setup

Create the required database and schemas in your snowflake:
1. Database: DEV
2. Schemas: Raw, Adhoc, Analytics
Run the dags:
1. stock_price_update (stock_price.py)
2. TrainPredict (predict_stock_price.py)

Results & Findings:
1. Daily incremental stock data updates ensure no duplicate entries.
2. ARIMA Model predicts stock prices for the next 7 days with reliable accuracy.
3. Airflow DAGs efficiently automate data processing and prediction tasks.
4. Snowflake ensures scalable data storage and SQL-based data retrieval.
   
Future Enhancements:
1. Implement LSTM models for improved accuracy.
2. Add real-time stock price streaming using WebSocket APIs.
3. Optimize feature selection and hyperparameter tuning for better ML performance.
4. Explore integration with visualization tools like Power BI or Tableau.

License:
This project is licensed under the MIT License.
