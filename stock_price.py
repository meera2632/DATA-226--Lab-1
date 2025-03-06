from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import pandas as pd
import yfinance as yf

def return_snowflake_conn():
    """Establish Snowflake Connection."""
    return SnowflakeHook(snowflake_conn_id='snowflake_conn').get_cursor()

@task
def extract(stocks):
    """Fetch stock price data from Yahoo Finance."""
    def fetch_data(symbol):
        try:
            stock = yf.Ticker(symbol)
            df = stock.history(period="180d")
            if df.empty:
                print(f"Warning: No data found for {symbol}")
                return pd.DataFrame()
            df['Symbol'] = symbol
            df.reset_index(inplace=True)
            return df[['Symbol', 'Date', 'Open', 'Close', 'Low', 'High', 'Volume']]
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            return pd.DataFrame()
    
    data = pd.concat([fetch_data(stock) for stock in stocks if not fetch_data(stock).empty])
    data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')  # Convert date format
    return data.to_dict(orient='records')

@task
def transform(data):
    """Transform data into the required format."""
    return pd.DataFrame(data).to_dict(orient='records')

@task
def load(records, target_table):
    """Load stock data into Snowflake."""
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                SYMBOL STRING,
                DATE DATE,
                OPEN FLOAT,
                CLOSE FLOAT,
                LOW FLOAT,
                HIGH FLOAT,
                VOLUME FLOAT,
                CONSTRAINT PK_STOCK UNIQUE (SYMBOL, DATE)
            )
        """)
        cur.execute(f"DELETE FROM {target_table}")  # Optional: Clears previous data

        insert_sql = f"""
            INSERT INTO {target_table} (SYMBOL, DATE, OPEN, CLOSE, LOW, HIGH, VOLUME)
            VALUES (%(Symbol)s, %(Date)s, %(Open)s, %(Close)s, %(Low)s, %(High)s, %(Volume)s)
        """
        cur.executemany(insert_sql, records)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error: {e}")
        raise e

# Define the DAG
with DAG(
    dag_id='stock_price_update',
    start_date=datetime(2024, 3, 3),
    catchup=False,
    schedule_interval='0 0 * * *',  # Runs daily at midnight
    tags=['ETL', 'Stock']
) as dag:

    target_table = "RAW.STOCK_PRICE_PRED"
    stocks = ["TSLA", "AAPL"]

    raw_data = extract(stocks)
    transformed_data = transform(raw_data)
    load_data = load(transformed_data, target_table)

    raw_data >> transformed_data >> load_data
