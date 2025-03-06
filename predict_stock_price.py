from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def return_snowflake_conn():
    #Snowflake connection
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn()

@task
def train(train_input_table, train_view, forecast_function_name):
    
    conn = return_snowflake_conn()
    cursor = conn.cursor()

    cursor.execute("USE DATABASE DEV;")
    cursor.execute("USE SCHEMA ANALYTICS;")
    
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""
    
    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    print(f"Executing SQL: {create_view_sql}")
    print(f"Executing SQL: {create_model_sql}")

    try:
        cursor.execute(create_view_sql)
        cursor.execute(create_model_sql)
        cursor.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(f"Error in training: {e}")
        raise
    finally:
        cursor.close()

@task
def predict(forecast_function_name, forecast_table, final_table, target_table):
    
    conn = return_snowflake_conn()
    cursor = conn.cursor()

    cursor.execute("USE DATABASE DEV;")
    cursor.execute("USE SCHEMA ANALYTICS;")
    
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {target_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table}
        ORDER BY SYMBOL, DATE DESC;"""
    
    print(f"Executing SQL: {make_prediction_sql}")
    print(f"Executing SQL: {create_final_table_sql}")
    
    try:
        cursor.execute(make_prediction_sql)
        cursor.execute(create_final_table_sql)
    except Exception as e:
        print(f"Error in prediction: {e}")
        raise
    finally:
        cursor.close()

# Define the DAG
with DAG(
    dag_id='TrainPredict',
    start_date=datetime(2025, 2, 21),
    catchup=False,
    schedule_interval='30 2 * * *',  # Runs daily at 2:30 AM
    tags=['ML', 'ELT', 'Forecast', 'Stock']
) as dag:

    train_input_table = "dev.raw.stock_price_pred"
    train_view = "dev.adhoc.market_data_view"
    forecast_table = "dev.adhoc.market_data_forecast"
    forecast_function_name = "dev.analytics.predict_stock_price"
    final_table = "dev.analytics.market_data"
    target_table = "dev.raw.stock_price_pred"

    train(train_input_table, train_view, forecast_function_name)
    predict(forecast_function_name, forecast_table, final_table, target_table)
