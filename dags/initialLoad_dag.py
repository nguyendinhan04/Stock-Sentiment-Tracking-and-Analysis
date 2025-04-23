from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from Clickhouse.Clickhouse_boundary import init_table
from initialLoad.initialLoad_implement import load_DIM_all
from initialLoad.initialLoad_implement import load_stock_symbol_DIM_all


with DAG('initial_load',start_date=datetime(2025,1,1),schedule_interval = None,catchup=False) as dag: 
    create_table = PythonOperator(
        task_id = "create_table",
        python_callable = init_table
    )
    # create_table

    # load industry, country, sector
    load_DIM = PythonOperator(
        task_id = "load_DIM",
        python_callable = load_DIM_all
    )
    # load_DIM

    load_stock_symbol_DIM  = PythonOperator(
        task_id = "load_stock_symbol_DIM",
        python_callable = load_stock_symbol_DIM_all
    )
    # load_stock_symbol_DIM

    create_table >> load_DIM >> load_stock_symbol_DIM
