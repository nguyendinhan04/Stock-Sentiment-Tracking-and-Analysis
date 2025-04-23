from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from datetime import datetime
import os
from dotenv import load_dotenv
from Minio.Minio_boundary import get_file_size

def test_load_to_ch(**kwargs):
    execution_date = kwargs["execution_date"]
    time_now = (execution_date).date()
    print(time_now)
    pass



def test(**kwargs):
    execution_date = kwargs['ds']
    print(type(datetime.strptime(execution_date, r"%Y-%m-%d")))
    print(f"Execution date: {datetime.strptime(execution_date, r'%Y-%m-%d')}")
with DAG('test_dag',start_date=datetime(2025,1,1),schedule_interval = '@daily',catchup=False) as dag: 
    start = PythonOperator(
        task_id= 'Start',
        python_callable = get_file_size
    )

    
