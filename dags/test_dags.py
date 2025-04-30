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
    exec_time__str = (kwargs["ts"])
    print(exec_time__str)



def test(**kwargs):
    execution_date = kwargs['ds']
    print(type(datetime.strptime(execution_date, r"%Y-%m-%d")))
    print(f"Execution date: {datetime.strptime(execution_date, r'%Y-%m-%d')}")

    
with DAG('test_dag',start_date=datetime(2025,1,1),schedule_interval = None,catchup=False) as dag: 
    start = PythonOperator(
        task_id= 'Start',
        python_callable = test_load_to_ch
    )

    
