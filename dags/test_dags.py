from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator



with DAG('test_dag',start_date=datetime(2025,1,1),schedule_interval = '@daily',catchup=False) as dag: 
    start = EmptyOperator(
        task_id= 'Start'
    )

    
