from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from dotenv import load_dotenv


from AlphaVantage.AlphaVantage_boundary import import_news
from transform.transform_news import load_news



with DAG('import_news',start_date=datetime(2025,1,1),schedule_interval = '0 15 * * *',catchup=False) as dag: 
    # import_news = PythonOperator(
    #     task_id = "import_news",
    #     python_callable = import_news
    # )
    # import_news

    load_news = PythonOperator(
        task_id = "load_news",
        python_callable = load_news
    )

    load_news

    # import_news >> load_news
