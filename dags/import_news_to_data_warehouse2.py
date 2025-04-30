from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from dotenv import load_dotenv


from AlphaVantage.AlphaVantage_boundary import import_news,import_news_hourly
from transform.transform_news import load_news,load_news_hourly


# '30 * * * *'
with DAG('import_news2_hourly',start_date=datetime(2025,1,1),schedule_interval = '30 * * * *',catchup=False,default_args={
        "retries": 3,  # thử lại 3 lần nếu lỗi
        "retry_delay": timedelta(minutes=5),  # mỗi lần cách nhau 5 phút
    },
) as dag: 
    import_news = PythonOperator(
        task_id = "import_news",
        python_callable = import_news_hourly
    )
    # import_news

    load_news = PythonOperator(
        task_id = "load_news",
        python_callable = load_news_hourly
    )

    # load_news

    import_news >> load_news