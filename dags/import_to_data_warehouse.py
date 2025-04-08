from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import os
from dotenv import load_dotenv
# import sys
# sys.path.append(r"/opt/airflow/PolygonAPI")
# sys.path.append(r"/opt/airflow/Finnhub")
# sys.path.append(r"/opt/airflow/AlphaVantage")


from PolygonAPI.PolygonBoundary import loadCurrentOHCL

def getEnv():
    load_dotenv()
    print(os.getenv("POLYGON_API"))

with DAG('import_to_data_warehouse', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    import_from_polygon = PythonOperator(
        task_id = "import_OHCL_from_polygon",
        python_callable = loadCurrentOHCL
    )



    import_from_polygon