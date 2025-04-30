from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator,BranchPythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from dotenv import load_dotenv


from PolygonAPI.PolygonBoundary import loadCurrentOHCL
from transform.transformOHCL_pandas import transform_load_OHCL
from transform.aggregation_OHCL import aggregate_OHCL



# '0 3 * * *'
with DAG('import_to_data_warehouse', start_date=datetime(2025, 1, 1), schedule_interval='0 3 * * *', catchup=False) as dag:
    import_from_polygon = PythonOperator(
        task_id = "import_OHCL_from_polygon",
        python_callable = loadCurrentOHCL
    )
    # import_from_polygon



    load_OHCL = PythonOperator(
        task_id = "load_OHCL",
        python_callable = transform_load_OHCL
    )
    # load_OHCL

    aggregate_OHCL = PythonOperator(
        task_id = "aggregate_OHCL",
        python_callable = aggregate_OHCL
    )
    # aggregate_OHCL

    import_from_polygon >> load_OHCL >> aggregate_OHCL

