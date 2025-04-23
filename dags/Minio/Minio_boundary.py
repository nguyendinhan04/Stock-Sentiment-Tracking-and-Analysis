from minio import Minio
from dotenv import load_dotenv
import os



def get_client():
    load_dotenv(dotenv_path= "/opt/airflow/dags/.env")
    minioClient = Minio("minio:9000",access_key=os.getenv("MINIO_ACCESS_KEY"), secret_key=os.getenv("MINIO_SECRET_KEY"),secure=False)
    return minioClient


def get_file_size(bucket_name = "polygon", object_name = "OHCL_2025-03-31.json"):
    minioClient = get_client()
    object_info  = minioClient.stat_object(
        bucket_name = bucket_name,
        object_name=object_name
    )
    print("SIZE cua file")
    print(object_info.size)

    



