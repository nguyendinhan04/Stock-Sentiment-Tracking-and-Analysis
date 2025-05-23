import requests
import os 
from dotenv import load_dotenv
import pprint
from minio import Minio
from json import dumps
from io import BytesIO,StringIO
from datetime import datetime,timedelta,timezone
import json



def import_news():
    load_dotenv(dotenv_path= "/opt/airflow/dags/.env")
    alpha_vantage_url = "https://www.alphavantage.co/query"



    MinioClient = Minio("minio:9000",access_key=os.getenv("MINIO_ACCESS_KEY"), secret_key=os.getenv("MINIO_SECRET_KEY"),secure=False)


    et_tz = timezone(offset=timedelta(days=0),name = 'America/New_York')
    et_time = datetime.now(et_tz) - timedelta(days=2)
    #xong 5
    # print(os.getenv("ALPHA_VANTAGE_API_KEY"))
    print(et_time)

    news_queyry_params = {
    "function" : "NEWS_SENTIMENT",
    "apikey" : os.getenv("ALPHA_VANTAGE_API_KEY"),
    "limit" : 1000,
    "time_from" : f"{et_time.strftime('%Y%m%d')}T0000",
    "time_to" : f"{et_time.strftime('%Y%m%d')}T2359",
    }


    response = requests.get(alpha_vantage_url, params=news_queyry_params)
    json_data = response.json()
    # print(json_data)
    data = json_data["feed"]

    cnt = 0
    input_data = ""
    for item in data:
        item = dict(item)
        item["in_date_id"] = cnt
        input_data = input_data + dumps(item) + "\n"
        cnt += 1

    stream = BytesIO(input_data.encode('utf-8'))
    stream.seek(0)

    MinioClient.put_object(
        bucket_name="news-data",
        object_name=f"news_{et_time.strftime('%Y-%m-%d')}.json",
        data=stream,
        length=len(input_data)
    )



