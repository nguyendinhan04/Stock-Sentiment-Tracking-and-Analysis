import os
from dotenv import load_dotenv
from polygon.rest import RESTClient
from datetime import datetime,timedelta,timezone
from minio import Minio
import io
from json import dumps


def loadCurrentOHCL(**kwargs):
    load_dotenv(dotenv_path= "/opt/airflow/dags/.env")
    # print("API key: " + os.getenv("POLYGON_API"))
    PolygonClient = RESTClient(os.getenv("POLYGON_API"))
    MinioClient = Minio("minio:9000",access_key="QEtufdfRqutsA3pK1C3z", secret_key="vI4BpdPXNyXTmG3rIlLSHXwfazJ4nBgWAN3Jn8E9",secure=False)
    et_tz = timezone(offset=timedelta(days=0),name = 'America/New_York')
    et_time = datetime.now(et_tz) - timedelta(days=1)
    # da xong 5
    print(et_time.strftime('%Y-%m-%d'))
    data = PolygonClient.get_grouped_daily_aggs(et_time.strftime('%Y-%m-%d'),adjusted="true")

    data_json = ""
    for item in data:
        json_data = {"ticker" : item.ticker,"o": item.open, "c": item.close, "h": item.high, "l": item.low, "v": item.volume, "vwap" : item.vwap, "time": item.timestamp, "trans": item.transactions}

        data_json = data_json + (dumps(json_data)) + "\n"


    data_byte = data_json.encode("utf-8")
    stream = io.BytesIO(data_byte)
    stream.seek(0)


    MinioClient.put_object(
        bucket_name= "polygon",
        object_name=f"OHCL_{et_time.strftime('%Y-%m-%d')}.json",
        data=stream,
        length=len(data_json),
    )


def getLastestOHCL(date_string):
    MinioClient = client = Minio("minio:9000",access_key="QEtufdfRqutsA3pK1C3z", secret_key="vI4BpdPXNyXTmG3rIlLSHXwfazJ4nBgWAN3Jn8E9",secure=False)


    lastest_OHCL = MinioClient.get_object(
        bucket_name="polygon",
        object_name=f"OHCL_{date_string}.json"
    )

    return lastest_OHCL
    


    


    
