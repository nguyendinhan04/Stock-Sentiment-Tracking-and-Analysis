import os
from dotenv import load_dotenv
from polygon import RESTClient
from datetime import datetime,timedelta
from pytz import timezone
from minio import Minio
import io
from json import dumps


def loadCurrentOHCL():
    load_dotenv(dotenv_path= "/opt/airflow/dags/.env")
    print("API key: " + os.getenv("POLYGON_API"))
    PolygonClient = RESTClient(os.getenv("POLYGON_API"))
    MinioClient = client = Minio("minio:9000",access_key="QEtufdfRqutsA3pK1C3z", secret_key="vI4BpdPXNyXTmG3rIlLSHXwfazJ4nBgWAN3Jn8E9",secure=False)
    et_tz = timezone('America/New_York')
    et_time = datetime.now(et_tz) - timedelta(days=1)
    print(et_time.strftime('%Y-%m-%d'))
    data = PolygonClient.get_grouped_daily_aggs(et_time.strftime('%Y-%m-%d'))

    data_json = ""
    for item in data:
        json_data = {"ticker" : item.ticker,"o": item.open, "c": item.close, "h": item.high, "l": item.low, "v": item.volume, "vwap" : item.vwap, "time": item.timestamp, "trans": item.transactions}

        data_json = data_json + (dumps(json_data, indent=2)) + "\n"

    data_byte = data_json.encode("utf-8")
    stream = io.BytesIO(data_byte)


    MinioClient.put_object(
        bucket_name= "polygon",
        object_name=f"OHCL_{et_time.strftime('%Y-%m-%d')}.json",
        data=stream,
        length=len(data),
    )

    


    


    
