import pandas as pd
import os
import numpy as np
from dotenv import load_dotenv
from minio import Minio
from datetime import timezone,timedelta,datetime
from io import StringIO
from clickhouse_driver import Client
import io
from datetime import datetime
from Clickhouse.Clickhouse_boundary import get_table,getClickhouseClient,query_table



def transform_load_OHCL():
    load_dotenv(dotenv_path= "/opt/airflow/dags/.env")

    MinioClient = Minio("minio:9000",access_key=os.getenv("MINIO_ACCESS_KEY"), secret_key=os.getenv("MINIO_SECRET_KEY"),secure=False)
    
    # et_tz = timezone(offset=timedelta(days=0),name = 'America/New_York')
    # et_time = datetime.now(et_tz) - timedelta(days=1)
    lastest_obj = None
    obj_list = MinioClient.list_objects(bucket_name="polygon")
    for item in obj_list:
        if not lastest_obj or item.last_modified > lastest_obj.last_modified :
            lastest_obj = item
    if not lastest_obj:
        return
    et_time = datetime.strptime(lastest_obj.object_name[5:15], "%Y-%m-%d")

    loaded_time_df = query_table(f"select toDate(toInt64(left(t,10))) as converted_date from candles_FACT where converted_date = '{lastest_obj.object_name[5:15]}'")
    if len(loaded_time_df) >= 1:
        print("DA INSERT NGAY HOM NAY")
        return
    # print(lastest_obj.object_name[5:15])
    print(et_time.date())

    #doc du lieu api tra ve trong trong minio
    response = MinioClient.get_object(
        bucket_name = "polygon",
        object_name = lastest_obj.object_name
    )
    data = response.read().decode("utf-8")
    df = pd.read_json(StringIO(data),lines = True)
    if len(df) < 1:
        print("KHONG CO DU LIEU TRA VE")
        return
    df = df.astype({"ticker":"string", "time" : "string"})


    #doc du lieu trong clickhouse
    load_dotenv(dotenv_path= "/opt/airflow/dags/.env")
    ClickhouseClientSetting = {'use_numpy': True}
    ClickhouseClient = Client(
        host = "clickhosue",
        port= "9000",
        user=os.getenv("CLICKHOUSE_USERNAME"),
        password = os.getenv("CLICKHOUSE_PASSWORD"),
        database = "default",
        settings = ClickhouseClientSetting
    )
    data, columns  = ClickhouseClient.execute(query  ="select stock_symbol, stock_symbol_name from stock_symbol_DIM", with_column_types=True)
    ticker_df = pd.DataFrame([row for row in data],columns = [col[0] for col in columns])

    currtime_string = df.head(1)["time"][0]
    currtime = datetime.fromtimestamp(int(currtime_string)/1000).date()


    #join du lieu voi nhau
    join_df = pd.merge(df,ticker_df,how="right",left_on="ticker",right_on = "stock_symbol")
    # print("join_id")
    # print(join_df.info())
    #ticker chua co trong DW
    # new_ticker_df = join_df.loc[pd.isna(join_df["stock_symbol_name"]), ["ticker","o","c","h","v","l","time","trans"]]
    

    #luu xuong minio nhung record nao bi fail
    # json_new_ticker_df = new_ticker_df.to_json(orient="records",lines = True)
    # new_ticker_BytesIO = io.BytesIO(json_new_ticker_df.encode("utf-8"))
    # new_ticker_BytesIO.seek(0)
    # MinioClient.put_object(
    #     bucket_name="ohcl-insert-fail",
    #     object_name = f"OHCL_{et_time.strftime('%Y-%m-%d')}.json",
    #     data = new_ticker_BytesIO,
    #     length = len(new_ticker_BytesIO.getvalue())
    # )


    #insert xuong clickhouse nhung record nao pass
    
    #nhung ticket nao co ghi nhan giao dich trong ngay
    pass_ticker_df = join_df.loc[pd.notna(join_df["ticker"]),["stock_symbol","o","c","h","v","l","time","trans","vwap"]]
    #nhung ticket nao khong ghi nhan giao dich trong ngay
    not_include_df = join_df.loc[pd.isna(join_df["ticker"]),["stock_symbol","o","c","h","v","l","time","trans","vwap"]]
    # print("not_include_df 1 ")
    # print(not_include_df.info())    
    #lay du lien candles cua ngay gan nhat de fill vao nhwung ticket khong co record candles
    history_candles_df = get_table("candles_FACT")
    history_candles_df["time_convert"] = history_candles_df["t"].apply(lambda x : datetime.fromtimestamp(int(x)/1000).date())
    # history_candles_df = history_candles_df[datetime.fromtimestamp(int(history_candles_df["t"])) > (currtime - timedelta(days=7))]
    nearest_date = (history_candles_df["time_convert"].max())
    # print("nearst: " + str(nearest_date))

    nearest_candles = history_candles_df[history_candles_df["time_convert"] == nearest_date]
    join_not_include_df = pd.merge(not_include_df,nearest_candles,how = "left",left_on="stock_symbol", right_on="stock_symbol",suffixes=["_left","_right"])
    # print("join_not_include_df 1 ")
    # print(join_not_include_df.info())
    join_not_include_df = join_not_include_df[pd.notna(join_not_include_df["c_right"])]
    join_not_include_df = join_not_include_df[["stock_symbol","c_right","vwap_right"]]
    join_not_include_df["o"] = join_not_include_df["c_right"]
    join_not_include_df["h"] = join_not_include_df["c_right"]
    join_not_include_df["l"] = join_not_include_df["c_right"]
    join_not_include_df["v"] = 0
    join_not_include_df["trans"] = 0
    join_not_include_df["time"] = currtime_string
    

    join_not_include_df.rename(columns={
        "c_right" : "c",
        "vwap_right" : "vwap"
    },inplace=True)

    # print("pass")
    # print(pass_ticker_df.info())
    # print("join_not_include_df")
    # print(join_not_include_df.info())
    insert_candles = pd.concat([join_not_include_df,pass_ticker_df])
    # print("insert_candles")
    # print(insert_candles.info())


    

    # pass_ticker_df["stock_symbol"] = pass_ticker_df["stock_symbol"].astype(str)

    insert_candles = insert_candles.rename(columns={
        "time": "t",
        "trans": "Trans"
    })
    # print(pass_ticker_df)

    ClickhouseClient.insert_dataframe(query="insert into candles_FACT values", dataframe=insert_candles)




