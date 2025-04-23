from dotenv import load_dotenv
from minio import Minio
import os
import pandas as pd
from io import StringIO
from clickhouse_driver import Client as ClickhouseClient
from Clickhouse.Clickhouse_boundary import get_table


def load_DIM(object_name):
    load_dotenv(dotenv_path= "/opt/airflow/dags/.env")
    MinioClient = Minio("minio:9000",access_key=os.getenv("MINIO_ACCESS_KEY"), secret_key=os.getenv("MINIO_SECRET_KEY"),secure=False)
    
    ClickhouseClientSetting = {'use_numpy': True}
    clickhouseClient = ClickhouseClient(
        host = "clickhosue",
        port= "9000",
        user=os.getenv("CLICKHOUSE_USERNAME"),
        password = os.getenv("CLICKHOUSE_PASSWORD"),
        database = "default",
        settings = ClickhouseClientSetting
    )
    
    
    
    #doc du lieu tu trong minio
    response = MinioClient.get_object(
        bucket_name="stock-symbol",
        # object_name="nasdaq_screener_nasdaq.csv"
        object_name = object_name
    )
    data = response.read().decode("utf-8")
    nasdaq_symbol_df = pd.read_csv(StringIO(data))

    


    # import du lieu country
    country_df = nasdaq_symbol_df[["Country"]]
    country_df.loc[pd.isna(country_df["Country"]),"Country"] = "unpredict"
    country_df.drop_duplicates(inplace = True)
    country_df.rename(columns={
        "Country": "country_name"
    },inplace=True)

    curr_country_df = get_table("country_DIM")
    country_df = country_df[~country_df["country_name"].isin(curr_country_df["country_name"])]


    clickhouseClient.insert_dataframe(query="insert into country_DIM(country_name) values",dataframe=country_df)


    # #import du lieu Sector
    sector_df = nasdaq_symbol_df[["Sector"]]
    sector_df.loc[pd.isna(sector_df["Sector"]),"Sector"] = "unpredict"
    sector_df.drop_duplicates(inplace=True)
    print(sector_df)
    sector_df.rename(columns={
        "Sector": "sector"
    },inplace=True)
    curr_sector_df = get_table("sector_DIM")
    sector_df = sector_df[~sector_df["sector"].isin(curr_sector_df["sector"])]
    clickhouseClient.insert_dataframe(query="insert into sector_DIM(sector) values",dataframe=sector_df)


    # #import du lieu Industry
    industry_df = nasdaq_symbol_df[["Industry"]]
    industry_df.loc[pd.isna(industry_df["Industry"]),"Industry"] = "unpredict"
    industry_df.drop_duplicates(inplace=True)
    industry_df.rename(columns={
        "Industry": "industry_name"
    },inplace=True)
    curr_industry_df = get_table("industry_DIM")
    industry_df = industry_df[~industry_df["industry_name"].isin(curr_industry_df["industry_name"])]
    clickhouseClient.insert_dataframe(query="insert into industry_DIM(industry_name) values",dataframe=industry_df)

def load_DIM_all():
    load_DIM("nasdaq_screener_nasdaq.csv")
    load_DIM("nasdaq_screener_nyse.csv")
    load_DIM("nasdaq_screener_amex.csv")



def load_stock_symbol_DIM(object_name, exchange_id):
    load_dotenv(dotenv_path= "/opt/airflow/dags/.env")
    MinioClient = Minio("minio:9000",access_key=os.getenv("MINIO_ACCESS_KEY"), secret_key=os.getenv("MINIO_SECRET_KEY"),secure=False)
    
    ClickhouseClientSetting = {'use_numpy': True}
    clickhouseClient = ClickhouseClient(
        host = "clickhosue",
        port= "9000",
        user=os.getenv("CLICKHOUSE_USERNAME"),
        password = os.getenv("CLICKHOUSE_PASSWORD"),
        database = "default",
        settings = ClickhouseClientSetting
    )

    response = MinioClient.get_object(
        bucket_name="stock-symbol",
        # object_name="nasdaq_screener_nasdaq.csv"
        object_name=object_name
    )
    data = response.read().decode("utf-8")
    nasdaq_symbol_df = pd.read_csv(StringIO(data))

    nasdaq_symbol_df["exchange"] = (exchange_id)
    nasdaq_symbol_df.loc[nasdaq_symbol_df["Name"] == "Nano Labs Ltd Class A Ordinary Shares", "Symbol"] = "NA"


    nasdaq_symbol_import_df = nasdaq_symbol_df[["Symbol","Name","Country","IPO Year","Sector","Industry","exchange"]]
    nasdaq_symbol_import_df.loc[pd.isna(nasdaq_symbol_import_df["Industry"]),"Industry"] = "unpredict"
    nasdaq_symbol_import_df.loc[pd.isna(nasdaq_symbol_import_df["Country"]),"Country"] = "unpredict"
    nasdaq_symbol_import_df.loc[pd.isna(nasdaq_symbol_import_df["Sector"]),"Sector"] = "unpredict"


    # print("nasdaq_symbol_import_df:")
    # print(nasdaq_symbol_import_df.info())

    # join dwx lieu voi cac DIM khac

    data, columns  = clickhouseClient.execute(query="select * from sector_DIM", with_column_types=True)
    sector_df = pd.DataFrame([row for row in data],columns = [col[0] for col in columns])


    data, columns  = clickhouseClient.execute(query="select * from country_DIM", with_column_types=True)
    country_df = pd.DataFrame([row for row in data],columns = [col[0] for col in columns])


    data, columns  = clickhouseClient.execute(query="select * from industry_DIM", with_column_types=True)
    industry_df = pd.DataFrame([row for row in data],columns = [col[0] for col in columns])

    join_df = pd.merge(nasdaq_symbol_import_df,country_df,how="left",left_on="Country",right_on="country_name")
    join_df = pd.merge(join_df,sector_df,how="left",left_on="Sector",right_on="sector")
    join_df = pd.merge(join_df,industry_df,how="left",left_on="Industry",right_on="industry_name")

    join_df = join_df[["Symbol","Name","country_id","sector_id","industry_id","exchange"]]

    join_df = join_df.rename(columns={
        "Symbol": "stock_symbol",
        "Name" : "stock_symbol_name"
    })


    join_df["exchange"] = join_df["exchange"].astype("UInt8")
    join_df["country_id"] = join_df["country_id"].astype(str)
    join_df["sector_id"] = join_df["sector_id"].astype(str)
    join_df["industry_id"] = join_df["industry_id"].astype(str)


    


    print("join_df")
    print(join_df.info())
    print(join_df[join_df["country_id"] == ''])
    clickhouseClient.insert_dataframe(query = "insert into stock_symbol_DIM values",dataframe = join_df)


def load_stock_symbol_DIM_all():
    load_stock_symbol_DIM("nasdaq_screener_nasdaq.csv",1)
    load_stock_symbol_DIM("nasdaq_screener_nyse.csv",2)
    load_stock_symbol_DIM("nasdaq_screener_amex.csv",3)

