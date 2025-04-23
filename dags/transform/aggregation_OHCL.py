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
from Clickhouse.Clickhouse_boundary import get_table,getClickhouseClient, query_table



def aggregate_OHCL():
    clickhouseClient = getClickhouseClient()

    script = """
        WITH max_data AS (
        SELECT max(toDate(toInt64(left(t,10)))) AS max_date FROM candles_FACT
    ),
        second_max_data as(
            SELECT max(toDate(toInt64(left(t,10)))) AS max_date_2 FROM candles_FACT where toDate(toInt64(left(t,10))) !=
                                                                                        (select max_date from max_data)
        ),
    curr_price as (select c,stock_symbol from candles_FACT where toDate(toInt64(left(t,10))) = (select max_date from
    max_data)),
    previous_price as (select c,stock_symbol from candles_FACT where toDate(toInt64(left(t,10))) = (select max_date_2 from
    second_max_data))
    select (select max_date from max_data) as date,curr_price.c - previous_price.c as delta, stock_symbol
    from curr_price join previous_price on curr_price.stock_symbol = previous_price.stock_symbol
    """


    curr_price_df = query_table(script)


    get_3_near_price_script = """with near_3_day as (
        SELECT (toDate(toInt64(left(t,10)))) AS near_date FROM candles_FACT order by toDate(toInt64(left(t,10))) limit 3
    )
        select avg(c) as avg_c_3, stock_symbol
        from candles_FACT where toDate(toInt64(left(t,10))) in (select near_date from near_3_day) group by stock_symbol"""

    near_3_price_df = query_table(get_3_near_price_script)


    get_5_near_price_script = """with near_5_day as (
        SELECT (toDate(toInt64(left(t,10)))) AS near_date FROM candles_FACT order by toDate(toInt64(left(t,10))) limit 5
    )
        select avg(c) as avg_c_5, stock_symbol
        from candles_FACT where toDate(toInt64(left(t,10))) in (select near_date from near_5_day) group by stock_symbol"""


    near_5_price_df = query_table(get_5_near_price_script)

    insert_df = pd.merge(curr_price_df,near_3_price_df,left_on="stock_symbol", right_on="stock_symbol")

    insert_df = pd.merge(insert_df,near_5_price_df,left_on = "stock_symbol", right_on="stock_symbol")

    insert_df.rename(columns = {"avg_c_3": "sliding_avg_3", "avg_c_5":"sliding_avg_5"},inplace=True)
    insert_df["date"] = insert_df["date"].apply(lambda x : str(x))
    print(insert_df.info())
    clickhouseClient.insert_dataframe(query = "insert into ohcl_indicator values",dataframe=insert_df)


    # short_ma_df = near_3_price_df[]



