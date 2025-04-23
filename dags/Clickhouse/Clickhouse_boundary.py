from clickhouse_driver import Client
import pandas as pd
import os
from dotenv import load_dotenv
from io import StringIO



def getClickhouseClient():
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
    return ClickhouseClient


def init_table():
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


    scripts = ["""
        create table if not exists industry_DIM(
            industry_id UUID DEFAULT generateUUIDv4(),
            industry_name String
        ) engine = MergeTree
        ORDER BY industry_id;""",

        """
        create table if not exists exchange_DIM(
            exchange_id UInt8,
            exchange_name String
        ) engine = MergeTree
        ORDER BY exchange_id;

        """,

        """create table if not exists country_DIM(
            country_id UUID DEFAULT generateUUIDv4(),
            country_name String
        ) engine MergeTree
        ORDER BY country_id
        """,


        """create table if not exists sector_DIM(
            sector_id UUID DEFAULT generateUUIDv4(),
            sector String
        ) engine = MergeTree
        ORDER BY sector_id
        """,

        """create table if not exists stock_symbol_DIM(
            stock_symbol String,
            stock_symbol_name String,
            country_id UUID,
            sector_id UUID,
            industry_id UUID,
            exchange UInt8
        ) engine = MergeTree
        ORDER BY stock_symbol;""",

        """create table if not exists source_DIM(
            source_id UUID DEFAULT generateUUIDv4(),
            source_name String,
            source_domain String
        ) engine = MergeTree
        ORDER BY source_id;""",

        """
        create table if not exists topic_DIM
        (
            topic_id UUID DEFAULT generateUUIDv4(),
            topic_name String
        ) engine = MergeTree
        ORDER BY topic_id;
        """,


        """create table if not exists news_DIM(
            insert_date Date,
            in_date_id UInt32,
            title String,
            url String,
            time_publish_id Date,
            source_id UUID,
            overall_sentiment_score Double
        ) engine = MergeTree
        ORDER BY (insert_date,in_date_id);""",

        """create table if not exists date_DIM(
            date_id String,
            date Date
        ) engine = MergeTree
        ORDER BY date_id;""",
        
        """create table if not exists candles_FACT(
            stock_symbol String,
            o Float32,
            h Float32,
            c Float32,
            l Float32,
            v UInt32,
            Trans UInt32,
            t String,
            vwap Float32
        ) engine = MergeTree
        ORDER BY (t,stock_symbol);""",

        """create table if not exists new_sentiment_stock_FACT(
            insert_date Date,
            in_date_id UInt32,
            stock_symbol String,
            sentiment_score Float32
        ) engine = MergeTree
        ORDER BY (insert_date,in_date_id,stock_symbol);""",

        """
            create table if not exists new_sentiment_industry_FACT
            (
            insert_date Date,
            in_date_id UInt32,
            topic_id UUID,
            sentiment_score Float32
            ) engine = MergeTree
            ORDER BY (insert_date,in_date_id,topic_id);""",

        """
        delete from exchange_DIM where exchange_id < 4;
        """,

        """
        insert into exchange_DIM values(1,'nasdaq'), (2,'nyse'),(3,'amex')
        """,

        """create table if not exists shot_MA(
            stock_symbol String,
            t String,
            ma Float64
        ) engine = MergeTree
        ORDER BY (stock_symbol,t)
        """,

        """create table if not exists long_MA(
            stock_symbol String,
            t String,
            ma Float64
        ) engine = MergeTree
        ORDER BY (stock_symbol,t)
        """,

        """
        create table if not exists avg_volume(
            stock_symbol String,
            date Date,
            sliding_avg Float32
        ) engine = MergeTree
        ORDER BY (stock_symbol, date)
        """,

        """
        create table if not exists ohcl_indicator(
            stock_symbol String,
            date String, 
            delta Float64,
            sliding_avg_3 Float64,
            sliding_avg_5 Float64
        ) engine = MergeTree
        ORDER BY date
        """        
        ]


    for script in scripts:
        ClickhouseClient.execute(script)

def get_table(table_name):
    ClickhouseClient = getClickhouseClient()
    data, columns  = ClickhouseClient.execute(query  =f"select * from {table_name}", with_column_types=True)
    ticker_df = pd.DataFrame([row for row in data],columns = [col[0] for col in columns])
    return ticker_df


def query_table(query):
    ClickhouseClient = getClickhouseClient()
    data, columns  = ClickhouseClient.execute(query  =f"{query}", with_column_types=True)
    ticker_df = pd.DataFrame([row for row in data],columns = [col[0] for col in columns])
    return ticker_df



def count_long_MA():
    ClickhouseClient = getClickhouseClient()
    curr_ohcl = ClickhouseClient.execute("""
    WITH max_data AS (
    SELECT toDate(max(toDate(toInt64(left(t,10))))) AS max_date FROM candles_FACT
    ),
    select * from candles_FACT where  toDate(toInt64(left(t, 10))) = (select max_date from max_data))
    group by 
""")
