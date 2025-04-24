import pandas as pd
import os
import numpy as np
from dotenv import load_dotenv
from minio import Minio
from datetime import timezone,timedelta,datetime
from io import StringIO
from clickhouse_driver import Client
import io
import json
from  Clickhouse.Clickhouse_boundary import get_table,getClickhouseClient

def load_news(**kwargs):
    execution_date = kwargs['ds']

    load_dotenv(dotenv_path= "/opt/airflow/dags/.env")
    minioClient = Minio("minio:9000",access_key=os.getenv("MINIO_ACCESS_KEY"), secret_key=os.getenv("MINIO_SECRET_KEY"),secure=False)

    clickhouseClient = getClickhouseClient()


    # et_tz = timezone(offset=timedelta(days=0),name = 'America/New_York')
    # et_time = datetime.now(et_tz) - timedelta(days=2)
    # print(et_time)
    # time_now = datetime.now()

    lastest_obj = None
    obj_list = minioClient.list_objects(bucket_name="news-data")
    for item in obj_list:
        if not lastest_obj or item.last_modified > lastest_obj.last_modified :
            lastest_obj = item
    if not lastest_obj:
        return
    # test_object_name = "news_2025-03-31.json"
    et_time = datetime.strptime(lastest_obj.object_name[5:15], "%Y-%m-%d")
    # et_time = datetime.strptime(test_object_name[5:15], "%Y-%m-%d")
    


    time_now = datetime.strptime(execution_date, r"%Y-%m-%d")
    time_now = et_time + timedelta(days = 2)
    print(et_time.date())
    print("now: " + str(time_now))


    # doc du lieu tu minio
    reponse = minioClient.get_object(
        bucket_name = "news-data",
        object_name = lastest_obj.object_name
        # object_name=test_object_name
    )
    data = reponse.read().decode("utf-8")


    df = pd.read_json(StringIO(data),lines = True)


    # kiem tra xem co source nao moi khong
    source_df = df[["source", "source_domain"]].drop_duplicates(subset = ["source","source_domain"])
    current_source_df = get_table("source_DIM")
    join_source_df = pd.merge(source_df, current_source_df,left_on = "source",right_on = "source_name",how="left",suffixes = ["_new","_cur"])
    new_source_df = join_source_df[pd.isna(join_source_df["source_name"])]
    insert_new_source_df = new_source_df[["source","source_domain_new"]]
    if len(insert_new_source_df) >= 1:
        clickhouseClient.insert_dataframe(query = "insert into source_DIM(source_name,source_domain) values", dataframe= insert_new_source_df.rename(columns = {"source": "source_name", "source_domain_new":"source_domain"}))



    
    #kiem tra xem co topic nao moi khong
    topic_df = df[["topics"]].explode('topics')
    filter_topic = topic_df[pd.notna(topic_df["topics"])]
    filter_topic["topic"] = filter_topic["topics"].apply(lambda x: x.get("topic"))
    filter_topic["relevance_score"] = filter_topic["topics"].apply(lambda x: x.get("relevance_score"))

    # insert topic
    topic_name_df = filter_topic[["topic"]].drop_duplicates(subset = ["topic"])
    current_topic_name_df = get_table("topic_DIM")

    topic_join_df = pd.merge(topic_name_df,current_topic_name_df,left_on="topic",right_on="topic_name",how="left")

    new_topic_df = topic_join_df[pd.isna(topic_join_df["topic_name"])]

    if(len(new_topic_df) >= 1):
        clickhouseClient.insert_dataframe(query = "insert into topic_DIM(topic_name) values", dataframe=new_topic_df[["topic"]].rename(columns={"topic": "topic_name"}))
    
    
    # insert news
    insert_news_df = df[["title","url", "time_published","source","overall_sentiment_score","in_date_id"]]
    current_source_df = get_table("source_DIM")
    join_news_df = pd.merge(insert_news_df,current_source_df,left_on = "source",right_on = "source_name",suffixes = ["_left","_right"],how = "left")
    join_news_df["time_published"] = join_news_df["time_published"].apply(lambda x : str(datetime.strptime(x[:8], "%Y%m%d").date()))

    join_news_df["insert_date"] = str(time_now.date())
    # print(join_news_df.info())

    if(len(insert_news_df) >= 1):
        clickhouseClient.insert_dataframe(query = "insert into news_DIM(title, url, time_publish_id, source_id, overall_sentiment_score,insert_date,in_date_id) values", dataframe = join_news_df[["title","url","time_published", "source_id","overall_sentiment_score","insert_date","in_date_id"]].rename(columns = {
            "time_published": "time_publish_id",
        }))



    # insert sentiment topic FACT
    sentiment_industry_df = df[["in_date_id", "topics"]]
    sentiment_industry_df = sentiment_industry_df.explode("topics")
    sentiment_industry_df = sentiment_industry_df[pd.notna(sentiment_industry_df["topics"])]
    sentiment_industry_df["topic"] = sentiment_industry_df["topics"].apply(lambda x : x.get("topic"))
    sentiment_industry_df["sentiment_score"] = sentiment_industry_df["topics"].apply(lambda x : x.get("relevance_score"))
    
    current_topic_df = get_table("topic_DIM")

    join_sentiment_industry_df = pd.merge(sentiment_industry_df,current_topic_df,left_on="topic", right_on = "topic_name",how = "left")

    # print(join_sentiment_industry_df.info())

    insert_sentiment_industry_df = join_sentiment_industry_df[["in_date_id","topic_id","sentiment_score"]]
    insert_sentiment_industry_df["insert_date"] = str(time_now.date())
    # print(insert_sentiment_industry_df.info())

    clickhouseClient.insert_dataframe(query = "insert into new_sentiment_industry_FACT(insert_date,in_date_id, topic_id, sentiment_score) values",dataframe = insert_sentiment_industry_df)

    # insert sentiment stock FACT   
    sentiment_stock_df = df[["in_date_id", "ticker_sentiment"]]
    sentiment_stock_df = sentiment_stock_df.explode("ticker_sentiment")
    sentiment_stock_df = sentiment_stock_df[pd.notna(sentiment_stock_df["ticker_sentiment"])]
    sentiment_stock_df["ticker"] = sentiment_stock_df["ticker_sentiment"].apply(lambda x : x.get("ticker"))
    sentiment_stock_df["sentiment_score"] = sentiment_stock_df["ticker_sentiment"].apply(lambda x : x.get("ticker_sentiment_score"))
    
    current_stock_symbol_df = get_table("stock_symbol_DIM")

    join_sentiment_stock_df = pd.merge(sentiment_stock_df,current_stock_symbol_df,left_on = "ticker", right_on = "stock_symbol")

    insert_sentiment_stock_df = join_sentiment_stock_df[["in_date_id","stock_symbol","sentiment_score"]]
    insert_sentiment_stock_df["insert_date"] = str(time_now.date())

    clickhouseClient.insert_dataframe(query = "insert into new_sentiment_stock_FACT(insert_date, in_date_id, stock_symbol, sentiment_score) values", dataframe = insert_sentiment_stock_df)


    

    
