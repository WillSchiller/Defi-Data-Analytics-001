import psycopg2
import re
from io import StringIO
import csv
import pandas as pd
import time
import numpy as np
from dotenv import load_dotenv
import os
import sql

load_dotenv()

time = int(time.time()) 

# LOCAL PSQL SERVER CONNECTION (COPY) #
conn_local = psycopg2.connect(
    host = os.getenv('LOCAL_PSQL_HOST'),
    port = os.getenv('LOCAL_PORT'),
    user = os.getenv('LOCAL_PSQL_USER'),
    password = os.getenv('LOCAL_PASSWORD'),
    database= os.getenv('LOCAL_DB'),
    )
cursor_local = conn_local.cursor()



# SQL queries  

tweet_trend =  ("SELECT DATE_TRUNC('day', date) AS date, sum(count) AS tweets FROM rsd_metrics WHERE symbol != 'NOT_A_SYMBOL' GROUP BY 1 ORDER BY date ASC", "CREATE TABLE IF NOT EXISTS tweet_trend(date TIMESTAMP, tweets FLOAT8)", "tweet_trend", ["date", "tweets"])
trending = ("SELECT * FROM rsd_metrics WHERE symbol != 'NOT_A_SYMBOL' AND timestamp >= (SELECT MAX(timestamp) FROM rsd_metrics) AND count >= 100 ORDER BY sma1_dif DESC LIMIT 10", "CREATE TABLE IF NOT EXISTS trending(id text PRIMARY KEY, timezone text, timestamp INTEGER, date TIMESTAMP, symbol text, count FLOAT8, symbol_sma1 FLOAT8, symbol_sma1_previous FLOAT8, symbol_sma7 FLOAT8, symbol_sma7_previous FLOAT8, symbol_sma14 FLOAT8, sma1_dif FLOAT8, sma7_dif FLOAT8, sma14_dif FLOAT8, sentiment FLOAT8, sentiment_sma1 FLOAT8, sentiment_sma7 FLOAT8, rsd_1 FLOAT8, rsd_7 FLOAT8, rsd_14 FLOAT8)", "trending", ["id", "timezone", "timestamp", "date", "symbol", "count", "symbol_sma1", "symbol_sma1_previous", "symbol_sma7", "symbol_sma7_previous", "symbol_sma14", "sma1_dif", "sma7_dif", "sma14_dif", "sentiment", "sentiment_sma1", "sentiment_sma7", "rsd_1", "rsd_7", "rsd_14"])
volatile = ("SELECT * FROM rsd_metrics WHERE symbol != 'NOT_A_SYMBOL' AND timestamp >= (SELECT MAX(timestamp) FROM rsd_metrics) AND count >= 100 ORDER BY rsd_7 DESC LIMIT 10",  "CREATE TABLE IF NOT EXISTS volatile(id text PRIMARY KEY, timezone text, timestamp INTEGER, date TIMESTAMP, symbol text, count FLOAT8, symbol_sma1 FLOAT8, symbol_sma1_previous FLOAT8, symbol_sma7 FLOAT8, symbol_sma7_previous FLOAT8, symbol_sma14 FLOAT8, sma1_dif FLOAT8, sma7_dif FLOAT8, sma14_dif FLOAT8, sentiment FLOAT8, sentiment_sma1 FLOAT8, sentiment_sma7 FLOAT8, rsd_1 FLOAT8, rsd_7 FLOAT8, rsd_14 FLOAT8)", "volatile", ["id", "timezone", "timestamp", "date", "symbol", "count", "symbol_sma1", "symbol_sma1_previous", "symbol_sma7", "symbol_sma7_previous", "symbol_sma14", "sma1_dif", "sma7_dif", "sma14_dif", "sentiment", "sentiment_sma1", "sentiment_sma7", "rsd_1", "rsd_7", "rsd_14"])
descreasing = ("SELECT * FROM rsd_metrics WHERE symbol != 'NOT_A_SYMBOL' AND timestamp >= (SELECT MAX(timestamp) FROM rsd_metrics) AND count >= 100 ORDER BY sma1_dif ASC LIMIT 10", "CREATE TABLE IF NOT EXISTS descreasing(id text PRIMARY KEY, timezone text, timestamp INTEGER, date TIMESTAMP, symbol text, count FLOAT8, symbol_sma1 FLOAT8, symbol_sma1_previous FLOAT8, symbol_sma7 FLOAT8, symbol_sma7_previous FLOAT8, symbol_sma14 FLOAT8, sma1_dif FLOAT8, sma7_dif FLOAT8, sma14_dif FLOAT8, sentiment FLOAT8, sentiment_sma1 FLOAT8, sentiment_sma7 FLOAT8, rsd_1 FLOAT8, rsd_7 FLOAT8, rsd_14 FLOAT8)",  "descreasing", ["id", "timezone", "timestamp", "date", "symbol", "count", "symbol_sma1", "symbol_sma1_previous", "symbol_sma7", "symbol_sma7_previous", "symbol_sma14", "sma1_dif", "sma7_dif", "sma14_dif", "sentiment", "sentiment_sma1", "sentiment_sma7", "rsd_1", "rsd_7", "rsd_14"])
token_stats = ("SELECT * FROM rsd_metrics WHERE symbol != 'NOT_A_SYMBOL' AND timestamp >= (SELECT MAX(timestamp) FROM rsd_metrics)", "CREATE TABLE IF NOT EXISTS token_stats(id text PRIMARY KEY, timezone text, timestamp INTEGER, date TIMESTAMP, symbol text, count FLOAT8, symbol_sma1 FLOAT8, symbol_sma1_previous FLOAT8, symbol_sma7 FLOAT8, symbol_sma7_previous FLOAT8, symbol_sma14 FLOAT8, sma1_dif FLOAT8, sma7_dif FLOAT8, sma14_dif FLOAT8, sentiment FLOAT8, sentiment_sma1 FLOAT8, sentiment_sma7 FLOAT8, rsd_1 FLOAT8, rsd_7 FLOAT8, rsd_14 FLOAT8)", "token_stats", ["id", "timezone", "timestamp", "date", "symbol", "count", "symbol_sma1", "symbol_sma1_previous", "symbol_sma7", "symbol_sma7_previous", "symbol_sma14", "sma1_dif", "sma7_dif", "sma14_dif", "sentiment", "sentiment_sma1", "sentiment_sma7", "rsd_1", "rsd_7", "rsd_14"])

def runQuery(params):
    get, create_table, table_name, cols = params
    print(f"Running: {table_name}")

    try:
        cursor_local.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn_local.commit()
        print("dropped table")
    except Exception as e:
        print(f"COULD NOT DROP TABLE: {e}")

    try:
        cursor_local.execute(create_table)
        conn_local.commit()
        print("created new table")
    except Exception as e:
        print(f"COULD NOT CREATE TABLE: {e}")
    
    df = pd.read_sql_query(get ,conn_local)
    add_all(df, table_name, cols)


#Just for testing
def returnSQL(sql):
    cursor_local.execute(sql)
    d = cursor_local.fetchall()
    return d



def add_all(df, table_name, cols):
    sio = StringIO()
    writer = csv.writer(sio)
    writer.writerows(df.values)
    sio.seek(0)
    with conn_local.cursor() as c:
        c.copy_from(
            file=sio,
            table=table_name,
            columns=cols,
            sep=","
        )
        conn_local.commit()
        print("data_saved")
    

if __name__ == '__main__':

    runQuery(sql.create_sql("trending"))
    runQuery(sql.create_sql("volatile"))
    runQuery(sql.create_sql("descreasing"))

    runQuery(tweet_trend)
    #runQuery(trending)
    #runQuery(volatile)
    #runQuery(descreasing)
    runQuery(token_stats)