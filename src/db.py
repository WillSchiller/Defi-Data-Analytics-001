import psycopg2
import re
from io import StringIO
import csv
import pandas as pd
import time
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()

# REMOTE PSQL SERVER CONNECTION (MAIN) #
conn = psycopg2.connect(
    host = os.getenv('REMOTE_PSQL_HOST'),
    port = os.getenv('REMOTE_PORT'),
    user = os.getenv('REMOTE_PSQL_USER'),
    password = os.getenv('REMOTE_PASSWORD'),
    database=os.getenv('REMOTE_DB')
    )
cursor = conn.cursor()


# LOCAL PSQL SERVER CONNECTION (COPY) #
conn_local = psycopg2.connect(
    host = os.getenv('LOCAL_PSQL_HOST'),
    port = os.getenv('LOCAL_PORT'),
    user = os.getenv('LOCAL_PSQL_USER'),
    password = os.getenv('LOCAL_PASSWORD'),
    database= os.getenv('LOCAL_DB'),
    )
cursor_local = conn_local.cursor()





# -----------------------------------------------
# TABLE CREATION #
# -----------------------------------------------

tweetvolumes = "CREATE TABLE IF NOT EXISTS tweetvolumes(id text PRIMARY KEY, timezone text,  timestamp INTEGER, date TIMESTAMP, symbol text, count FLOAT8, sma1 FLOAT8, sma2 FLOAT8, sma3 FLOAT8, sentiment FLOAT8)"
tweetvolumescleaned = "CREATE TABLE IF NOT EXISTS tweetvolumescleaned(id text PRIMARY KEY, timezone text,  timestamp INTEGER, date TIMESTAMP, symbol text, count FLOAT8, sentiment FLOAT8)"
tweetvolumeshours = "CREATE TABLE IF NOT EXISTS tweetvolumeshours(id text PRIMARY KEY, timezone text,  timestamp INTEGER, date TIMESTAMP, symbol text, count FLOAT8, sma24 FLOAT8, sma168 FLOAT8, sentiment FLOAT8, savg24 FLOAT8, savg168 FLOAT8)"
rsd_metrics = "CREATE TABLE IF NOT EXISTS rsd_metrics(id text PRIMARY KEY, timezone text, timestamp INTEGER, date TIMESTAMP, symbol text, count FLOAT8, symbol_sma1 FLOAT8, symbol_sma1_previous FLOAT8, symbol_sma7 FLOAT8, symbol_sma7_previous FLOAT8, symbol_sma14 FLOAT8, sma1_dif FLOAT8 ,sma7_dif FLOAT8, sma14_dif FLOAT8, sentiment FLOAT8, sentiment_sma1 FLOAT8, sentiment_sma7 FLOAT8, rsd_1 FLOAT8, rsd_7 FLOAT8, rsd_14 FLOAT8)"
pricedata = "CREATE TABLE IF NOT EXISTS pricedata(id text PRIMARY KEY, symbol TEXT, last_updated_at INTEGER, usd FLOAT8, usd_24h_change FLOAT8, usd_market_cap BIGINT, usd_24h_vol BIGINT)" 
tokens = "CREATE TABLE IF NOT EXISTS tokens(symbol text PRIMARY KEY, name TEXT)"


def create_table(sql):
    cursor_local.execute(sql)
    conn_local.commit()
    print("table created")
    #cursor.execute(sql)
    #conn.commit()
    #print("table created")


def drop_table(table):
    sql = f"DROP TABLE {table}"
    cursor_local.execute(sql)
    conn_local.commit()
    print("drop table")
    #cursor.execute(sql)
    #conn.commit()
    #print("drop table")





# -----------------------------------------------
# CALCULATE METRICS # 
# -----------------------------------------------
##create overlap and update metrics + cant go backwards. 
def backdate_rsd_metrics(start_time, end_time=0):
    if end_time == 0:
        end_time = int(time.time())
    else:
        pass

    while start_time <= end_time:
        loop_end_time = start_time + 1296000
        if loop_end_time > end_time:
            loop_end_time = end_time
            

        print(f"start time is: {start_time} /// end time is: {end_time}")
        make_rsd_metrics(start_time, loop_end_time)
        start_time = start_time + 86400


    

def make_rsd_metrics(start_time, end_time=0):
    if end_time == 0:
        end_time = int(time.time())
    else:
        pass

    
    df = pd.read_sql_query(f"SELECT id, timezone, date, timestamp, symbol, count, sentiment from tweetvolumescleaned WHERE timestamp >= {start_time} AND timestamp <= {end_time} GROUP BY 1,2,3 ORDER BY date ASC" ,conn_local)
    #sma
    df['symbol_sma1'] = df.groupby('symbol')['count'].transform(lambda x: x.rolling(24, 0).mean())
    df['symbol_sma1_previous'] = df.groupby('symbol')['count'].transform(lambda x: x.rolling(48, 24).mean())
    df['symbol_sma7'] = df.groupby('symbol')['count'].transform(lambda x: x.rolling(168, 0).mean())
    df['symbol_sma7_previous'] = df.groupby('symbol')['count'].transform(lambda x: x.rolling(336, 168).mean())
    df['symbol_sma14'] = df.groupby('symbol')['count'].transform(lambda x: x.rolling(336, 0).mean())
    #dif
    df['sma1_dif'] = (df['symbol_sma1'] - df['symbol_sma1_previous']) / df['symbol_sma1_previous']
    df['sma7_dif'] = (df['symbol_sma7'] - df['symbol_sma7_previous']) / df['symbol_sma7_previous']
    df['sma14_dif'] = (df['count'] -  df['symbol_sma14']) /df['count']

    #sentiment
    df['sentiment_sma1'] = df.groupby('symbol')['sentiment'].transform(lambda x: x.rolling(24, 0).mean())
    df['sentiment_sma7'] = df.groupby('symbol')['sentiment'].transform(lambda x: x.rolling(168, 0).mean())
    #std cv
    df['rsd_1'] = df.groupby('symbol')['count'].transform(lambda x: x.rolling(24, 0).std()).fillna(0)/df.groupby('symbol')['count'].transform(lambda x: x.rolling(24, 0).mean())
    df['rsd_7'] = df.groupby('symbol')['count'].transform(lambda x: x.rolling(168, 0).std()).fillna(0)/df.groupby('symbol')['count'].transform(lambda x: x.rolling(168, 0).mean())
    df['rsd_14'] = df.groupby('symbol')['count'].transform(lambda x: x.rolling(336, 0).std()).fillna(0)/df.groupby('symbol')['count'].transform(lambda x: x.rolling(336, 0).mean())
    #df['std56'] = df.groupby('symbol')['count'].transform(lambda x: x.rolling(1344, 0).std()).fillna(0)
    #df['std112'] = df.groupby('symbol')['count'].transform(lambda x: x.rolling(2688, 0).std()).fillna(0)
    #pd.set_option('display.max_columns', None)
    cols = ['id','timezone', 'timestamp', 'date', 'symbol', 'count', 'symbol_sma1', 'symbol_sma1_previous', 'symbol_sma7', 'symbol_sma7_previous', 'symbol_sma14', 'sma1_dif', 'sma7_dif', 'sma14_dif', 'sentiment', 'sentiment_sma1', 'sentiment_sma7', 'rsd_1', 'rsd_7', 'rsd_14']
    df = df[cols]
    df = df.fillna(0)
    df = df[df.timestamp > (end_time - 86400)]
    print(df)
    df_ready = df.empty
    if df_ready != True:
        add_metrics_local(df, 'rsd_metrics')
    else:
        print("df is empty")

def make_tokens_index():
    SQL = "SELECT DISTINCT symbol FROM rsd_metrics WHERE symbol != 'NOT_A_SYMBOL' AND count > 10"
    df = pd.read_sql_query(SQL ,conn_local)
    df_ready = df.empty
    if df_ready != True:
        add_tokens(df)



# -----------------------------------------------
# PUSH DATA TO DB # 
# -----------------------------------------------

def add(df, cols, t):
    sio = StringIO()
    writer = csv.writer(sio)
    writer.writerows(df.values)
    sio.seek(0)
    with conn_local.cursor() as c:
        c.copy_from(
            file=sio,
            table=t,
            columns=cols,
            sep=","
        )
        conn_local.commit()

def add_tokens(df):
    sio = StringIO()
    writer = csv.writer(sio)
    writer.writerows(df.values)
    sio.seek(0)
    with conn_local.cursor() as c:
        c.copy_from(
            file=sio,
            table="tokens",
            columns=[
            "symbol"
        ],
            sep=","
        )
        conn_local.commit()

def add_all(df):
    sio = StringIO()
    writer = csv.writer(sio)
    writer.writerows(df.values)
    sio.seek(0)
    with conn_local.cursor() as c:
        c.copy_from(
            file=sio,
            table="rsd_metrics",
            columns=[
            "id",
            "timezone",
            "timestamp",
            "date",
            "symbol",
            "count",
            "symbol_sma1",
            "symbol_sma7",
            "symbol_sma14",
            "sma1_dif",
            "sma7_dif", 
            "sma14_dif",
            "sentiment",
            "sentiment_sma1",
            "sentiment_sma7",
            "rsd_1",
            "rsd_7",
            "rsd_14"
        ],
            sep=","
        )
        conn_local.commit()

def build_sql(df,t):
    rows = len(df.index)
    print(rows)
    values = ""
    if t == 'tweetvolumescleaned':

        for index, row in df.iterrows():

            if index < rows - 1:
                values = values + f"('{row[0]}', {row[1]}, {row[2]}, '{row[3]}', '{row[4]}', {row[5]}, {row[6]}), "
            else:
                values = values + f"('{row[0]}', {row[1]}, {row[2]}, '{row[3]}', '{row[4]}', {row[5]}, {row[6]})"

    elif t == 'rsd_metrics':
        df = df.reset_index()
        print(df)
        for index, row in df.iterrows():
            if index < rows - 1:
                values = values + f"('{row[1]}', '{row[2]}', {row[3]}, '{row[4]}', '{row[5]}', {row[6]}, {row[7]}, {row[8]}, {row[9]}, {row[10]}, {row[11]}, {row[12]}, {row[13]}, {row[14]}, {row[15]}, {row[16]}, {row[17]}, {row[18]}, {row[19]}, {row[20]}), "
            else:
                values = values + f"('{row[1]}', '{row[2]}', {row[3]}, '{row[4]}', '{row[5]}', {row[6]}, {row[7]}, {row[8]}, {row[9]}, {row[10]}, {row[11]}, {row[12]}, {row[13]}, {row[14]}, {row[15]}, {row[16]}, {row[17]}, {row[18]}, {row[19]}, {row[20]})"
    elif t == 'cmc_price':
        #df = df.reset_index()
        print(df)
        for index, row in df.iterrows():
            if index < rows - 1:
                values = values + f"('{row[0]}', '{row[1]}', '{row[2]}', {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]}, {row[8]}, {row[9]}, {row[10]}, {row[11]}, {row[12]}, {row[13]}, {row[14]}, '{row[15]}', {row[16]}, '{row[17]}'), "
            else:
                values = values + f"('{row[0]}', '{row[1]}', '{row[2]}', {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]}, {row[8]}, {row[9]}, {row[10]}, {row[11]}, {row[12]}, {row[13]}, {row[14]}, '{row[15]}', {row[16]}, '{row[17]}')"


    else:
        for index, row in df.iterrows():
            if index < rows - 1:
                values = values + f"({row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, '{row[5]}', '{row[6]}'), "
            else:
                values = values + f"({row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, '{row[5]}', '{row[6]}')"
    #print(values)
    return values

    

def add_metrics(df, t):
    print(df)
    print("adding metrics")
    values = build_sql(df,t)

    sql = f"INSERT INTO {t}(id, timezone, timestamp, date, symbol, count, sentiment) VALUES{values} ON CONFLICT (id) DO NOTHING"
    #print(sql)
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
    conn.commit()
    print("metrics added")


def add_metrics_local(df, t):
    print("adding metrics")
    values = build_sql(df,t)
    print(values)
    sql = ''
    if t =='tweetvolumescleaned':
        sql = f"INSERT INTO {t}(id, timezone, timestamp, date, symbol, count, sentiment) VALUES{values} ON CONFLICT (id) DO NOTHING"
    elif t == 'rsd_metrics':
        sql = f"INSERT INTO {t}(id, timezone, timestamp, date, symbol, count, symbol_sma1, symbol_sma1_previous, symbol_sma7, symbol_sma7_previous, symbol_sma14, sma1_dif, sma7_dif, sma14_dif, sentiment, sentiment_sma1, sentiment_sma7, rsd_1, rsd_7, rsd_14) VALUES{values} ON CONFLICT (id) DO NOTHING"
    elif t == 'cmc_price':
        sql = f"INSERT INTO {t}(source, symbol, slug, max_supply, circulating_supply, total_supply, price, volume_24h, volume_change_24h, percent_change_1h, percent_change_24h, percent_change_7d, market_cap, market_cap_dominance, fully_diluted_market_cap, last_updated, timestamp, id) VALUES{values} ON CONFLICT (id) DO NOTHING"
    else:
        sql = f"INSERT INTO {t}(usd, usd_market_cap, usd_24h_vol, usd_24h_change, last_updated_at, symbol, id) VALUES{values} ON CONFLICT (id) DO NOTHING"


    #print(sql)
    try:
        cursor_local.execute(sql)
    except Exception as e:
        print(e)
    conn_local.commit()
    print("metrics added locally")

# -----------------------------------------------
# HELPER FUNCTIONS / TABLES #
# -----------------------------------------------


if __name__ == '__main__':
    #drop_table('rsd_metrics')
    #create_table(rsd_metrics)
    #make_tokens_index()
    make_rsd_metrics(int(time.time()) - 1296000)
    #backdate_rsd_metrics(1640995200)
    