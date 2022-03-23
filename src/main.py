import psycopg2
import pandas as pd
import datetime
import time
import db
import re
import tokens
import keyring
from dotenv import load_dotenv
import os

load_dotenv()
tz = time.time() - 15778476 # Timestamp 6 months ago

# REMOTE PSQL SERVER CONNECTION (MAIN) #
conn_external = psycopg2.connect(
    host = os.getenv('REMOTE_PSQL_HOST'),
    port = os.getenv('REMOTE_PORT'),
    user = os.getenv('REMOTE_PSQL_USER'),
    password = os.getenv('REMOTE_PASSWORD'),
    database=os.getenv('REMOTE_DB')
    )
cursor_external = conn_external.cursor()


# -----------------------------------------------
# DATA PIPELINE #
# -----------------------------------------------
def get_data():
    sql = f'SELECT H.id,H.timezone,H.timestamp,H.date,H.symbol,H.count,H.sentiment FROM tweetvolumeshours H LEFT OUTER JOIN tweetvolumescleaned C ON (H.id = C.id) WHERE C.id IS NULL LIMIT 100000'
    df = pd.read_sql_query(sql,conn_external)
    df['symbol'] = df['symbol'].apply(lambda x: clean_data(x))
    print("opened")
    print("add data")
    #print(df.to_string())
    df_ready = df.empty
    if df_ready != True:
        t = "tweetvolumescleaned"
        cols=[
            "id",
            "timezone",
            "timestamp",
            "date",
            "symbol",
            "count",
            "sentiment"
        ]
        #Duplicates the table on remote server so that query can only pull items that are not already saved. 
        db.add_metrics(df, t)
        db.add_metrics_local(df,t)


def clean_data(string):
    found = False
    for x in tokens.tokens:
        if string.startswith(x):
            found = True
            return x
            break
    if found == False:
        if re.match('^[a-zA-Z]{1,4}$', string):
            found = True
            return string
        else:
            return 'NOT_A_SYMBOL'



if __name__ == '__main__':
    start = time.time()
    get_data()
    end = time.time()
    print(end - start)

