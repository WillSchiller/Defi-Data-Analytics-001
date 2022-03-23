import json 
import pandas as pd
from pycoingecko import CoinGeckoAPI
import list
import time
import numpy as np
import db # !Folder moved to src! # 
import datetime as dt
import pendulum

#---------------------------------------------------------------------#
# This script gets price data from GoinGecko API
# Turned out that this is just an overly complicated way to get data
# Script is now redundant since we now use CoinMarketCap & Binance APIs
#---------------------------------------------------------------------#

start = time.time()
cg = CoinGeckoAPI()
list_of_ids = list.ids

while list_of_ids:
    arr = []
    length = min([len(list_of_ids), 500])
    

    for i in range(length):
        arr.append(list_of_ids.pop(0))

    try:
        res = cg.get_price(ids=arr, vs_currencies='USD', include_market_cap='true', include_24hr_vol='true', include_24hr_change='true', include_last_updated_at='true')
        #Format data
        df = pd.DataFrame.from_dict(res).T
        df.insert(0, 'index', value=np.arange(len(df)))
        df['symbol'] = df.index
        df.index = df['index']
        df = df.drop(['index'], axis=1)
        pd.options.display.float_format = '{:.2f}'.format

        time_now = int((dt.datetime.now().strftime('%M')))
        

        if time_now >= 44:
            df['last_updated_at'] = dt.datetime.now()
            df['last_updated_at'] = df['last_updated_at'].dt.floor('H')
            df['last_updated_at'] = df['last_updated_at'].view('int64') / 10**9
            df['last_updated_at'] = df['last_updated_at'].astype(int)
            print("doing")
        else:
            df['last_updated_at'] = pd.to_datetime(df['last_updated_at'],unit='s', origin='unix')
            df['last_updated_at'] = df['last_updated_at'].dt.floor('H')
            df['last_updated_at'] = df['last_updated_at'].view('int64') / 10**9
            df['last_updated_at'] = df['last_updated_at'].astype(int)

        df['usd_market_cap'] = df['usd_market_cap'].fillna(0).astype(int)
        df['usd_24h_vol'] = df['usd_24h_vol'].fillna(0).astype(int)
        df['usd_24h_change'] = df['usd_24h_change'].fillna(0)
        df['usd'] = df['usd'].fillna(0)
        df['id'] = df['last_updated_at'].astype(str) + "-" + df['symbol']

        #print(df)
        df_ready = df.empty
        if df_ready != True:
            t = "pricedata"
            cols=[
                "usd",
                "usd_market_cap",
                "usd_24h_vol",
                "usd_24h_change",
                "last_updated_at",
                "symbol", 
                 "id",
            ]
            #Duplicates the table on remote server so that query can only pull items that are not already saved. 
            db.add_metrics_local(df,t)

    except Exception as e:
        print(f"{e} ----")

    #Prevent API rate limit
    time.sleep(1.2)

end = time.time()
print(end - start)