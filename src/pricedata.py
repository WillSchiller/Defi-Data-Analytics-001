from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from datetime import datetime
import pandas as pd
import json
from dotenv import load_dotenv
import os
import db
from dateutil import parser

pd.set_option('display.float_format', lambda x: '%.5f' % x)

# ---------------------------------------------------------------#
# API PARAMS
# ---------------------------------------------------------------#
load_dotenv()
headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': os.getenv('CMC_API_KEY'),
    }
url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'


# ---------------------------------------------------------------#
# MAKE REQUESTS CONCAT/PROCESS DATA & RETURN
# ---------------------------------------------------------------#
def getpairs():
    data = []
    start = 1
    for _ in range(2):
        parameters = {
            'start': start,
            'limit': '5000',
            'sort': 'market_cap',
        }
        session = Session()
        session.headers.update(headers)
        try:
            response = session.get(url, params=parameters)
            response = json.loads(response.content)
            response = response['data']
            for coin in response:
                data.append(
                    {
                    'source': 'CMC',
                    'symbol':coin['symbol'], 
                    'slug':coin['slug'], 
                    'max_supply': coin['max_supply'],
                    'circulating_supply': coin['circulating_supply'], 
                    'total_supply': coin['total_supply'],
                    'price': coin['quote']['USD']['price'],
                    'volume_24h': coin['quote']['USD']['volume_24h'],
                    'volume_change_24h': coin['quote']['USD']['volume_change_24h'],
                    'percent_change_1h': coin['quote']['USD']['percent_change_1h'], 
                    'percent_change_24h': coin['quote']['USD']['percent_change_24h'],
                    'percent_change_7d': coin['quote']['USD']['percent_change_7d'],
                    'market_cap': coin['quote']['USD']['market_cap'],
                    'market_cap_dominance': coin['quote']['USD']['market_cap_dominance'],
                    'fully_diluted_market_cap': coin['quote']['USD']['fully_diluted_market_cap'], 
                    'last_updated': coin['quote']['USD']['last_updated']
                    }
                )
                start += 1  
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)
        
    return data


# ---------------------------------------------------------------#
# CREATE TABLE & SAVE DATA TO PSQL
# ---------------------------------------------------------------#

cmc_price ='''
    CREATE TABLE IF NOT EXISTS cmc_price(
        id TEXT PRIMARY KEY,
        source TEXT,
        symbol TEXT,
        slug TEXT, 
        max_supply text,
        circulating_supply TEXT,
        total_supply TEXT,
        price DECIMAL,
        volume_24h TEXT,
        volume_change_24h text,
        percent_change_1h DECIMAL,
        percent_change_24h DECIMAL,
        percent_change_7d DECIMAL,
        market_cap text,
        market_cap_dominance DECIMAL,
        fully_diluted_market_cap text,
        last_updated TIMESTAMP,
        timestamp Integer
        
    )
'''

cols = [ "source",
        "symbol",
        "slug", 
        "max_supply",
        "circulating_supply",
        "total_supply",
        "price",
        "volume_24h",
        "volume_change_24h",
        "percent_change_1h",
        "percent_change_24h",
        "percent_change_7d",
        "market_cap",
        "market_cap_dominance",
        "fully_diluted_market_cap",
        "last_updated"
        "timestamp",
        "id" ]

 
def make_df(data):
    df = pd.DataFrame(data).fillna(0)
    #make this lamda
    df['last_updated'] = pd.to_datetime(df['last_updated']).dt.strftime('%Y-%m-%dT%H:%M.%fZ')
    for index, row in df.iterrows():
        UTC = datetime.strptime(row['last_updated'], '%Y-%m-%dT%H:%M.%fZ')
        epoch_time = int((UTC- datetime(1970, 1, 1)).total_seconds())
        df['timestamp'] = epoch_time

    df['id'] = 'CMC-' + df['symbol'] + '-' + df['last_updated']
    return df


if __name__ == '__main__':
    db.drop_table('cmc_price')
    db.create_table(cmc_price)
    data = getpairs()
    df = make_df(data)
    db.add_metrics_local(df, "cmc_price")

