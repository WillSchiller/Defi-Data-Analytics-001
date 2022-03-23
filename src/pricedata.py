from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

import json
from dotenv import load_dotenv
import os



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
                    {'symbol':coin['symbol'], 
                    'name':coin['name'], 
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


if __name__ == '__main__':
    d = getpairs()
    print(d)