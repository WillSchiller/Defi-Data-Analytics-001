# DEFI DATA ANALYTICS SERVER 001

- This repo contains several data pipelines for [DefiData](https://www.defidata.dev/)
- Functionality includes aggregation of Twitter data & collection and processing of pricing data
- License is GPU 3

## Requirements 

- PostSQL
- CoinMarketCap API key ~~ NOW REDUNDANT
- Python3

## OVERVIEW

- ### Data Processing
    The main script **main.py** copies raw data from the CT-SCANNER on an hourly basis to provide data redundancy of data and decoupled database for analytics. During the copy process the data is cleaned to remove any token symbols that do not match the regex acceptance test or match the token in the predefined list **token.py**. This separation allows us to run complex and experimental analysis on the data while our main services remain isolated from any changes. In the future we will likely look to update this batch process model to something like [Redis Streams](https://redis.io/topics/streams-intro).

    **main.py** should be scheduled to run hourly a little after the hour to allow time for data to be added to the remote server (5 * * * *).

- ### Metrics
    The **db.py** file handles all of the metrics creation and aggregation of data via a mixture of SQL, Pandas & Numpy functions. It should be run hourly to provide updated metrics to the Defi Data frontend. 
- ### API Data 
    The **queries.py** file is a simple script that uses some basic SQL to create top 10 tables for various metrics. These tables are used by the API server to serve data to the frontend. By reducing the data in the tables to just the data we need we make the UI load more quickly. May also move to a caching solution in future.



## QUICK START

- Fill in the env.example with the local and remote PSQL servers credentials.
- If using Linux, schedule your scripts with Crontab. Choose Vim for editor.
```
crontab -e
``` 

```

0 * * * * cd  /{PATH TO FILE}/src && python3 main.py
0 * * * * cd /{PATH TO FILE}/src && python3 db.py
5 * * * * cd /{PATH TO FILE}/src && python3 queries.py

```
*That's it. Everything scheduled to run hourly :)*