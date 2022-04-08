# ------------ SQL QUERIES FOR queries.py ------------------- # 

def orderby(table):
    if table == 'trending':
        return 'sma1_dif DESC '
    elif table == 'volatile':
        return 'rsd_7 DESC'
    else:
        return 'sma1_dif ASC'

def create_sql(table):

    order_by = orderby(table)

    sql_get =f'''
    with

    Top_10 AS (
        SELECT
            *
        FROM 
            rsd_metrics 
        WHERE 
            symbol != 'NOT_A_SYMBOL' 
        AND 
            timestamp >= (SELECT MAX(timestamp) FROM rsd_metrics)
        AND 
            count >= 100 
            ORDER BY 
                {order_by}
            LIMIT 10
        ), 


    PRICE_DATA AS ( 
        SELECT 
            symbol,
            price,
            percent_change_24h,
            timestamp 
        FROM 
            cmc_price
        ),

    RANKED_PRICE AS (
        SELECT 
            *,
            RANK() OVER (PARTITION BY symbol ORDER BY timestamp DESC, price DESC, percent_change_24h DESC) AS latest
        FROM
            PRICE_DATA
        ),			


    LATEST_PRICES AS (
            SELECT *
            FROM RANKED_PRICE
            WHERE latest = 1
    ), 

    JOIN_DATA AS (
        SELECT
            Top_10.id as id,
            Top_10.timestamp as timestamp,
            Top_10.date as date, 
            Top_10.symbol as symbol,
            Top_10.count as count,
            Top_10.symbol_sma1 as symbol_sma1,
            Top_10.sma1_dif as sma1_dif,
            Top_10.symbol_sma7 as symbol_sma7,
            Top_10.sma7_dif as sma7_dif,
            Top_10.sentiment as sentiment,
            Top_10.rsd_1 as rsd_1,
            Top_10.rsd_7 as rsd_7,
            coalesce(LATEST_PRICES.price, 0) as price,
            coalesce(LATEST_PRICES.percent_change_24h, 0) as percent_change_24h
        FROM
            Top_10
        LEFT JOIN 
            LATEST_PRICES
        ON
            Top_10.symbol = LATEST_PRICES.symbol
    )
    SELECT * FROM JOIN_DATA 
    ORDER BY {order_by}
    '''

    sql_create =f'''
    CREATE TABLE IF NOT EXISTS {table}(
        id text PRIMARY KEY, 
        timestamp INTEGER, 
        date TIMESTAMP, 
        symbol text,
        count FLOAT8, 
        symbol_sma1 FLOAT8,
        sma1_dif FLOAT8, 
        symbol_sma7 FLOAT8, 
        sma7_dif FLOAT8,
        sentiment FLOAT8,
        rsd_1 FLOAT8,
        rsd_7 FLOAT8,
        price FLOAT8,
        percent_change_24h FLOAT
        )
    '''

    cols = ["id", "timestamp", "date", "symbol", "count", "symbol_sma1", "sma1_dif", "symbol_sma7", "sma7_dif", "sentiment", "rsd_1", "rsd_7", "price", "percent_change_24h"]

    return (sql_get, sql_create, table, cols)





