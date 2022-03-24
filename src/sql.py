trending_price ='''
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
        count >= 100 ORDER BY sma1_dif DESC LIMIT 10
    ), 


PRICE_DATA AS ( 
    SELECT 
        symbol,
        price,
        timestamp 
    FROM 
        cmc_price
    ),

RANKED_PRICE AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY symbol ORDER BY timestamp DESC, price DESC) AS latest
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
        *
    FROM
        Top_10
    LEFT JOIN 
        LATEST_PRICES
    ON
        Top_10.symbol = LATEST_PRICES.symbol
)
SELECT * FROM JOIN_DATA 
'''




