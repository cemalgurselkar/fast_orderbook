COPY (
    SELECT 
        exchange,
        symbol,
        
        time_bucket(
            INTERVAL '1 minute',
            to_timestamp(event_time_ms / 1000.0)
        ) AS window_start,

        COUNT (*) AS trade_count,

        SUM (quantity) AS total_quantity,
        SUM (price * quantity) AS quote_volume,

        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(price) AS avg_price,

        SUM(price * quantity) / 
        NULLIF(SUM(quantity), 0) AS vwap

    FROM read_parquet(
        '{silver_glob}',
        union_by_name= true
    )

    GROUP BY
        exchange,
        symbol,
        window_start
    
    ORDER BY
        window_start
)
TO '{gold_output}'
(
    FORMAT "parquet",
    COMPRESSION "zstd"
);