COPY (
    WITH validated_trades AS (
        SELECT 
            exchange,
            symbol,
            trade_id,
            price,
            quantity,
            event_time_ms,
            trade_time_ms,
            is_buyer_maker,
        
            ROW_NUMBER() OVER (
                PARTITION BY
                    exchange,
                    symbol,
                    trade_id
                ORDER BY
                    event_time_ms DESC,
                    trade_time_ms DESC
            ) AS row_num
        
        FROM bronze_trades
        WHERE exchange IS NOT NULL
            AND TRIM(exchange) <> ''
            AND symbol IS NOT NULL
            AND TRIM(symbol) <> ''
            
            AND trade_id IS NOT NULL
            
            AND price IS NOT NULL
            AND price > 0

            AND quantity IS NOT NULL
            AND quantity > 0

            AND event_time_ms IS NOT NULL
            AND event_time_ms > 0

            AND trade_time_ms IS NOT NULL
            AND trade_time_ms > 0
    )

    SELECT *
    FROM validated_trades
    WHERE row_num = 1

    ORDER BY
        exchange,
        symbol,
        event_time_ms
)
TO '{silver_output}'
(
    FORMAT "parquet",
    COMPRESSION "zstd"
);