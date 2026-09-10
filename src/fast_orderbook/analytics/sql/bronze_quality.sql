WITH duplicate_keys AS (
    SELECT 
        exchange,
        symbol,
        trade_id,
        COUNT(*) AS duplicate_count
    FROM bronze_trades
    GROUP BY exchange, symbol, trade_id
    HAVING COUNT(*) > 1
)

SELECT 
    COUNT(*) AS total_rows,

    COUNT(*) FILTER (
        WHERE exchange IS NULL
        OR TRIM(exchange) = ''
        OR symbol IS NULL
        OR TRIM(symbol) = ''
        OR trade_id IS NULL
        OR event_time_ms IS NULL
        OR trade_time_ms IS NULL
    ) AS missing_values,

    COUNT (*) FILTER (
        WHERE price is NULL OR price <= 0
    ) AS invalid_price_rows,

    COUNT (*) FILTER (
        WHERE quantity is NULL OR quantity <= 0
    ) AS invalid_quantity_rows,

    COUNT(*) FILTER (
        WHERE event_time_ms <= 0
        OR trade_time_ms <= 0
    ) AS invalid_timestamp_rowss,
    
    (
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM duplicate_keys
    ) AS duplicate_rows

FROM bronze_trades;