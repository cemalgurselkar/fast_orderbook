CREATE OR REPLACE VIEW bronze_trades AS

SELECT *
FROM read_parquet(
    '{parquet_glob}',
    union_by_name=True
);