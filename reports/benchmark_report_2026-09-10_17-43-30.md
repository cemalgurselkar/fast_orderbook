# Fast-Orderbook Benchmark Report

Generated: 2026-09-10T17:43:30+03:00

## Environment

- CPU: Intel(R) Core(TM) i5-10300H CPU @ 2.50GHz
- Logical CPUs: 8
- Memory: 23Gi
- Kernel: 6.8.0-138-generic
- Python: Python 3.11.15
- Benchmark duration per workload: 15s
- Kafka partitions per benchmark topic: 8
- Consumer queue capacity: 10,000
- Parquet batch size: 1,000
- Parquet compression: ZSTD
- Latency sampling: 1 / 100 events

## Results

| Target eps | Producer eps | Written eps | Produced | Received | Written | Failed | p50 ms | p95 ms | p99 ms | CPU | Max RSS MB | Max Queue | Producer Queue Full |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|

## Failure

Benchmark process failed at target rate:

`50000 events/sec`

Exit code:

`1`
