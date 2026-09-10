# Fast-Orderbook Benchmark Report

Generated: 2026-09-10T17:44:28+03:00

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
| 50,000 | 50,000 | 11,569 | 749,993 | 691,904 | 691,904 | 0 | 28303.056 | 44820.983 | 45337.008 | 120.1% | 371.7 | 1 | 0 |
| 100,000 | 93,224 | 12,551 | 1,398,365 | 1,282,364 | 1,282,364 | 0 | 72622.684 | 84432.421 | 84920.251 | 117.2% | 694.4 | 1 | 0 |
| 250,000 | 108,698 | 12,770 | 1,630,475 | 1,340,506 | 1,340,506 | 0 | 82047.403 | 87767.187 | 88356.313 | 117.6% | 833.0 | 1 | 0 |
| 500,000 | 120,966 | 12,989 | 1,814,495 | 1,289,052 | 1,289,052 | 0 | 64609.143 | 81701.327 | 82252.740 | 118.5% | 649.6 | 1 | 0 |
| 750,000 | 122,964 | 13,239 | 1,844,467 | 1,282,291 | 1,282,291 | 0 | 56840.584 | 79487.023 | 79765.786 | 118.7% | 587.0 | 1 | 0 |
| 1,000,000 | 124,832 | 12,659 | 1,872,473 | 1,310,000 | 1,310,000 | 0 | 60978.608 | 85887.769 | 86200.947 | 118.4% | 600.3 | 1 | 0 |

## Interpretation

- **Target eps**: requested synthetic input rate.
- **Producer eps**: rate the Python load generator actually submitted.
- **Written eps**: events durably written to benchmark Parquet output divided by measured wall time.
- **Produced / Received / Written** should be compared for loss/backlog analysis.
- **p50/p95/p99** are sampled producer-to-consumer latencies.
- **Max Queue** shows pressure on the bounded in-process queue.
- **Producer Queue Full** shows pressure on librdkafka's producer buffer.

A target workload must not be described as sustained throughput unless the measured producer and writer rates support that claim.

