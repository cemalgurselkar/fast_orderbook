# Proje mimarisi notları

Bu klasör, MarketStream kodunun çalışma biçimini kısa ve kodla uyumlu biçimde açıklar.

## Okuma sırası

1. [Genel mimari](01_genel_mimari.md)
2. [Kafka ve veri alımı](02_kafka_ve_ingestion.md)
3. [Airflow orkestrasyonu](03_airflow.md)
4. [PySpark ve Silver katmanı](04_pyspark_silver.md)
5. [Veri katmanları ve sunum](05_veri_katmanlari_ve_serving.md)
6. [Optimizasyon yol haritası](06_optimizasyon_yol_haritasi.md)

## Tek cümlede sistem

Binance WebSocket verisi normalize edilip Kafka'ya yazılır; bir tüketici Bronze Parquet dosyalarını,
ayrı bir tüketici canlı API durumunu besler; Airflow ise Bronze → Silver → Gold → PostgreSQL
zincirini 15 dakikada bir çalıştırır.

## Temel giriş noktaları

| İş | Modül |
|---|---|
| Binance → Kafka | `fast_orderbook.ingestion.producer` |
| Kafka → Bronze | `fast_orderbook.ingestion.runner` |
| Bronze → Silver | `fast_orderbook.processing.spark` |
| Silver → Gold | `fast_orderbook.analytics.runner` |
| Gold → PostgreSQL | `fast_orderbook.serving.postgres` |
| REST ve WebSocket | `fast_orderbook.serving.api` |
| Streamlit | `fast_orderbook.ui.ui` |

