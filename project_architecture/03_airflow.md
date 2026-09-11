# Airflow orkestrasyonu

## DAG özeti

DAG adı `fast_orderbook_market_pipeline` ve çalışma aralığı 15 dakikadır:

```text
bronze_to_silver
        |
        v
silver_to_gold
        |
        v
gold_to_postgres
```

Görevler `BashOperator` ile Python modüllerini çağırır:

| Task | Çalıştırılan modül |
|---|---|
| `bronze_to_silver` | `fast_orderbook.processing.spark` |
| `silver_to_gold` | `fast_orderbook.analytics.runner` |
| `gold_to_postgres` | `fast_orderbook.serving.postgres` |

## Zamanlama ve hata davranışı

- Schedule: `*/15 * * * *`
- `catchup=False`: Geçmişte kaçan periyotları geriye dönük çalıştırmaz.
- `max_active_runs=1`: Aynı DAG'ın iki çalışmasının çakışmasını engeller.
- Her task için 2 retry ve retry'lar arasında 1 dakika bekleme vardır.

Task'ler sıralı olduğu için Silver başarısızsa Gold, Gold başarısızsa PostgreSQL yüklemesi başlamaz.

## Container yapısı

Airflow imajı:

- `apache/airflow:3.1.0` tabanını kullanır.
- PySpark için OpenJDK 17 kurar.
- Projenin `platform` bağımlılıklarını imaja kurar.
- Kaynak kodu ve `data/` klasörünü host üzerinden container'a bağlar.
- Projeyi container içinde `/opt/fast-orderbook` altında görür.

`docker-compose.yml`, geliştirme ortamında tek Kafka broker, tek PostgreSQL ve Airflow standalone
servisi çalıştırır.

## İdempotentlik durumu

- Silver çıktıları `overwrite` ile yeniden oluşturulur.
- Gold Parquet önce silinir, sonra yeniden yazılır.
- PostgreSQL yüklemesi birincil anahtar çakışmasında `ON CONFLICT DO UPDATE` kullanır.

Son adım tekrar çalışmaya uygundur. Fakat Gold dosyasının silinmesi ile yeni dosyanın tamamlanması
arasındaki hata, geçici olarak Gold çıktısını yok bırakabilir.

## Eksik gözlem ve kalite adımları

DAG şu anda yalnızca süreçlerin exit code'una bakar. Şunlar ayrı task veya kontrol olarak yoktur:

- Bronze verisinin gerçekten geldiğini doğrulama
- Satır sayısı ve null oranı kalite eşikleri
- Silver/Gold çıktısının boş olmadığını kontrol etme
- Gecikme, işlenen kayıt sayısı ve dosya sayısı metriği
- PostgreSQL yüklemesi sonrası doğrulama sorgusu

Bu kontroller, performans optimizasyonundan önce veri hattının doğruluğunu görünür kılar.

