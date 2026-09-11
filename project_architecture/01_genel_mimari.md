# Genel mimari

## Veri akışı

```text
Binance WebSocket
       |
       v
BinanceClient: JSON mesajı -> Python event modeli
       |
       v
Kafka: event türüne göre 5 topic
       |
       |-------------------------------|
       v                               v
Bronze consumer                  Live consumer
       |                               |
       v                               v
Parquet / Bronze                 Bellek içi snapshot
       |                               |
       v                               v
PySpark / Silver                 REST + WebSocket
       |                               |
       v                               v
DuckDB / Gold                    Streamlit ve istemciler
       |
       v
PostgreSQL -> REST -> Streamlit
```

Kafka'dan sonra sistem iki kola ayrılır:

- **Kalıcı veri kolu:** Veriyi Bronze, Silver ve Gold katmanlarından geçirir.
- **Canlı sunum kolu:** Veriyi diske uğratmadan FastAPI'nin bellek içi durumuna aktarır.

Bu ayrım, UI gecikmesini batch veri hattından bağımsız tutar. İki kol farklı Kafka consumer
group kullandığı için aynı mesajı birbirinden bağımsız okuyabilir.

## Kaynaktan modele

`BinanceClient`, varsayılan olarak 10 sembol ve 5 event türü için birleşik Binance WebSocket
bağlantısı oluşturur. Gelen JSON alanları şu değişmez dataclass modellerine çevrilir:

- `TradeEvent`
- `DepthEvent`
- `BookTickerEvent`
- `TickerEvent`
- `KlineEvent`

Sayısal metinlerin `float`, zamanların milisaniye `int` olması sonraki katmanların Binance'ın kısa
alan adlarına bağımlılığını azaltır.

## Airflow'un yeri

Airflow canlı akışı başlatmaz. Canlı producer ve Bronze consumer uzun süre çalışan ayrı
süreçlerdir. Airflow yalnızca aşağıdaki batch zincirini zamanlar:

```text
Bronze -> Silver -> Gold -> PostgreSQL
```

## SDK ile platform arasındaki fark

`marketstream.MarketStream`, Binance istemcisini doğrudan kullanır; Kafka, Spark ve PostgreSQL
gerektirmez. Platform modu ise veriyi saklamak, işlemek, API ve UI üzerinden sunmak içindir.

## Mevcut ölçek modeli

- Tek Binance producer süreci
- Tek Kafka broker
- Topic'ler için kodda özel partition tanımı yok
- Bronze yazımı tek süreçte
- Spark `local[*]` modunda
- Canlı durum tek FastAPI sürecinin belleğinde

Bu yapı geliştirme bilgisayarı ve mimariyi öğrenmek için uygundur; dağıtık üretim sistemi değildir.

