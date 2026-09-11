# Kafka ve veri alımı

## Topic düzeni

Event sınıfı, `TOPICS` eşlemesiyle bir Kafka topic'ine yönlendirilir:

| Event | Topic |
|---|---|
| Trade | `market.trades` |
| Depth | `market.depth` |
| Book ticker | `market.book_ticker` |
| 24 saat ticker | `market.ticker` |
| 1 dakikalık kline | `market.klines` |

Mesaj anahtarı `symbol`, değer ise `orjson` ile serileştirilmiş event sözlüğüdür. Aynı sembolün
mesajları, topic partition sayısı artırıldığında aynı partition'a yönelme eğilimindedir ve partition
içindeki sıra korunur.

## Producer davranışı

`KafkaMarketProducer` ayarları:

- `linger.ms = 5`: Daha iyi batch oluşturmak için çok kısa bekleme.
- `batch.num.messages = 10_000`: Producer batch üst sınırı.
- Her publish sonrasında `poll(0)`: Teslim callback'lerinin ilerlemesini sağlar.
- Kapanışta `flush()`: Yerel kuyruktaki mesajların gönderilmesini bekler.

Binance bağlantısı koparsa istemci 1 saniyeden başlayıp 30 saniyeye kadar çıkan exponential
backoff ile yeniden bağlanır.

## İki bağımsız consumer group

| Amaç | Consumer group |
|---|---|
| Bronze Parquet yazımı | `marketstream-bronze-writer` |
| Canlı API durumu | `marketstream-live-serving` |

Farklı group ID kullanımı kritiktir. Aynı group kullanılsaydı mesajlar iki servis arasında
paylaştırılır ve her servis tüm event'leri göremezdi.

Consumer, beş topic'e birden abone olur. `consume()` bloklayan bir kütüphane çağrısı olduğu için
`asyncio.to_thread` ile event loop dışında çalıştırılır. Tek çağrıda en fazla 5.000 mesaj ve 100 ms
timeout kullanılır.

## Bronze yazım yolu

```text
Kafka batch
   -> asyncio.Queue
   -> topic + symbol tamponu
   -> 10.000 event veya 2 saniye
   -> PyArrow Parquet, ZSTD
```

Dosya düzeni:

```text
data/bronze/binance/{dataset}/{symbol}/{UTC-tarih}/{benzersiz-dosya}.parquet
```

Tamponlar `(topic, symbol)` anahtarıyla ayrılır. Böylece farklı veri türleri ve semboller aynı
Parquet dosyasına karışmaz.

## Teslimat garantisinin mevcut sınırı

Consumer'da `enable.auto.commit=True` kullanılıyor. Offset, Parquet yazımının başarıyla
tamamlandığı işleme bağlı değil. Süreç yanlış anda kapanırsa commit edilmiş fakat diske yazılmamış
mesaj riski vardır. Bu nedenle mevcut tasarım tam olarak "at-least-once" veya "exactly-once"
garantisi vermez.

## İncelerken önemli noktalar

- `queue_maxsize=10_000`, event değil **Kafka batch sayısını** sınırlar.
- Parquet yazımı senkron olduğu için yazım sırasında event loop kısa süreli bloke olabilir.
- Producer teslim hatalarını izleyen callback veya başarısız mesaj sayacı yoktur.
- Topic ve partition sayıları kod tarafından açıkça oluşturulmuyor; broker varsayımlarına bağlıdır.

