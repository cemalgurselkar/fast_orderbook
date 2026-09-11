# PySpark ve Silver katmanı

## Amaç

`SilverProcessor`, ham Bronze Parquet dosyalarını okuyup temel doğrulama ve tekrar kayıt temizliği
yapar. Spark yerel makinedeki tüm çekirdekleri kullanacak şekilde `local[*]` modunda çalışır.

Bronze okumasında `recursiveFileLookup=true` kullanıldığı için sembol ve tarih alt klasörlerindeki
tüm Parquet dosyaları birlikte okunur.

## Ortak işlemler

Her dataset için önce `exchange` ve `symbol` null kayıtları elenir. Tekrarlı kayıtlarda çoğunlukla
bir Window oluşturulur, anahtar alanlara göre gruplanır ve en yeni event tutulur.

| Dataset | Doğrulama | Tekilleştirme anahtarı |
|---|---|---|
| Trades | Pozitif fiyat, miktar ve zaman | exchange, symbol, trade_id |
| Depth | Geçerli zaman ve update ID aralığı | exchange, symbol, final_update_id |
| Book ticker | Pozitif fiyatlar, geçerli miktarlar | exchange, symbol, update_id |
| Ticker | Pozitif son fiyat, geçerli hacimler | exchange, symbol, event_time_ms |
| Klines | OHLC, zaman, hacim ve işlem sayısı | exchange, symbol, interval, open_time_ms |

Kline aynı mum kapanana kadar tekrar gelebildiği için aynı `open_time_ms` içindeki en güncel hali
tutulur.

## Yazma davranışı

Her dataset ayrı hedefe yazılır:

```text
data/silver/trades
data/silver/depth
data/silver/book_ticker
data/silver/ticker
data/silver/klines
```

Yazma modu `overwrite` olduğu için her 15 dakikalık çalışmada tüm Silver dataset yeniden üretilir.
Bu yaklaşım küçük veri için basit ve anlaşılırdır; veri büyüdükçe önceki bütün tarihçeyi tekrar
okumak ve yazmak maliyetli hale gelir.

## Performans açısından dikkat çekenler

1. `df.write` bir Spark action'dır; hemen ardından kullanılan `df.count()` aynı dönüşümleri yeniden
   hesaplatabilir. Cache/persist veya yazılan çıktının metriğini farklı toplamak bunu azaltabilir.
2. Bronze'daki çok sayıda küçük Parquet dosyası Spark planlama ve dosya açma maliyetini artırır.
3. Şema açıkça verilmediği için Parquet şema çıkarımı yapılır.
4. Silver çıktıları sembol veya tarih ile partition edilmiyor.
5. Her çalışma tüm geçmişi işler; watermark veya son işlenen partition bilgisi yoktur.

İlk optimizasyon hedefi Spark ayarlarını rastgele değiştirmek değil, Spark UI üzerinden scan,
shuffle, task süresi ve dosya sayısını ölçmek olmalıdır.

