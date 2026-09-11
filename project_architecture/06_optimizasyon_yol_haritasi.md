# Optimizasyon yol haritası

Bu sıra, yalnızca events/s değerini artırmak yerine önce veri kaybını ölçmeyi ve darboğazı doğru
katmanda bulmayı amaçlar.

## 1. Ölçümün doğruluğu

- Produced, Kafka'ya teslim edilen, consumed ve Parquet'e yazılan sayıları ayrı ölç.
- Throughput yanında p50/p95/p99 gecikme, CPU, bellek ve disk yazma hızını kaydet.
- ısınma ve kapanış/flush sürelerini sabit bir benchmark protokolüyle ayır.
- Her değişiklikte aynı sembol, payload dağılımı, süre ve Kafka ayarını kullan.

Başarı ölçütü: 50K hedefinde yalnızca üretim hızı değil, alınan ve yazılan kayıt sayıları da hedefle
uyumlu olmalı.

## 2. Kafka doğruluğu ve kapasitesi

- Auto commit yerine Parquet batch başarıyla yazıldıktan sonra kontrollü offset commit değerlendir.
- Producer delivery callback ile hata ve teslim sayılarını ölç.
- Topic partition sayısını açıkça tanımla; sonra consumer paralelliğini partition sayısıyla birlikte
  test et.
- `linger.ms`, Kafka batch boyutu ve consumer fetch ayarlarını benchmark sonucuyla değiştir.

## 3. Bronze yazma yolu

- Senkron PyArrow yazımının event loop üzerindeki etkisini ölç; gerekirse yazımı ayrı thread veya
  worker sürecine taşı.
- Queue sınırının batch sayısı olduğunu dikkate alarak gerçek maksimum bellek kullanımını hesapla.
- 2 saniyelik flush nedeniyle oluşan küçük dosya sayısını izle.
- Daha büyük dosya hedefi ile gecikme arasında kabul edilebilir denge belirle.

## 4. Spark maliyeti

- `write` sonrasındaki `count()` tekrar hesaplamasını kaldır veya ölçümlü cache kullan.
- Tüm geçmişi overwrite etmek yerine yalnızca yeni tarih partition'larını işle.
- Bronze ve Silver verisini sembol/tarih bazında partition etmeyi dene.
- Küçük dosyaları kontrollü biçimde birleştir ve açık Spark şemaları kullan.

## 5. Gold ve PostgreSQL

- Gold çıktısını geçici dosyaya yazıp başarıdan sonra atomik olarak hedefe taşı.
- `executemany` ile satır taşımak büyüdüğünde PostgreSQL `COPY` yaklaşımını karşılaştır.
- API bağlantıları için connection pool değerlendir.
- Gold üretimini yalnızca yeni Silver partition'larıyla incremental hale getir.

## 6. Canlı sunum

- Gerçek order book gerekiyorsa snapshot + sıralı depth delta algoritması ekle.
- Çoklu FastAPI worker kullanılacaksa bellek içi state'in worker'lar arasında paylaşılmadığını
  hesaba kat.
- Streamlit polling ile WebSocket tabanlı güncellemeyi gecikme ve kaynak kullanımı açısından ölç.

## Önerilen çalışma biçimi

Her optimizasyon için tek hipotez kullan:

```text
Darboğaz -> yapılacak tek değişiklik -> ölçülecek metrik -> kabul ölçütü -> sonuç
```

Örneğin: “Parquet yazımı event loop'u bekletiyor” hipotezini doğrulamadan Kafka ayarlarını ve Spark
partition sayısını aynı anda değiştirme. Böylece hangi değişikliğin gerçekten fayda sağladığı
anlaşılır.

