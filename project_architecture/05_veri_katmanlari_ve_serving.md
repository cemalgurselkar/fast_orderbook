# Veri katmanları ve sunum

## Bronze

Bronze, normalize edilmiş Kafka event'lerinin Parquet karşılığıdır. Event türü, sembol ve UTC tarih
klasörleriyle ayrılır. ZSTD sıkıştırması depolama boyutunu azaltır.

Bronze veri ham Binance JSON'u değildir; alanlar daha önce dataclass modellerine çevrilmiş ve
sayısal tipler normalize edilmiştir.

## Silver

Silver, PySpark tarafından doğrulanmış ve tekilleştirilmiş beş dataset içerir. Bu katman analize
uygun temiz veriyi temsil eder, fakat mevcut uygulamada her çalışmada tamamen yeniden yazılır.

## Gold

DuckDB yalnızca Silver trades verisini kullanır ve bir dakikalık özet üretir:

- İşlem sayısı
- Toplam miktar
- Quote hacmi
- Minimum, maksimum ve ortalama fiyat
- VWAP

Çıktı `data/gold/market_summary_1m.parquet` dosyasıdır. Ardından PostgreSQL'deki
`market_summary_1m` tablosuna upsert edilir.

## Tarihsel API

`GET /markets/{symbol}/summary`, PostgreSQL'den seçilen sembolün son 1-1000 dakikalık özetini
okur. Her istekte yeni bir psycopg bağlantısı açılır.

## Canlı API

Canlı consumer, Kafka event'lerini `LiveMarketHub` içine aktarır. Her sembol için şunlar tutulur:

- Son fiyat
- Son book ticker'dan best bid ve ask
- Son depth mesajından ilk 10 bid ve ask
- Son 50 trade
- Son event türü ve alınma zamanı

`GET /markets/{symbol}/live` bu durumun snapshot'ını döndürür. WebSocket endpoint'i önce varsa bir
snapshot, ardından yeni event'leri gönderir.

Her WebSocket abonesi için sınırlı bir `asyncio.Queue` vardır. Kuyruk dolduğunda en eski event
atılır; bu davranış yavaş istemcinin belleği sınırsız büyütmesini engeller fakat tüm mesajların
teslimini garanti etmez.

## Order book hakkında önemli ayrım

Mevcut `bids` ve `asks`, tam order book değildir. Binance depth stream'inden gelen son delta
mesajının ilk 10 seviyesidir. Doğru bir order book oluşturmak için REST snapshot alınması, update ID
sırasının doğrulanması ve depth delta'larının yerel deftere uygulanması gerekir.

## Streamlit

Streamlit doğrudan Binance veya Kafka'ya bağlanmaz. Canlı snapshot'ı FastAPI REST endpoint'inden
poll eder; tarihsel grafikleri PostgreSQL tabanlı summary endpoint'inden alır. WebSocket şu anda UI
tarafından değil, harici istemciler tarafından kullanılabilir.

