# Docker notları

## Docker bu projede ne yapıyor?

Docker, MarketStream'in dış servislerini aynı geliştirme ortamında çalıştırıyor:

- Kafka
- PostgreSQL
- Airflow

Binance producer, Bronze consumer, FastAPI ve Streamlit ise container içinde değil, `setup.sh`
tarafından bilgisayardaki `.venv` üzerinden ayrı süreçler olarak başlatılıyor.

```text
Docker container'ları                 Host süreçleri
---------------------                 --------------
Kafka :9092        <----------------  Producer ve consumer
PostgreSQL :5432   <----------------  FastAPI
Airflow :8080                         Batch pipeline
                                      Streamlit :8501
                                      FastAPI :8000
```

## Temel kavramlar

### Image

Bir servisin çalışması için gereken işletim sistemi katmanlarını, programları ve bağımlılıkları
içeren salt okunur şablondur. Örneğin proje `apache/kafka:4.1.0` ve `postgres:17` hazır image'larını
kullanır.

### Container

Image'ın çalışan örneğidir. `fast_orderbook_kafka`, Kafka image'ından oluşturulan container adıdır.
Container silinebilir; kalıcı tutulması gereken veriler volume içinde saklanır.

### Dockerfile

Yeni bir image'ın nasıl üretileceğini tarif eder. Projede özel Dockerfile yalnızca Airflow içindir:

```text
docker/airflow/Dockerfile
```

### Docker Compose

Birden fazla container'ı tek YAML dosyasında birlikte tanımlar. Bu projedeki
`docker-compose.yml`, Kafka, Airflow ve PostgreSQL servislerinin portlarını, ortam değişkenlerini,
volume'larını ve başlatma komutlarını bir araya getirir.

## Airflow Dockerfile satırlarının anlamı

```dockerfile
FROM apache/airflow:3.1.0
```

Hazır Airflow image'ı temel alınır.

```dockerfile
USER root
RUN apt-get ... openjdk-17-jre-headless
```

Sistem paketi kurmak için geçici olarak root kullanılır. Java, PySpark'ın çalışması için gereklidir.

```dockerfile
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

Spark'ın Java kurulumunu bulmasını sağlar.

```dockerfile
COPY pyproject.toml ...
COPY src ...
RUN pip install --no-cache-dir "/opt/fast-orderbook[platform]"
```

Paket tanımı ve kaynak kod image'a kopyalanır; Spark, DuckDB, PyArrow ve PostgreSQL gibi platform
bağımlılıkları kurulur. `--no-cache-dir`, pip indirme cache'inin image boyutunu büyütmesini engeller.

Kurulumdan önce tekrar `USER airflow` kullanıldığı için Airflow root yetkisiyle çalışmaz.

## `docker-compose.yml` yapısı

### Kafka servisi

Kafka tek node üzerinde hem broker hem controller rolünde KRaft modunda çalışır. Ayrı ZooKeeper
yoktur.

```yaml
ports:
  - "9092:9092"
```

Soldaki port bilgisayarın, sağdaki port container'ın portudur. Host üzerinde çalışan Python
süreçleri Kafka'ya `localhost:9092` ile bağlanır.

Replication değerlerinin `1` olması tek broker'lı geliştirme ortamı için gereklidir. Broker
kaybedilirse başka replica olmadığı için yüksek erişilebilirlik sağlanmaz.

### PostgreSQL servisi

PostgreSQL şu geliştirme bilgileriyle başlar:

- Veritabanı: `fast_orderbook`
- Kullanıcı: `fast_orderbook`
- Port: `5432`

`postgres_data` bir **named volume**'dur. Container yeniden oluşturulsa bile veritabanı dosyaları
bu volume içinde kalır.

Healthcheck, `pg_isready` ile PostgreSQL'in bağlantı kabul edip etmediğini kontrol eder.

### Airflow servisi

Airflow hazır image yerine projedeki Dockerfile ile build edilir:

```yaml
build:
  context: .
  dockerfile: docker/airflow/Dockerfile
```

`context: .`, Docker build sırasında proje kökünün build context olarak gönderildiği anlamına gelir.

Airflow içindeki önemli yollar:

| Host | Container | Amaç |
|---|---|---|
| `./airflow/dags` | `/opt/airflow/dags` | DAG dosyaları |
| `./airflow/logs` | `/opt/airflow/logs` | Task logları |
| `./airflow/plugins` | `/opt/airflow/plugins` | Airflow eklentileri |
| `./src` | `/opt/fast-orderbook/src` | Güncel Python kodu |
| `./data` | `/opt/fast-orderbook/data` | Bronze, Silver ve Gold |

Bunlar **bind mount**'tur: host klasörü ile container yolu doğrudan eşleşir. Örneğin Airflow'un
ürettiği Silver dosyaları bilgisayardaki `data/silver` altında görünür.

Airflow container'ı başlarken önce metadata migration, sonra standalone sunucu çalıştırılır:

```text
airflow db migrate -> airflow standalone
```

`POSTGRES_DSN`, Gold verisini proje PostgreSQL'ine yükleyen Python kodu içindir. Airflow metadata
veritabanı ayrıca PostgreSQL'e yönlendirilmemiştir.

## Container ağı

Compose servisleri otomatik olarak aynı Docker ağına bağlar. Container'lar birbirine servis adıyla
ulaşabilir:

```text
postgres:5432
kafka:9092
```

Host üzerinde çalışan süreçler ise yayınlanan portları kullanır:

```text
localhost:5432
localhost:9092
```

Mevcut Kafka `advertised.listeners` değeri `localhost:9092` olduğu için host istemcileriyle
uyumludur. İleride Kafka'ya başka container'lardan bağlanılacaksa container içi listener ayrıca
tanımlanmalıdır; container içindeki `localhost`, Kafka container'ını değil ilgili container'ın
kendisini ifade eder.

## `depends_on` ne sağlar?

Airflow için Kafka ve PostgreSQL bağımlılığı belirtilmiştir. Bu, container'ların başlatılma sırasını
düzenler; servislerin tamamen hazır hale geldiğini tek başına garanti etmez. PostgreSQL healthcheck
tanımlı olsa da Airflow bağımlılığında `condition: service_healthy` kullanılmıyor.

`setup.sh`, host süreçlerini başlatmadan önce Kafka ve PostgreSQL portlarını ayrıca bekler.

## `setup.sh` ile ilişki

Şu komut:

```bash
docker compose up -d --build
```

- Gerekirse Airflow image'ını yeniden build eder.
- Üç container'ı arka planda başlatır.
- Mevcut container varsa yapılandırmaya göre yeniden kullanır veya oluşturur.

Ardından `setup.sh`, host tarafındaki producer, consumer, API ve UI süreçlerini başlatır.

## Sık kullanılan komutlar

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f airflow
docker compose logs -f kafka
docker compose logs -f postgres
docker compose restart airflow
docker compose down
```

`docker compose down`, container ve Compose ağını kaldırır; named volume içindeki PostgreSQL verisi
kalır. `docker compose down -v` volume'u da siler ve PostgreSQL verisini kaybettirir.

## Kod değişince neyi yeniden build etmek gerekir?

- `src/` ve DAG değişiklikleri bind mount sayesinde container'a doğrudan yansır.
- `pyproject.toml`, Dockerfile veya sistem bağımlılığı değişirse Airflow image'ı yeniden build
  edilmelidir.
- Kafka/PostgreSQL image sürümü ya da Compose ayarı değişirse ilgili container yeniden oluşturulur.

## Mevcut geliştirme sınırları

- Kafka ve PostgreSQL portları host'a açıktır.
- Kullanıcı adı ve parola geliştirme amacıyla dosyada açık biçimde bulunur.
- Kafka PLAINTEXT kullanır; TLS ve kimlik doğrulama yoktur.
- Kafka tek broker, PostgreSQL tek instance olarak çalışır.
- Projede `.dockerignore` bulunmadığı için tüm proje build context'e girebilir; büyük `data/` ve log
  klasörleri build süresini artırabilir.
- Airflow standalone modu geliştirme içindir.

Bu ayarlar yerel öğrenme ve geliştirme için uygundur; doğrudan production güvenlik veya yüksek
erişilebilirlik yapılandırması değildir.
