#!/usr/bin/env bash
# Installs the full platform and starts infrastructure, ingestion, API, and UI services.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime"
LOG_DIR="$RUNTIME_DIR/logs"

cd "$ROOT_DIR"


require_command() {
    local command_name="$1"
    local install_hint="$2"

    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "Hata: $command_name bulunamadı. $install_hint"
        exit 1
    fi
}


wait_for_port() {
    local host="$1"
    local port="$2"
    local service_name="$3"

    echo "==> $service_name bekleniyor ($host:$port)..."

    for _attempt in {1..60}; do
        if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
            exec 3>&-
            exec 3<&-
            echo "==> $service_name hazır."
            return
        fi

        sleep 1
    done

    echo "Hata: $service_name 60 saniye içinde hazır olmadı."
    exit 1
}


wait_for_url() {
    local url="$1"
    local service_name="$2"

    for _attempt in {1..30}; do
        if curl --fail --silent "$url" >/dev/null 2>&1; then
            echo "==> $service_name hazır: $url"
            return
        fi

        sleep 1
    done

    echo "Hata: $service_name başlatılamadı. Logları kontrol edin: $LOG_DIR"
    exit 1
}


start_process() {
    local name="$1"
    shift

    local pid_file="$RUNTIME_DIR/$name.pid"
    local log_file="$LOG_DIR/$name.log"
    local existing_pid=""

    if [[ -f "$pid_file" ]]; then
        existing_pid="$(<"$pid_file")"
    fi

    if [[ "$existing_pid" =~ ^[0-9]+$ ]] && kill -0 "$existing_pid" 2>/dev/null; then
        echo "==> $name yeniden başlatılıyor (PID $existing_pid)..."
        kill "$existing_pid"

        for _attempt in {1..10}; do
            if ! kill -0 "$existing_pid" 2>/dev/null; then
                break
            fi

            sleep 1
        done

        if kill -0 "$existing_pid" 2>/dev/null; then
            echo "Hata: $name süreci durdurulamadı (PID $existing_pid)."
            exit 1
        fi
    fi

    echo "==> $name başlatılıyor..."
    nohup "$@" >"$log_file" 2>&1 &

    local process_pid=$!
    echo "$process_pid" >"$pid_file"

    sleep 1

    if ! kill -0 "$process_pid" 2>/dev/null; then
        echo "Hata: $name başlatılamadı."
        tail -n 20 "$log_file" || true
        exit 1
    fi

    echo "==> $name çalışıyor (PID $process_pid)."
}


echo "==> MarketStream kurulumu"

require_command python3.11 "Python 3.11 kurun."
require_command java "OpenJDK 17 kurun: sudo apt install openjdk-17-jdk"
require_command docker "Docker ve Docker Compose eklentisini kurun."
require_command curl "curl paketini kurun."

if ! docker info >/dev/null 2>&1; then
    echo "Hata: Docker daemon çalışmıyor veya mevcut kullanıcı Docker'a erişemiyor."
    exit 1
fi

JAVA_BINARY="$(command -v java)"
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$JAVA_BINARY")")")"
export PATH="$JAVA_HOME/bin:$PATH"

echo "==> JAVA_HOME=$JAVA_HOME"

if [[ ! -d ".venv" ]]; then
    echo "==> Python sanal ortamı oluşturuluyor..."
    python3.11 -m venv .venv
fi

echo "==> Proje, API ve UI bağımlılıkları kuruluyor..."
.venv/bin/python -m pip install -e ".[api,platform,ui]"

mkdir -p "$LOG_DIR"

echo "==> Kafka, PostgreSQL ve Airflow başlatılıyor..."
docker compose up -d --build

wait_for_port localhost 9092 Kafka
wait_for_port localhost 5432 PostgreSQL

start_process \
    bronze-consumer \
    .venv/bin/python -u -m fast_orderbook.ingestion.runner

start_process \
    market-producer \
    .venv/bin/python -u -m fast_orderbook.ingestion.producer

start_process \
    api \
    .venv/bin/python -m uvicorn \
    fast_orderbook.serving.api:app \
    --host 0.0.0.0 \
    --port 8000

start_process \
    ui \
    .venv/bin/python -m streamlit run \
    src/fast_orderbook/ui/ui.py \
    --server.address 0.0.0.0 \
    --server.port 8501 \
    --server.headless true \
    --browser.gatherUsageStats false

wait_for_url http://localhost:8000/health FastAPI
wait_for_url http://localhost:8501/_stcore/health Streamlit

docker compose ps

echo
echo "==> Kurulum tamamlandı."
echo "    UI:      http://localhost:8501"
echo "    API:     http://localhost:8000/docs"
echo "    Airflow: http://localhost:8080"
echo "    Loglar:  $LOG_DIR"

if [[ -n "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ]] && command -v xdg-open >/dev/null 2>&1; then
    nohup xdg-open http://localhost:8501 >/dev/null 2>&1 || true
fi
