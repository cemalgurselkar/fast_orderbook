#!/usr/bin/env bash

set -e

echo "==> fast-orderbook setup"

# Java / Spark
if ! command -v java >/dev/null 2>&1; then
    echo "Java bulunamadı. Önce OpenJDK 17 kur:"
    echo "sudo apt install openjdk-17-jdk"
    exit 1
fi

export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"
export PATH="$JAVA_HOME/bin:$PATH"

echo "==> JAVA_HOME=$JAVA_HOME"

# Python virtual environment
if [ ! -d ".venv" ]; then
    echo "==> Creating virtual environment..."
    python3.11 -m venv .venv
fi

source .venv/bin/activate

# Python dependencies
echo "==> Installing project dependencies..."
python -m pip install -e .

# Docker infrastructure
echo "==> Starting Docker services..."
docker compose up -d

docker compose ps

echo "==> Setup complete."