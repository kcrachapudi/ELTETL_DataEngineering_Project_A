#!/usr/bin/env bash
# setup.sh — spin up the full local environment from scratch
# Usage: bash setup.sh

set -euo pipefail

echo ""
echo "=========================================="
echo "  Project 1 — Local Environment Setup"
echo "=========================================="
echo ""

# ── Check prerequisites ────────────────────────────────────────────────────
check_cmd() {
    if ! command -v "$1" &>/dev/null; then
        echo "ERROR: $1 is not installed. Please install it first."
        exit 1
    fi
}

check_cmd python3
check_cmd docker
check_cmd pip3

echo "[1/5] Checking Python version..."
python3 -c "
import sys
v = sys.version_info
if v < (3, 10):
    print(f'ERROR: Python 3.10+ required, got {v.major}.{v.minor}')
    sys.exit(1)
print(f'  Python {v.major}.{v.minor}.{v.micro} — OK')
"

# ── Python dependencies ────────────────────────────────────────────────────
echo ""
echo "[2/5] Installing Python dependencies..."
pip3 install -r requirements.txt -q
echo "  Dependencies installed — OK"

# ── Environment file ───────────────────────────────────────────────────────
echo ""
echo "[3/5] Setting up environment config..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "  Created .env from .env.example"
    echo "  IMPORTANT: Edit .env and set your credentials before running the pipeline."
else
    echo "  .env already exists — skipping"
fi

# ── Docker / PostgreSQL ────────────────────────────────────────────────────
echo ""
echo "[4/5] Starting PostgreSQL via Docker..."
docker compose up -d postgres
echo "  Waiting for PostgreSQL to be healthy..."

# Poll until healthy
MAX_WAIT=30
WAITED=0
until docker compose exec -T postgres pg_isready -U pipeline_user -d pipeline_db &>/dev/null; do
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "ERROR: PostgreSQL did not become healthy within ${MAX_WAIT}s"
        exit 1
    fi
    sleep 1
    WAITED=$((WAITED + 1))
done
echo "  PostgreSQL is healthy — OK"

# ── Smoke test ────────────────────────────────────────────────────────────
echo ""
echo "[5/5] Running smoke tests..."
python3 -c "
from parsers import EDIParser, EDI834Parser, EDI835Parser, EDI837Parser, EDI270Parser, EDI271Parser
from parsers.json_parser import JSONParser
from parsers.csv_parser import CSVParser
from parsers.xml_parser import XMLParser
print('  All parsers import correctly — OK')
"

echo ""
echo "=========================================="
echo "  Setup complete!"
echo ""
echo "  PostgreSQL:  localhost:5432 / pipeline_db"
echo "  API server:  run.sh to start"
echo "  pgAdmin:     docker compose --profile debug up"
echo "=========================================="
echo ""
