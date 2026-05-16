#!/usr/bin/env bash
# teardown.sh — stop and clean up the local environment
# Usage:
#   bash teardown.sh          stop containers, keep data
#   bash teardown.sh --full   stop containers AND delete all data volumes

set -euo pipefail

MODE=${1:-""}

echo ""
echo "=========================================="
echo "  Project 1 — Teardown"
echo "=========================================="
echo ""

if [ "$MODE" == "--full" ]; then
    echo "Full teardown — stopping containers and removing volumes..."
    docker compose down -v
    echo "  Containers stopped and volumes deleted."
    echo ""
    echo "  WARNING: All PostgreSQL data has been deleted."
    echo "  Run setup.sh to start fresh."
else
    echo "Stopping containers (data volumes preserved)..."
    docker compose down
    echo "  Containers stopped. Data volumes intact."
    echo "  Run 'docker compose up -d postgres' to restart."
fi

echo ""
echo "  Done."
echo ""
