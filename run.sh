#!/usr/bin/env bash
# run.sh — execute the full Project 1 pipeline end to end
# Usage: bash run.sh
#        bash run.sh --api          (start API server only)
#        bash run.sh --tests        (run all tests)
#        bash run.sh --source edi   (run only EDI ingestion)

set -euo pipefail

MODE=${1:-"pipeline"}

echo ""
echo "=========================================="
echo "  Project 1 Pipeline — $(date '+%Y-%m-%d %H:%M:%S')"
echo "  Mode: $MODE"
echo "=========================================="
echo ""

# Load env vars if .env exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

case "$MODE" in

  --api)
    echo "Starting inbound API server..."
    uvicorn api.inbound_api:app \
        --host "${API_HOST:-0.0.0.0}" \
        --port "${API_PORT:-8000}" \
        --reload \
        --log-level "${LOG_LEVEL:-info}"
    ;;

  --tests)
    echo "Running all test suites..."
    echo ""
    echo "--- EDI parser tests ---"
    python3 tests/test_edi_parser.py
    echo ""
    echo "--- Healthcare EDI tests ---"
    python3 tests/test_healthcare_edi.py
    echo ""
    echo "--- Integration tests ---"
    python3 tests/test_integrations.py
    ;;

  --source)
    SOURCE=${2:-"all"}
    echo "Running ingestion for source: $SOURCE"
    python3 -c "
import logging, sys
logging.basicConfig(level='INFO', format='%(asctime)s %(levelname)s — %(message)s')

source = '$SOURCE'

if source in ('edi', 'all'):
    from parsers import EDIParser
    df = EDIParser().parse('sample_data/sample_850.edi')
    print(f'EDI 850: {len(df)} rows')

if source in ('healthcare', 'all'):
    from parsers import EDI834Parser, EDI837Parser, EDI835Parser
    df834 = EDI834Parser().parse('sample_data/healthcare/sample_834.edi')
    df837 = EDI837Parser().parse('sample_data/healthcare/sample_837p.edi')
    df835 = EDI835Parser().parse('sample_data/healthcare/sample_835.edi')
    print(f'834: {len(df834)} members | 837: {len(df837)} claim lines | 835: {len(df835)} remit lines')

print('Ingestion complete.')
"
    ;;

  pipeline|--pipeline)
    echo "Running full pipeline..."
    python3 -c "
import logging
logging.basicConfig(
    level='INFO',
    format='%(asctime)s %(levelname)s %(name)s — %(message)s'
)
logger = logging.getLogger('pipeline')

# ── Step 1: Extract + Parse ────────────────────────────────────────────
logger.info('Step 1: Extracting and parsing sources...')
from parsers import EDIParser, EDI834Parser, EDI837Parser, EDI835Parser, EDI270Parser, EDI271Parser
from parsers.json_parser import JSONParser
from parsers.csv_parser import CSVParser

results = {}

results['edi_850'] = EDIParser().parse('sample_data/sample_850.edi')
results['edi_856'] = EDIParser().parse('sample_data/sample_856.edi')
results['edi_834'] = EDI834Parser().parse('sample_data/healthcare/sample_834.edi')
results['edi_837'] = EDI837Parser().parse('sample_data/healthcare/sample_837p.edi')
results['edi_835'] = EDI835Parser().parse('sample_data/healthcare/sample_835.edi')

for name, df in results.items():
    logger.info(f'  {name}: {len(df)} rows, {len(df.columns)} columns')

# ── Step 2: Load ────────────────────────────────────────────────────────
logger.info('Step 2: Loading to PostgreSQL...')
try:
    from config.settings import settings
    conn = settings.get_db_connection()
    from loaders.postgres_loader import PostgresLoader
    loader = PostgresLoader(conn)

    load_map = {
        'raw.edi_850':      ('edi_850', ['po_number', 'line_number']),
        'raw.edi_856':      ('edi_856', ['shipment_id', 'hl_number']),
        'raw.edi_834':      ('edi_834', ['member_id', 'coverage_type']),
        'raw.edi_837':      ('edi_837', ['claim_id', 'procedure_code']),
        'raw.edi_835':      ('edi_835', ['claim_id', 'procedure_code']),
    }
    for table, (key, pks) in load_map.items():
        n = loader.load(results[key], table=table.split('.')[1],
                        mode='upsert', primary_keys=pks)
        logger.info(f'  Loaded {n} rows → {table}')
    conn.close()
except Exception as e:
    logger.warning(f'DB load skipped (no DB connection): {e}')

logger.info('Pipeline complete.')
"
    ;;

  *)
    echo "Unknown mode: $MODE"
    echo "Usage: bash run.sh [--api | --tests | --source <type> | --pipeline]"
    exit 1
    ;;

esac
