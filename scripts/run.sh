#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate
export DO_NOT_TRACK=1
export PREFECT_SERVER_ANALYTICS_ENABLED=false
mkdir -p evidence
LOG="evidence/run_$(date +%F_%H%M).log"
python flows/spark_etl_flow.py 2>&1 | tee "$LOG"
echo "Лог сохранён: $LOG"
