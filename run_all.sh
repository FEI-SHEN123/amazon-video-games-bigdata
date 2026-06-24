#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "${PYTHON_BIN:-}" && -x "$PROJECT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"
else
  PYTHON_BIN="${PYTHON_BIN:-python3}"
fi

if [[ -z "${PYSPARK_SUBMIT_ARGS:-}" ]]; then
  export PYSPARK_SUBMIT_ARGS="--master local[2] --driver-memory 4g --conf spark.executor.memory=4g --conf spark.sql.shuffle.partitions=8 --conf spark.default.parallelism=8 --conf spark.sql.files.maxPartitionBytes=32m pyspark-shell"
fi

cd "$PROJECT_DIR"

mkdir -p outputs/tables outputs/figures

echo "Step 1/5: checking raw HDFS data"
"$PYTHON_BIN" scripts/01_load_check.py

echo "Step 2/5: cleaning and transforming data"
"$PYTHON_BIN" scripts/02_clean_transform.py

echo "Step 3/5: running Spark SQL analyses"
"$PYTHON_BIN" scripts/03_analysis_sql.py

echo "Step 4/5: generating visualizations"
"$PYTHON_BIN" scripts/04_visualization.py

echo "Step 5/5: running helpful review prediction"
"$PYTHON_BIN" scripts/05_method_depth.py

echo "Pipeline finished. Tables are in outputs/tables and figures are in outputs/figures."
