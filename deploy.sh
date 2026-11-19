#!/usr/bin/env bash
# ДЗ5: подготовка окружения для потока Prefect (запускать на edge team-22-en)
set -euo pipefail
cd "$(dirname "$0")"

HADOOP=/opt/hadoop-3.4.1/bin
SPARK_HOME=/opt/spark-3.5.3-bin-hadoop3
INPUT=/user/team/spark-hw4/input

echo "[1/4] Проверка кластера из ДЗ1-ДЗ4"
[ -d "$SPARK_HOME" ] || { echo "ОШИБКА: Spark не найден ($SPARK_HOME), нужно ДЗ4"; exit 1; }
yarn_nodes=$($HADOOP/yarn node -list 2>/dev/null | grep -c RUNNING || true)
[ "$yarn_nodes" -eq 3 ] || { echo "ОШИБКА: RUNNING NodeManager: $yarn_nodes (ожидалось 3)"; exit 1; }
$HADOOP/hdfs dfsadmin -report | grep "Live datanodes"
for unit in hive-metastore hive-server2; do
  systemctl is-active --quiet "$unit" || { echo "ОШИБКА: сервис $unit не запущен"; exit 1; }
done
$HADOOP/hdfs dfs -test -d "$INPUT" || { echo "ОШИБКА: нет входных данных $INPUT"; exit 1; }
echo "Кластер в порядке: 3 NodeManager, Hive работает, данные в HDFS на месте"

echo "[2/4] Проверка поддержки venv"
python3 -c "import ensurepip" 2>/dev/null || sudo apt-get install -y python3-venv

echo "[3/4] Виртуальное окружение и библиотеки"
[ -d .venv ] || python3 -m venv .venv
source .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet "prefect>=3" "pyspark==3.5.3"

echo "[4/4] Версии"
prefect version | grep -E "^Version"
python -c "import pyspark; print('pyspark', pyspark.__version__)"
echo "Готово. Запуск потока: ./scripts/run.sh"
