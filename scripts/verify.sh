#!/usr/bin/env bash
set -euo pipefail
export HADOOP_HOME=/opt/hadoop-3.4.1
export HIVE_HOME=/opt/apache-hive-4.0.0-alpha-2-bin
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
HADOOP=/opt/hadoop-3.4.1/bin
BEELINE=/opt/apache-hive-4.0.0-alpha-2-bin/bin/beeline

echo "=== 1. Приложение Spark в YARN ==="
$HADOOP/yarn application -list -appStates FINISHED 2>/dev/null | grep hw5-prefect-etl | tail -1

echo "=== 2. Данные таблиц в HDFS ==="
$HADOOP/hdfs dfs -ls /user/team/spark-hw5/warehouse/hw5.db

echo "=== 3. Чтение таблиц через Beeline (Hive) ==="
$BEELINE -u jdbc:hive2://127.0.0.1:10000/default -n team --silent=true -e "
SHOW TABLES IN hw5;
SELECT count(*) AS rows_events_clean FROM hw5.events_clean;
SELECT count(*) AS rows_daily_stats FROM hw5.daily_stats;
SELECT event_date, count(*) AS rows_, sum(amount) AS total FROM hw5.events_clean GROUP BY event_date ORDER BY event_date;
"
