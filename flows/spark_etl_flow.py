"""ДЗ5: поток Prefect, запускающий Spark на YARN, читающий данные из HDFS и сохраняющий таблицы."""
import os

os.environ.setdefault("SPARK_HOME", "/opt/spark-3.5.3-bin-hadoop3")
os.environ.setdefault("HADOOP_CONF_DIR", "/opt/hadoop-3.4.1/etc/hadoop")
os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64")
os.environ.setdefault("PYSPARK_PYTHON", "/usr/bin/python3")
os.environ.setdefault("DO_NOT_TRACK", "1")

from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from pyspark.sql import SparkSession, functions as F

HDFS_URI = "hdfs://10.22.0.11:9000"
INPUT_DIR = "/user/team/spark-hw4/input"
WAREHOUSE = f"{HDFS_URI}/user/team/spark-hw5/warehouse"


@task(name="1-start-spark-on-yarn", cache_policy=NO_CACHE)
def start_spark_session():
    log = get_run_logger()
    spark = (
        SparkSession.builder
        .appName("hw5-prefect-etl")
        .master("yarn")
        .config("spark.submit.deployMode", "client")
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        .enableHiveSupport()
        .getOrCreate()
    )
    log.info("Spark %s, master=%s, applicationId=%s",
             spark.version, spark.sparkContext.master, spark.sparkContext.applicationId)
    return spark


@task(name="2-connect-hdfs", cache_policy=NO_CACHE)
def connect_hdfs(spark):
    log = get_run_logger()
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(HDFS_URI), spark._jsc.hadoopConfiguration())
    path = jvm.org.apache.hadoop.fs.Path(INPUT_DIR)
    if not fs.exists(path):
        raise RuntimeError(f"{INPUT_DIR} не найден в HDFS")
    files = sorted(s.getPath().getName() for s in fs.listStatus(path))
    log.info("HDFS подключён (%s). В %s файлов: %d -> %s", HDFS_URI, INPUT_DIR, len(files), files)
    return f"{HDFS_URI}{INPUT_DIR}"


@task(name="3-read-data", cache_policy=NO_CACHE)
def read_data(spark, input_path):
    log = get_run_logger()
    df = spark.read.option("header", True).csv(input_path)
    log.info("Прочитано строк из HDFS: %d", df.count())
    return df


@task(name="4-transform", cache_policy=NO_CACHE)
def transform(df):
    log = get_run_logger()
    clean = (
        df
        .withColumn("event_id", F.col("event_id").cast("long"))
        .withColumn("user_id", F.col("user_id").cast("long"))
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .dropna(subset=["event_id", "user_id", "event_type", "amount", "event_ts"])
        .filter(F.col("event_type") != "")
        .dropDuplicates(["event_id"])
        .withColumn("event_date", F.to_date("event_ts"))
    )
    stats = (
        clean.groupBy("event_date", "country", "event_type")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("user_id").alias("users"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
        )
    )
    log.info("После очистки строк: %d, строк агрегата: %d", clean.count(), stats.count())
    return clean, stats


@task(name="5-save-tables", cache_policy=NO_CACHE)
def save_tables(spark, clean, stats):
    log = get_run_logger()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS hw5 LOCATION '{WAREHOUSE}/hw5.db'")
    (clean.write.mode("overwrite").format("parquet")
          .partitionBy("event_date").saveAsTable("hw5.events_clean"))
    (stats.write.mode("overwrite").format("parquet")
          .partitionBy("event_date").saveAsTable("hw5.daily_stats"))
    for t in ("hw5.events_clean", "hw5.daily_stats"):
        log.info("Таблица %s сохранена, строк: %d", t, spark.table(t).count())


@flow(name="hw5-spark-yarn-etl", log_prints=True)
def main():
    spark = start_spark_session()
    try:
        input_path = connect_hdfs(spark)
        df = read_data(spark, input_path)
        clean, stats = transform(df)
        save_tables(spark, clean, stats)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
