import os
import threading
import time

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json, col, to_timestamp, window, broadcast

REDPANDA_BROKERS = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
TOPIC = os.environ.get("TICKETS_TOPIC", "client_tickets")
DATA_DIR = os.environ.get("DATA_DIR", "./data")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "./checkpoints")
WINDOW_DURATION = os.environ.get("WINDOW_DURATION", "24 hours")
WATERMARK_DURATION = os.environ.get("WATERMARK_DURATION", "1 hour")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "20 seconds")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("TicketsStreaming")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.jars",
            "/opt/spark-jars/spark-sql-kafka-0-10_2.13-4.2.0.jar,"
            "/opt/spark-jars/spark-token-provider-kafka-0-10_2.13-4.2.0.jar,"
            "/opt/spark-jars/kafka-clients-3.7.0.jar,"
            "/opt/spark-jars/commons-pool2-2.12.0.jar",
        )
        .getOrCreate()
    )


def build_dataframes(spark: SparkSession):
    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", REDPANDA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    schema = (
        StructType()
        .add("ticket_id", StringType())
        .add("client_id", StringType())
        .add("created_at", StringType())
        .add("demande", StringType())
        .add("type_demande", StringType())
        .add("priorite", StringType())
    )

    df_tickets = (
        df_raw.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )

    df_tickets_ts = df_tickets.withColumn(
        "created_at_ts", to_timestamp(col("created_at"))
    )

    df_watermarked = df_tickets_ts.withWatermark("created_at_ts", WATERMARK_DURATION)

    result_1 = df_watermarked.groupBy(
        window(col("created_at_ts"), WINDOW_DURATION),
        col("type_demande"),
    ).count()

    services = [
        {"type_demande": "Facturation", "equipe_support": "Comptabilité"},
        {"type_demande": "Support technique", "equipe_support": "SAV"},
        {"type_demande": "Résiliation", "equipe_support": "Service Client"},
        {"type_demande": "Question générale", "equipe_support": "Service Client"},
        {"type_demande": "Réclamation", "equipe_support": "Service Client"},
        {"type_demande": "Demande d'information", "equipe_support": "Service Client"},
    ]
    df_services = spark.createDataFrame([Row(**s) for s in services])
    df_enrichi = df_tickets.join(broadcast(df_services), on="type_demande", how="left")

    return result_1, df_enrichi


requetes_actives = {}


def lancer_avec_supervision(nom, build_query_fn, max_tentatives=5):
    tentative = 0
    while tentative < max_tentatives:
        try:
            query = build_query_fn()
            requetes_actives[nom] = query
            print(f"[{nom}] Démarré (tentative {tentative + 1}).")
            query.awaitTermination()
            break
        except Exception as e:
            tentative += 1
            attente = min(2**tentative, 60)
            print(f"[{nom}] Erreur (tentative {tentative}/{max_tentatives}) : {e}")
            print(f"[{nom}] Nouvelle tentative dans {attente}s...")
            time.sleep(attente)
    else:
        print(f"[{nom}] Abandon après {max_tentatives} tentatives.")


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    result_1, df_enrichi = build_dataframes(spark)

    def build_query_count():
        return (
            result_1.writeStream.queryName("comptage_par_type")
            .format("parquet")
            .option("path", f"{DATA_DIR}/comptage_par_type")
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/comptage_par_type")
            .outputMode("append")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )

    def build_query_enrichi():
        return (
            df_enrichi.writeStream.queryName("tickets_enrichis")
            .format("parquet")
            .option("path", f"{DATA_DIR}/tickets_enrichis")
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/tickets_enrichis")
            .outputMode("append")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )

    t1 = threading.Thread(
        target=lancer_avec_supervision,
        args=("comptage", build_query_count),
        daemon=True,
    )
    t2 = threading.Thread(
        target=lancer_avec_supervision,
        args=("enrichi", build_query_enrichi),
        daemon=True,
    )

    t1.start()
    t2.start()

    t1.join()
    t2.join()


if __name__ == "__main__":
    main()
