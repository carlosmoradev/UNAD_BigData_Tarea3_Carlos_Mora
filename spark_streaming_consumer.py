#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==============================================================================
# Universidad Nacional Abierta y a Distancia (UNAD)
# Escuela de Ciencias Básicas, Tecnología e Ingeniería (ECBTI)
# Curso: Big Data (Código: 202016911) - Tarea 3
# 
# Componente: Consumidor en Streaming con Apache Spark y Kafka
# Caso de Estudio: Monitoreo en Tiempo Real de Egress, Costos NAT GW y Alertas FinOps
# Estudiante: Carlos Mario Mora Restrepo (Cód. 98636340)
# Grupo: 27
# ==============================================================================

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    max as spark_max,
    round as spark_round,
    sum as spark_sum,
    timestamp_seconds,
    when,
    window
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType
)

def define_streaming_schema() -> StructType:
    """
    Define el esquema estructurado de los eventos de flujo emitidos por el productor.
    El campo timestamp se define como LongType para soportar marcas de tiempo UNIX Epoch en segundos.
    """
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("version", IntegerType(), True),
        StructField("account_id", StringType(), True),
        StructField("interface_id", StringType(), True),
        StructField("service_name", StringType(), True),
        StructField("src_addr", StringType(), True),
        StructField("dst_addr", StringType(), True),
        StructField("src_port", IntegerType(), True),
        StructField("dst_port", IntegerType(), True),
        StructField("protocol", IntegerType(), True),
        StructField("packets", LongType(), True),
        StructField("bytes", LongType(), True),
        StructField("mb_transferred", DoubleType(), True),
        StructField("costo_nat_usd", DoubleType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", LongType(), True)
    ])

def main():
    parser = argparse.ArgumentParser(description="Consumidor Spark Streaming de AWS VPC Flow Logs - Tarea 3 UNAD")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Dirección del broker Kafka (default: localhost:9092)"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="vpc_flow_logs",
        help="Topic de Kafka a consumir (default: vpc_flow_logs)"
    )
    parser.add_argument(
        "--window-duration",
        type=str,
        default="1 minute",
        help="Duración de la ventana de agregación temporal (default: 1 minute)"
    )
    args = parser.parse_args()

    print("=" * 85)
    print(">>> INICIANDO CONSUMIDOR APACHE SPARK STREAMING (VPC FLOW LOGS & FINOPS)")
    print(f">>> Broker Kafka: {args.bootstrap_servers}")
    print(f">>> Topic:        {args.topic}")
    print(f">>> Ventana:      {args.window_duration}")
    print("=" * 85)

    # 1. Crear sesión distribuida de Spark
    spark = SparkSession.builder \
        .appName("UNAD_BigData_Tarea3_VPC_Streaming_CarlosMora") \
        .getOrCreate()

    # Silenciar logs verbosos
    spark.sparkContext.setLogLevel("WARN")

    # 2. Configurar la lectura continua desde Apache Kafka
    schema = define_streaming_schema()

    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.bootstrap_servers) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Deserializar el payload JSON y derivar la columna temporal event_time
    parsed_stream = kafka_stream \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", timestamp_seconds(col("timestamp")))

    # 4. Agregación analítica en ventanas de tiempo por servicio e interfaz
    windowed_finops = parsed_stream \
        .groupBy(
            window(col("event_time"), args.window_duration),
            col("service_name"),
            col("interface_id")
        ) \
        .agg(
            count("event_id").alias("flujos_recibidos"),
            spark_round(spark_sum("mb_transferred"), 2).alias("volumen_total_mb"),
            spark_round(spark_sum("costo_nat_usd"), 4).alias("costo_nat_usd_acumulado"),
            spark_round(spark_max("mb_transferred"), 2).alias("pico_max_mb"),
            spark_sum(when(col("action") == "REJECT", 1).otherwise(0)).alias("bloqueos_reject")
        ) \
        .orderBy(col("volumen_total_mb").desc())

    # 5. Publicar métricas continuas a la consola
    print("\n⚡ Conexión establecida con Kafka. Procesando micro-batches de telemetría en vivo...\n")
    query = windowed_finops.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
