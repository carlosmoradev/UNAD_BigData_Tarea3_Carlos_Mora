#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==============================================================================
# Universidad Nacional Abierta y a Distancia (UNAD)
# Escuela de Ciencias Básicas, Tecnología e Ingeniería (ECBTI)
# Curso: Big Data (Código: 202016911)
# Tarea 3: Procesamiento de Datos con Apache Spark
# 
# Componente Práctico: Procesamiento en Batch con Apache Spark (PySpark)
# Caso de Estudio: Motor de Análisis Forense y FinOps sobre AWS VPC Flow Logs
# Estudiante: Carlos Mario Mora Restrepo (Cód. 98636340)
# Grupo: 27
# ==============================================================================

import argparse
import csv
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType
)

def create_spark_session(app_name: str = "UNAD_BigData_Tarea3_VPC_Flow_Logs_CarlosMora") -> SparkSession:
    """
    Inicializa la sesión distribuida de Spark configurada para análisis de logs masivos.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def define_vpc_flow_log_schema() -> StructType:
    """
    Esquema tipado conforme a la especificación estándar v2 de AWS VPC Flow Logs.
    """
    return StructType([
        StructField("version", IntegerType(), False),
        StructField("account_id", StringType(), False),
        StructField("interface_id", StringType(), False),
        StructField("service_name", StringType(), False),
        StructField("src_addr", StringType(), False),
        StructField("dst_addr", StringType(), False),
        StructField("src_port", IntegerType(), False),
        StructField("dst_port", IntegerType(), False),
        StructField("protocol", IntegerType(), False),
        StructField("packets", LongType(), False),
        StructField("bytes", LongType(), False),
        StructField("start", LongType(), False),
        StructField("end", LongType(), False),
        StructField("action", StringType(), False),
        StructField("log_status", StringType(), False)
    ])

def main():
    parser = argparse.ArgumentParser(description="Procesamiento en Batch de AWS VPC Flow Logs con PySpark - Tarea 3 UNAD")
    parser.add_argument(
        "--data-path",
        type=str,
        default="data/aws_vpc_flow_logs.csv",
        help="Ruta local o HDFS del dataset de VPC Flow Logs (default: data/aws_vpc_flow_logs.csv)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output/batch_metrics",
        help="Directorio para exportar métricas consolidadas (default: output/batch_metrics)"
    )
    args = parser.parse_args()

    print("\n" + "=" * 85)
    print(">>> INICIANDO PROCESAMIENTO EN BATCH CON APACHE SPARK")
    print(">>> Caso de Estudio: Análisis Forense y FinOps sobre AWS VPC Flow Logs")
    print(">>> Objetivo: Detección de Fugas de Egress por NAT Gateway ($0.045/GB) y Top Talkers")
    print(f">>> Fuente de datos: {args.data_path}")
    print("=" * 85 + "\n")

    spark = create_spark_session()

    # --------------------------------------------------------------------------
    # 1. INGESTA Y VALIDACIÓN DE ESQUEMA
    # --------------------------------------------------------------------------
    print("--- [PASO 1] Ingesta y Esquema de Datos de VPC Flow Logs ---")
    schema = define_vpc_flow_log_schema()
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(args.data_path)

    total_records = df.count()
    print(f"Total de registros de flujo cargados: {total_records:,}")
    print("\nEsquema estructurado del DataFrame (AWS VPC Flow Logs v2):")
    df.printSchema()

    print("\nMuestra de flujos de red registrados:")
    df.select("interface_id", "service_name", "src_addr", "dst_addr", "dst_port", "packets", "bytes", "action") \
      .show(5, truncate=False)

    # --------------------------------------------------------------------------
    # 2. LIMPIEZA, FEATURE ENGINEERING Y MODELADO FINOPS
    # --------------------------------------------------------------------------
    print("\n--- [PASO 2] Enriquecimiento y Cálculo de Métricas FinOps ---")
    
    # Clasificación de destino: Tráfico privado dentro de VPC vs Tráfico público (Egress a Internet / NAT Gateway)
    # Rango privado RFC 1918: 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
    df_enriched = df \
        .withColumn("gb_transferidos", F.round(F.col("bytes") / (1024 * 1024 * 1024), 4)) \
        .withColumn("mb_transferidos", F.round(F.col("bytes") / (1024 * 1024), 2)) \
        .withColumn("es_trafico_privado", 
            F.when(
                (F.col("dst_addr").startswith("10.")) |
                (F.col("dst_addr").startswith("192.168.")) |
                (F.col("dst_addr").startswith("172.16.")) |
                (F.col("dst_addr").startswith("172.17.")) |
                (F.col("dst_addr").startswith("172.18.")) |
                (F.col("dst_addr").startswith("172.19.")) |
                (F.col("dst_addr").startswith("172.20.")) |
                (F.col("dst_addr").startswith("172.31.")),
                1
            ).otherwise(0)
        ) \
        .withColumn("categoria_trafico",
            F.when(F.col("es_trafico_privado") == 1, "INTRA_VPC_PRIVADO (Sin Costo)")
             .otherwise("INTERNET_EGRESS (Via NAT Gateway)")
        ) \
        .withColumn("costo_nat_gateway_usd",
            # Tarifa estándar AWS Data Processing Fee en NAT Gateway = $0.045 por GB transferido
            F.when((F.col("es_trafico_privado") == 0) & (F.col("action") == "ACCEPT"),
                   F.round(F.col("gb_transferidos") * 0.045, 4))
             .otherwise(0.0)
        ) \
        .withColumn("protocolo_desc",
            F.when(F.col("protocol") == 6, "TCP")
             .when(F.col("protocol") == 17, "UDP")
             .when(F.col("protocol") == 1, "ICMP")
             .otherwise("OTRO")
        )

    # Persistencia en caché para acelerar consultas analíticas subsiguientes
    df_enriched.cache()

    # --------------------------------------------------------------------------
    # 3. ANÁLISIS EXPLORATORIO DE DATOS (EDA) CON DATAFRAMES
    # --------------------------------------------------------------------------
    print("\n--- [PASO 3] Análisis Exploratorio de Datos (EDA) y FinOps ---")

    print("\n3.1 Resumen General de Transferencia y Costos FinOps (USD):")
    resumen_global_finops = df_enriched.groupBy("categoria_trafico").agg(
        F.count("interface_id").alias("total_flujos"),
        F.round(F.sum("gb_transferidos"), 2).alias("volumen_total_gb"),
        F.round(F.sum("costo_nat_gateway_usd"), 2).alias("costo_total_nat_usd"),
        F.round(F.avg("mb_transferidos"), 2).alias("promedio_mb_por_flujo")
    )
    resumen_global_finops.show(truncate=False)

    print("\n3.2 Top Talkers (Interfaces ENI y Servicios de Mayor Tráfico y Costo):")
    top_talkers = df_enriched.filter(F.col("action") == "ACCEPT") \
        .groupBy("interface_id", "service_name") \
        .agg(
            F.count("bytes").alias("total_flujos"),
            F.round(F.sum("gb_transferidos"), 2).alias("total_gb"),
            F.round(F.sum("costo_nat_gateway_usd"), 2).alias("costo_nat_usd"),
            F.round(F.max("gb_transferidos"), 2).alias("pico_max_gb")
        ).orderBy(F.col("total_gb").desc())
    top_talkers.show(10, truncate=False)

    print("\n3.3 Distribución de Tráfico por Puerto Destino:")
    puertos_summary = df_enriched.groupBy("dst_port", "protocolo_desc") \
        .agg(
            F.count("interface_id").alias("cantidad_conexiones"),
            F.round(F.sum("gb_transferidos"), 2).alias("volumen_gb")
        ).orderBy(F.col("volumen_gb").desc())
    puertos_summary.show(8, truncate=False)

    # --------------------------------------------------------------------------
    # 4. IMPLEMENTACIÓN ESPECÍFICA CON RDDs (Low-Level API de Apache Spark)
    # Cumplimiento estricto de los requisitos de la guía académica:
    # Transformaciones: map, filter, reduceByKey
    # Acciones: count, take, collect
    # --------------------------------------------------------------------------
    print("\n--- [PASO 4] Demostración de Procesamiento con RDDs (Bajo Nivel) ---")
    
    # Extracción del RDD subyacente
    rdd_raw = df_enriched.rdd

    # 4.1 Transformación `filter`: Flujos con picos de tráfico superiores a 100 MB
    rdd_spikes = rdd_raw.filter(lambda row: row["mb_transferidos"] >= 100.0)
    count_spikes = rdd_spikes.count()  # Acción RDD: count
    print(f"RDD - Flujos de alto volumen / picos identificados (> 100 MB): {count_spikes:,}")

    # 4.2 Transformación `map`: Generar pares clave-valor (service_name, (gb_transferidos, costo_nat_usd, 1))
    rdd_pares = rdd_raw.map(lambda row: (row["service_name"], (row["gb_transferidos"], row["costo_nat_gateway_usd"], 1)))

    # 4.3 Transformación `reduceByKey`: Agregación distribuida por servicio
    rdd_totales = rdd_pares.reduceByKey(lambda a, b: (
        round(a[0] + b[0], 4),
        round(a[1] + b[1], 4),
        a[2] + b[2]
    ))

    # 4.4 Transformación `map`: Proyección final calculando promedio de GB por flujo
    rdd_resumen = rdd_totales.map(lambda kv: (
        kv[0],                          # Servicio
        round(kv[1][0], 2),             # Total GB
        round(kv[1][1], 2),             # Total Costo USD
        kv[1][2],                       # Conteo flujos
        round(kv[1][0] / kv[1][2], 4)   # Promedio GB/flujo
    ))

    print("\nRDD - Cálculo Distribuido de Costos por Servicio vía reduceByKey (Primeros 3 con take):")
    for item in rdd_resumen.take(3):  # Acción RDD: take
        print(f"  * Servicio: {item[0]:<25} | Total GB: {item[1]:>10,.2f} | Costo NAT: ${item[2]:>8,.2f} USD | Flujos: {item[3]:>6}")

    # --------------------------------------------------------------------------
    # 5. AUDITORÍA DE SEGURIDAD Y TRÁFICO RECHAZADO (REJECT)
    # --------------------------------------------------------------------------
    print("\n--- [PASO 5] Auditoría de Seguridad: Análisis de Conexiones REJECT ---")

    seguridad_resumen = df_enriched.groupBy("action").agg(
        F.count("interface_id").alias("cantidad_flujos"),
        F.round((F.count("interface_id") / total_records) * 100, 2).alias("porcentaje_%"),
        F.round(F.sum("mb_transferidos"), 2).alias("volumen_mb")
    )
    seguridad_resumen.show(truncate=False)

    print("\n5.1 Puertos Más Atacados / Escaneados con Bloqueo de Security Group (REJECT):")
    top_reject_ports = df_enriched.filter(F.col("action") == "REJECT") \
        .groupBy("dst_port", "protocolo_desc") \
        .agg(F.count("interface_id").alias("bloqueos_detectados")) \
        .orderBy(F.col("bloqueos_detectados").desc())
    top_reject_ports.show(6)

    print("\n5.2 Top 5 IPs Externas Rechazadas por Políticas de Red:")
    top_reject_ips = df_enriched.filter(F.col("action") == "REJECT") \
        .groupBy("src_addr") \
        .agg(F.count("interface_id").alias("intentos_bloqueados")) \
        .orderBy(F.col("intentos_bloqueados").desc())
    top_reject_ips.show(5)

    # --------------------------------------------------------------------------
    # 6. EXPORTACIÓN DE RESULTADOS
    # --------------------------------------------------------------------------
    print(f"\n--- [PASO 6] Exportando Métricas Consolidadas a {args.output_dir} ---")
    os.makedirs(args.output_dir, exist_ok=True)

    def export_df_to_csv(dataframe, filepath):
        rows = dataframe.collect()
        if not rows:
            return
        fieldnames = dataframe.columns
        with open(filepath, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(fieldnames)
            for r in rows:
                writer.writerow([r[col] for col in fieldnames])

    export_df_to_csv(top_talkers, os.path.join(args.output_dir, "top_talkers_finops.csv"))
    export_df_to_csv(resumen_global_finops, os.path.join(args.output_dir, "resumen_global_finops.csv"))
    export_df_to_csv(seguridad_resumen, os.path.join(args.output_dir, "resumen_seguridad_rejects.csv"))
    print("✅ Archivos CSV de métricas exportados exitosamente.")

    print("\n" + "=" * 85)
    print(">>> PROCESAMIENTO EN BATCH COMPLETADO EXITOSAMENTE")
    print("=" * 85 + "\n")

    spark.stop()

if __name__ == "__main__":
    main()
