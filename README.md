# UNAD — Big Data (Código: 202016911)
## Tarea 3: Procesamiento de Datos con Apache Spark
### Caso Práctico: Análisis Forense de Red, FinOps y Detección de Fugas de Tráfico en AWS con Spark y Kafka

* **Estudiante:** Carlos Mario Mora Restrepo (Cód. 98636340)
* **Grupo Colaborativo:** 202016911A_2204 (Grupo No. 27)
* **Tutor:** Javier Antonio Ballesteros Ricaurte
* **Universidad:** Universidad Nacional Abierta y a Distancia (UNAD)
* **Escuela:** Escuela de Ciencias Básicas, Tecnología e Ingeniería (ECBTI)
* **Programa:** Ingeniería de Sistemas — Octubre de 2026
* **Video explicativo:** https://da.gd/sGlFG

---

## 1. Definición del Problema y Justificación del Dataset

### 1.1 Planteamiento del Problema (El Desafío FinOps en Cloud)
En infraestructuras empresariales desplegadas en la nube (Amazon Web Services), uno de los costos ocultos más volátiles y difíciles de rastrear corresponde a la **transferencia de datos (Data Transfer Fees)** y al procesamiento a través de **NAT Gateways** ($0.045 USD por cada Gigabyte procesado). Cuando servicios internos, cargas de trabajo de analítica o agentes de sincronización transfieren terabytes hacia endpoints públicos externos (GCP, Cloudflare, Internet) en lugar de usar canales privados (VPC Endpoints / Direct IGW), se generan picos de facturación inesperados de miles de dólares por día.

Para diagnosticar y prevenir este problema a escala, es indispensable analizar **AWS VPC Flow Logs**: registros de telemetría de red que capturan metadatos de cada paquete que atraviesa las interfaces de red elásticas (ENIs). Este reto requiere dos enfoques complementarios:
1. **Analítica en Batch (Histórica):** Procesar millones de registros para perfilar los **"Top Talkers"** (servicios que consumen el 80% del ancho de banda), desglosar costos atribuibles al NAT Gateway y auditar tráfico bloqueado por Security Groups (`REJECT`).
2. **Analítica en Tiempo Real (Streaming):** Monitorear flujos en vivo mediante **Apache Kafka y Spark Streaming** para disparar alertas inmediatas cuando una interfaz presente un pico anómalo de transferencia antes de que impacte la factura mensual.

### 1.2 Estructura del Conjunto de Datos (`data/aws_vpc_flow_logs.csv`)
Se implementó un conjunto de datos representativo de **50.000 registros de flujo** (6.21 MB) basado estrictamente en el formato oficial **AWS VPC Flow Logs (Versión 2)**:

| Variable | Tipo de Dato | Descripción |
| :--- | :--- | :--- |
| `version` | Integer | Versión del formato de log de AWS (`2`) |
| `account_id` | String | Identificador sintético de cuenta de AWS (`123456789012`) |
| `interface_id` | String | Identificador sintético de interfaz ENI (`eni-0a11223344`, `eni-0b22334455`) |
| `service_name` | String | Nombre del servicio (`ETL-Data-Sync-Agent`, `Analytics-Spark-Worker-01`, etc.) |
| `src_addr` | String | Dirección IP de origen (privada RFC 1918 o pública documental RFC 5737) |
| `dst_addr` | String | Dirección IP de destino |
| `src_port` | Integer | Puerto de red de origen |
| `dst_port` | Integer | Puerto de red de destino (443 HTTPS, 5432 Postgres, 8080 API, 22 SSH) |
| `protocol` | Integer | Protocolo IANA (6: TCP, 17: UDP, 1: ICMP) |
| `packets` | Long | Número de paquetes transferidos durante la ventana de captura |
| `bytes` | Long | Total de bytes transferidos |
| `start` | Long | Marca temporal de inicio de captura (UNIX Epoch en segundos) |
| `end` | Long | Marca temporal de finalización de captura (UNIX Epoch en segundos) |
| `action` | String | Estado de la regla del Security Group (`ACCEPT` o `REJECT`) |
| `log_status` | String | Estado del registro (`OK`) |

---

## 2. Arquitectura de la Solución Técnica

La solución implementa una **Arquitectura Lambda** para analítica distribuida de telemetría en la nube:

```
[AWS VPC Infrastructure / ENIs]
              │
              ├─► (Logs Históricos CSV / HDFS) ──► Apache Spark (Batch) ──► [Catalyst Optimizer & RDDs] ──► Reportes FinOps & Top Talkers
              │
              └─► (Telemetría en Vivo) ────────► Apache Kafka Broker ──► Spark Streaming ────────► [Ventanas 1 min] ──► Alertas de Picos en Vivo
                                                  (Topic: vpc_flow_logs)
```

1. **Capa Batch (`batch_analysis.py`):**
   * **Ingesta estructurada con DataFrames:** Carga y validación tipada estricta.
   * **Modelado FinOps:** Clasificación automática de tráfico interno (VPC sin costo) vs. Egress público; cálculo de costo proyectado en NAT Gateway a razón de **$0.045 USD / GB**.
   * **Identificación de Top Talkers:** Agrupación y ordenamiento de ENIs y servicios con mayor impacto en volumen y facturación.
   * **Implementación con RDDs (Low-Level API de Spark):** Demostración explícita de transformaciones (`map`, `filter`, `reduceByKey`) y acciones (`count`, `take`) requeridas en la rúbrica de evaluación de la UNAD.
   * **Auditoría de Seguridad:** Análisis de flujos `REJECT` para detectar escaneos a puertos administrativos (22 SSH, 3389 RDP, 5432 Postgres).
   * **Exportación:** Salida en `output/batch_metrics/` en formato CSV estándar.

2. **Capa de Tiempo Real (`kafka_producer.py` & `spark_streaming_consumer.py`):**
   * **Productor Kafka:** Emite eventos JSON continuos al topic `vpc_flow_logs` simulando el stream de CloudWatch Logs, inyectando ráfagas de picos de transferencia y bloqueos de seguridad.
   * **Consumidor Spark Streaming:** Ingesta micro-batches desde Kafka, normaliza la marca temporal con `timestamp_seconds`, y calcula agregaciones continuas en **ventanas de tiempo de 1 minuto** agrupadas por servicio e interfaz, mostrando el throughput acumulado y alertando picos en vivo.

---

## 3. Instrucciones de Ejecución Paso a Paso

### 3.1 Requisitos Previos
* Python 3.8+ con librerías: `pyspark`, `kafka-python`.
* Java OpenJDK 8, 11 o superior.
* Entorno: Máquina Virtual UNAD (VirtualBox: `vboxuser` / `bigdata`) o clúster local.

Instalación de dependencias:
```bash
pip install pyspark kafka-python
```

### 3.2 Ejecución del Pipeline Batch (Apache Spark)
1. Para generar o actualizar el dataset histórico (50.000 registros):
   ```bash
   python3 scripts/generate_dataset.py
   ```
2. Para ejecutar el análisis en batch:
   ```bash
   python3 batch_analysis.py --data-path data/aws_vpc_flow_logs.csv
   ```
   *Si se ejecuta dentro de la máquina virtual con Hadoop HDFS:*
   ```bash
   # Cargar dataset a HDFS
   hdfs dfs -mkdir -p /unad/tarea3
   hdfs dfs -put -f data/aws_vpc_flow_logs.csv /unad/tarea3/
   
   # Ejecutar apuntando al HDFS
   python3 batch_analysis.py --data-path hdfs://localhost:9000/unad/tarea3/aws_vpc_flow_logs.csv
   ```

### 3.3 Ejecución del Pipeline en Streaming (Kafka + Spark Streaming)

#### Paso 1: Iniciar Zookeeper y Kafka (en la Máquina Virtual)
Se asume Apache Kafka 3.9.2 (build Scala 2.12) instalado en `/opt/Kafka`. Primero verificar si ya están corriendo (`jps` debe mostrar `QuorumPeerMain` y `Kafka`); si es así, omitir este paso, porque un segundo arranque falla por puerto ocupado.
```bash
jps

# Iniciar servidor Zookeeper
nohup /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties > /tmp/zk.log 2>&1 &
sleep 8

# Iniciar Broker de Kafka
nohup /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties > /tmp/kafka.log 2>&1 &
```

#### Paso 2: Crear el Topic en Kafka
```bash
/opt/Kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic vpc_flow_logs
```

#### Paso 3: Ejecutar el Productor de Telemetría (Terminal 1)
```bash
python3 kafka_producer.py --topic vpc_flow_logs --delay 0.8
```

#### Paso 4: Ejecutar el Consumidor de Spark Streaming (Terminal 2)
El consumidor lee desde `latest`: arrancarlo **antes** del productor, o no verá los eventos ya enviados. Necesita el conector de Kafka, por eso se lanza con `spark-submit --packages` (con `python3` directo falla con `Failed to find data source: kafka`). En la primera ejecución descarga el paquete y la ventana de 1 minuto sale vacía hasta recibir datos.
```bash
/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark_streaming_consumer.py --topic vpc_flow_logs
```

---

## 4. Guion para el Video Explicativo (Máximo 5 Minutos)

| Tiempo | Sección | Contenido a Explicar en Pantalla |
| :---: | :--- | :--- |
| **0:00 - 0:45** | **Introducción y Problema FinOps** | Presentación personal (Carlos Mario Mora, Grupo 27). Explicar el problema de negocio: Picos de costos por transferencia de datos en AWS y peajes de NAT Gateway ($0.045/GB). Presentar el dataset de VPC Flow Logs (50.000 registros con esquema estándar de AWS). |
| **0:45 - 2:30** | **Procesamiento Batch (Spark)** | Mostrar y ejecutar `batch_analysis.py`. Explicar en el código: 1) Ingesta con DataFrames tipados, 2) Modelado FinOps ($0.045/GB a Internet), 3) Demostración de RDDs con `map`, `filter` y `reduceByKey` para cálculo distribuido, 4) Tabla de Top Talkers (`Analytics-Spark-Worker-01`, `ETL-Data-Sync-Agent` y `Storage-Backup-Sync-Agent` concentrando el costo) y auditoría de bloqueos `REJECT`. |
| **2:30 - 4:15** | **Streaming (Kafka + Spark Streaming)** | Mostrar Zookeeper y Kafka activos. Ejecutar `python3 kafka_producer.py` mostrando la emisión continua de flujos de red. En otra terminal, ejecutar `spark_streaming_consumer.py` con `spark-submit` y mostrar cómo Spark procesa ventanas de 1 minuto agrupando por servicio y calculando throughput y costos en vivo. |
| **4:15 - 5:00** | **Conclusiones Técnicas** | Contrastar las diferencias entre procesamiento Batch (alto rendimiento, optimizaciones globales con Catalyst) y Streaming (baja latencia, ventanas de agregación). Concluir con la importancia de Apache Spark como motor unificado de Big Data. |

---

## 5. Estructura de la Presentación Online (Diapositivas)

* **Diapositiva 1 — Portada:** Título del proyecto, estudiante Carlos Mario Mora Restrepo, Grupo 27, Tutor Javier Ballesteros, UNAD ECBTI.
* **Diapositiva 2 — Planteamiento del Problema:** El reto de las 5 V's del Big Data aplicado a telemetría de red y control de costos FinOps en la nube.
* **Diapositiva 3 — Arquitectura Tecnológica:** Diagrama de flujo integrando HDFS, Apache Spark Batch, Apache Kafka y Spark Streaming.
* **Diapositiva 4 — Procesamiento Batch con PySpark:** Esquema de VPC Flow Logs, EDA, operaciones con DataFrames y demostración de RDDs (`map`, `filter`, `reduceByKey`).
* **Diapositiva 5 — Análisis FinOps y Seguridad:** Resultados de Top Talkers, costos proyectados de NAT Gateway y análisis de tráfico `REJECT`.
* **Diapositiva 6 — Procesamiento en Streaming (Kafka + Spark):** Pipeline en tiempo real, consumo por ventanas de 1 minuto y alertas en vivo.
* **Diapositiva 7 — Conclusiones y Enlaces:** Enlace al repositorio de código en GitHub y enlace al video demostrativo.
