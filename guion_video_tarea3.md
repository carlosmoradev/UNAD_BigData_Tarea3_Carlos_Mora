# Guion Técnico Cronometrado: Video Explicativo Tarea 3 (Apache Spark y Kafka)
**Duración máxima:** 5 minutos (estricto rúbrica UNAD)  
**Estudiante:** Carlos Mario Mora Restrepo (Cód. 98636340) — Grupo 27  
**Tutor:** Javier Antonio Ballesteros Ricaurte  
**Curso:** Big Data (202016911) — UNAD ECBTI — Octubre 2026  
**Video publicado:** https://da.gd/sGlFG  

---

## 📋 Preparación Previa a la Grabación (Checklist)

1. **Pantalla 1 (Navegador):** Presentación abierta en Gamma en modo presentación completa:
   `https://gamma.app/docs/Analisis-Forense-de-Red-y-FinOps-sobre-AWS-VPC-Flow-Logs-m7orh8mgcf4064g`
2. **Máquina Virtual UNAD (`BIGDATA`):**
   * Iniciada (acceso por `ssh hadoop`) y con Zookeeper y Kafka arriba. Verificar que `jps` muestre `QuorumPeerMain` y `Kafka`:
     ```bash
     jps
     ```
   * Si NO aparecen (p. ej. tras reiniciar la VM), arrancarlos antes de grabar. No relanzarlos si ya corren: fallan por puerto ocupado.
     ```bash
     nohup /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties > /tmp/zk.log 2>&1 &
     sleep 8
     nohup /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties > /tmp/kafka.log 2>&1 &
     ```
   * Topic `vpc_flow_logs` verificado o creado:
     ```bash
     /opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic vpc_flow_logs 2>/dev/null || true
     ```
   * Scripts y datos en `~/tarea3` de la VM (`cd ~/tarea3`). Borrar `output/` antes de grabar: `rm -rf ~/tarea3/output`.
3. **Pantalla 2 (Terminales listas):**
   * **Terminal 1 (Batch):** Directorio de trabajo con el comando listo para dar Enter:
     ```bash
     cd ~/tarea3
     python3 batch_analysis.py --data-path data/aws_vpc_flow_logs.csv
     ```
   * **Terminal 2 y 3 (Streaming dividido):** en ambos paneles ejecutar `cd ~/tarea3` primero.
     * Panel izquierdo (Productor): `python3 kafka_producer.py --topic vpc_flow_logs`
     * Panel derecho (Consumidor): `/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_streaming_consumer.py --topic vpc_flow_logs`

---

## ⏱️ GUION PASO A PASO (0:00 – 5:00)

---

### BLOQUE 1: Introducción, Problema FinOps y Arquitectura (0:00 – 1:00)
**Visual:** Presentación en Gamma (Diapositivas 1 a 4).

* **0:00 – 0:15 | Slide 1 (Portada):**
  > *"Cordial saludo tutor Javier Ballesteros y compañeros. Mi nombre es Carlos Mario Mora Restrepo, estudiante del programa de Ingeniería de Sistemas de la UNAD, perteneciente al Grupo 27 del curso Big Data. En este video sustento la implementación práctica de la Tarea 3: Procesamiento de datos distribuidos con Apache Spark y Apache Kafka."*

* **0:15 – 0:35 | Slide 2 (El Problema FinOps) y Slide 3 (Dataset):**
  > *"Como caso de estudio seleccioné un reto crítico en arquitecturas Cloud de AWS: el control de costos (FinOps) asociado a la transferencia de datos por NAT Gateway, cuya tarifa es de $0.045 dólares por gigabyte. Para auditar y optimizar este tráfico sin comprometer datos reales, estructuré un dataset sintético representativo de 50.000 registros basado fielmente en el estándar de AWS VPC Flow Logs versión 2."*

* **0:35 – 1:00 | Slide 4 (Arquitectura Lambda):**
  > *"Diseñé una Arquitectura Lambda sobre Apache Spark. En la capa Batch, procesamos el histórico para identificar los 'Top Talkers' y auditar bloqueos de red con DataFrames y RDDs. En la capa Streaming, ingestamos telemetría continua mediante un topic de Apache Kafka y calculamos métricas en ventanas de 1 minuto con Spark Streaming para disparar alertas tempranas de facturación."*

---

### BLOQUE 2: Procesamiento en Batch con Spark (1:00 – 2:30)
**Visual:** Cambiar a la Terminal. Mostrar brevemente el editor/código y ejecutar `batch_analysis.py`.

* **1:00 – 1:25 | Explicación del Código:**
  > *"Pasando al código, en `batch_analysis.py` implementamos dos niveles de abstracción. Por un lado, utilizamos DataFrames para la ingesta estructurada con esquema tipado estricto y modelado de costos de egress. Por otro lado, cumpliendo con la guía, implementamos transformaciones de bajo nivel sobre RDDs usando `filter` para aislar flujos pesados mayores a 100 megabytes y `reduceByKey` con map-side combine para calcular el consumo acumulado por servicio de forma altamente eficiente."*

* **1:25 – 2:00 | Ejecución en Vivo:**
  *(Lanzas el comando en la terminal)*
  ```bash
  cd ~/tarea3
  python3 batch_analysis.py --data-path data/aws_vpc_flow_logs.csv
  ```
  > *"Ejecutamos el análisis batch. En segundos, Spark carga los 50.000 registros y nos genera el reporte consolidado."*

* **2:00 – 2:30 | Análisis de Resultados en Pantalla:**
  *(Señalas con el cursor la salida de la consola)*
  > *"Observamos tres hallazgos clave:*  
  > *1. En el balance FinOps, el tráfico de salida hacia Internet representa 784 GB y $35 dólares de costo, mientras que el tráfico intra-VPC no genera costo de NAT Gateway.*  
  > *2. En los Top Talkers, vemos que tres servicios (`Analytics-Spark-Worker-01`, `ETL-Data-Sync-Agent` y `Storage-Backup-Sync-Agent`) concentran más del 98% del costo de toda la infraestructura.*  
  > *3. En la auditoría de seguridad, detectamos un 3.8% de tráfico bloqueado con `REJECT`, principalmente escaneos externos hacia puertos como RDP 3389, 8080 y 9000."*

---

### BLOQUE 3: Procesamiento en Tiempo Real con Kafka y Spark Streaming (2:30 – 4:15)
**Visual:** Terminal dividida en dos paneles (Lado a Lado).

* **2:30 – 3:00 | Explicación del Pipeline Streaming:**
  *(Dale Enter al consumidor Spark YA, al empezar este bloque: tarda unos 20-40 s en arrancar. Mientras, hablás. Al llegar a 3:00 lanzás el productor.)*
  > *"Ahora pasamos al procesamiento en tiempo real cumpliendo el Anexo 3. En el panel izquierdo tenemos el productor `kafka_producer.py`, que serializa eventos JSON hacia el broker de Kafka en el topic `vpc_flow_logs`. En el panel derecho tenemos `spark_streaming_consumer.py`, que consume el topic y realiza agregaciones continuas sobre ventanas de tiempo de 1 minuto."*

* **3:00 – 3:45 | Ejecución del Streaming:**
  *(El consumidor ya debe estar corriendo desde 2:30; ahora lanzas el productor)*
  ```bash
  # Panel Derecho: Consumidor Spark Streaming
  cd ~/tarea3
  /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_streaming_consumer.py --topic vpc_flow_logs

  # Panel Izquierdo: Productor Kafka
  cd ~/tarea3
  python3 kafka_producer.py --topic vpc_flow_logs
  ```
  > *"Arrancamos el consumidor y el productor. Vemos cómo los eventos fluyen en tiempo real. Un aspecto técnico fundamental que resolvimos fue corregir la marca temporal UNIX Epoch mediante la función `timestamp_seconds`, permitiendo que Spark asigne correctamente la marca de agua y las ventanas de agregación."*

* **3:45 – 4:15 | Demostración de Alertas FinOps:**
  *(Esperas que aparezca la primera ventana de micro-batch en consola)*
  > *"Aquí vemos el primer micro-batch completado. Spark Streaming totaliza el volumen en megabytes transferido por cada servicio e interfaz en la ventana actual, calcula el costo acumulado y nos permite emitir alertas en caliente cuando un servicio presenta una anomalía de egress antes de que impacte la factura a fin de mes."*

---

### BLOQUE 4: Conclusiones y Cierre (4:15 – 5:00)
**Visual:** Volver a Gamma (Slide 7 - Conclusiones).

* **4:15 – 4:45 | Conclusiones Técnicas:**
  > *"Como conclusión, esta implementación demuestra la versatilidad de Apache Spark para unificar cargas de trabajo históricas y en tiempo real bajo una misma arquitectura en memoria. La combinación de Kafka para la ingesta desacoplada y Spark para el procesamiento analítico permite resolver problemas de gran escala, optimizando costos operativos y fortaleciendo la seguridad en la nube."*

* **4:45 – 5:00 | Cierre:**
  > *"Todo el código fuente, scripts de generación y documentación detallada se encuentran publicados en mi repositorio público de GitHub. Muchas gracias por su atención y quedo atento a la retroalimentación."*

---

## 🎯 Consejos para no pasarte de los 5 minutos:
1. **No abras archivos de código para leerlos línea por línea:** Solo mostrá los primeros comentarios o la función principal 10 segundos; el tutor evalúa la ejecución y la comprensión de los resultados.
2. **Dejá los comandos ya escritos en el historial:** Usá la flecha arriba (`↑`) para ejecutar rápido sin perder tiempo tipeando.
3. **Tené el cronómetro a la vista:** Si al llegar al minuto 2:15 aún no pasaste a Kafka, saltá directo a la terminal dividida.
