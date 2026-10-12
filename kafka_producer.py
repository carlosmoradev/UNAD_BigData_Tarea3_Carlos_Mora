#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==============================================================================
# Universidad Nacional Abierta y a Distancia (UNAD)
# Escuela de Ciencias Básicas, Tecnología e Ingeniería (ECBTI)
# Curso: Big Data (Código: 202016911) - Tarea 3
# 
# Componente: Productor de Eventos en Tiempo Real (Apache Kafka)
# Caso de Estudio: Telemetría Continua de AWS VPC Flow Logs y Monitoreo FinOps (100% Sintético)
# Estudiante: Carlos Mario Mora Restrepo (Cód. 98636340)
# Grupo: 27
# ==============================================================================

import argparse
import json
import random
import sys
import time

try:
    from kafka import KafkaProducer
except ImportError:
    print("❌ Error: Librería 'kafka-python' no encontrada.")
    print("👉 Instálala ejecutando: pip install kafka-python")
    sys.exit(1)

def generate_synthetic_flow_log_event(counter: int) -> dict:
    """
    Genera un registro 100% sintético de flujo de red en tiempo real simulando
    la telemetría de interfaces de red elásticas (ENIs) en una VPC empresarial.
    """
    interfaces = [
        {"eni": "eni-0a11223344", "service": "ETL-Data-Sync-Agent", "ip": "10.100.10.15"},
        {"eni": "eni-0b22334455", "service": "Analytics-Spark-Worker-01", "ip": "10.100.10.50"},
        {"eni": "eni-0c33445566", "service": "Core-API-Gateway", "ip": "10.100.1.20"},
        {"eni": "eni-0d44556677", "service": "Auth-Identity-Service", "ip": "10.100.10.30"},
        {"eni": "eni-0e55667788", "service": "PostgreSQL-Database-Primary", "ip": "10.100.20.5"},
        {"eni": "eni-0f66778899", "service": "Redis-Cache-Cluster", "ip": "10.100.20.25"},
        {"eni": "eni-01778899aa", "service": "Ingress-ALB-Frontend", "ip": "10.100.1.100"},
        {"eni": "eni-028899aabb", "service": "Storage-Backup-Sync-Agent", "ip": "10.100.10.80"}
    ]

    # IPs públicas documentales RFC 5737
    public_ips = [
        "198.51.100.25",   # Data Warehouse externo
        "198.51.100.80",   # Object Storage externo
        "203.0.113.50",    # CDN / Repositorio externo
        "192.0.2.140",     # API Externa
        "203.0.113.199"    # Escáner simulado
    ]

    internal_ips = [
        "10.100.10.10", "10.100.10.40", "10.100.20.10", "10.100.20.15"
    ]

    iface = random.choice(interfaces)
    now_epoch = int(time.time())

    # 1. Probabilidad de ráfaga de costo / FinOps Spike (~6% de los eventos)
    is_spike = (random.random() < 0.06) and (iface["service"] in ["ETL-Data-Sync-Agent", "Analytics-Spark-Worker-01", "Storage-Backup-Sync-Agent"])
    # 2. Probabilidad de escaneo bloqueado REJECT (~5% de los eventos)
    is_reject = random.random() < 0.05

    if is_spike:
        src_addr = iface["ip"]
        dst_addr = random.choice(public_ips[:2])
        src_port = random.randint(32768, 61000)
        dst_port = 443
        protocol = 6  # TCP
        # Spike de 150 MB a 1.2 GB en un micro-lote
        transferred_bytes = random.randint(150_000_000, 1_200_000_000)
        packets = transferred_bytes // 1460
        action = "ACCEPT"
        event_type = "🚨 [SPIKE FINOPS]"
    elif is_reject:
        src_addr = public_ips[-1]
        dst_addr = iface["ip"]
        src_port = random.randint(1024, 65535)
        dst_port = random.choice([22, 23, 3389, 5432, 8080, 9000])
        protocol = 6
        packets = random.randint(1, 5)
        transferred_bytes = packets * 40
        action = "REJECT"
        event_type = "🛡️ [REJECT SEGURIDAD]"
    else:
        src_addr = iface["ip"]
        is_internet = random.random() < 0.35
        dst_addr = random.choice(public_ips[:4]) if is_internet else random.choice(internal_ips)
        src_port = random.randint(32768, 61000)
        dst_port = 443 if is_internet else random.choice([5432, 6379, 8080, 9092])
        protocol = 6
        packets = random.randint(20, 2000)
        transferred_bytes = packets * random.randint(200, 1200)
        action = "ACCEPT"
        event_type = "✅ [NORMAL]"

    mb_transferred = round(transferred_bytes / (1024 * 1024), 2)
    # Tarifa AWS NAT Gateway = $0.045 por GB transferido a Internet
    is_public_dst = not (dst_addr.startswith("10.") or dst_addr.startswith("172.") or dst_addr.startswith("192.168."))
    costo_nat_usd = round((transferred_bytes / (1024 ** 3)) * 0.045, 4) if (is_public_dst and action == "ACCEPT") else 0.0

    return {
        "event_id": f"FLOW-{counter:07d}",
        "version": 2,
        "account_id": "123456789012",
        "interface_id": iface["eni"],
        "service_name": iface["service"],
        "src_addr": src_addr,
        "dst_addr": dst_addr,
        "src_port": src_port,
        "dst_port": dst_port,
        "protocol": protocol,
        "packets": packets,
        "bytes": transferred_bytes,
        "mb_transferred": mb_transferred,
        "costo_nat_usd": costo_nat_usd,
        "action": action,
        "timestamp": now_epoch,
        "event_type": event_type
    }

def main():
    parser = argparse.ArgumentParser(description="Productor Kafka de AWS VPC Flow Logs Sintéticos - Tarea 3 UNAD")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Servidores de Kafka (default: localhost:9092)"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="vpc_flow_logs",
        help="Nombre del topic de Kafka (default: vpc_flow_logs)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.8,
        help="Intervalo de envío entre eventos en segundos (default: 0.8)"
    )
    args = parser.parse_args()

    print("=" * 85)
    print(">>> PRODUCTOR DE TELEMETRÍA KAFKA — AWS VPC FLOW LOGS & FINOPS (SINTÉTICO)")
    print(f">>> Broker: {args.bootstrap_servers}")
    print(f">>> Topic:  {args.topic}")
    print(f">>> Frecuencia: {args.delay} seg/evento")
    print("=" * 85)

    try:
        producer = KafkaProducer(
            bootstrap_servers=[args.bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception as e:
        print(f"\n❌ Error al conectar con el broker de Kafka ({args.bootstrap_servers}): {e}")
        print("💡 Recuerda iniciar Zookeeper y Kafka en la máquina virtual:")
        print("   sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &")
        print("   sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &\n")
        sys.exit(1)

    print("\n🚀 Transmitiendo flujos de red sintéticos en tiempo real a Kafka... (Ctrl+C para detener)\n")
    counter = 1
    try:
        while True:
            event = generate_synthetic_flow_log_event(counter)
            producer.send(args.topic, value=event)
            
            tipo = event["event_type"]
            svc = event["service_name"]
            eni = event["interface_id"]
            mb = event["mb_transferred"]
            costo = event["costo_nat_usd"]
            action = event["action"]
            
            print(f"{tipo} #{counter:<5} | {eni} ({svc:<26}) | {action:<6} | {mb:>8,.2f} MB | Costo NAT: ${costo:>7,.4f} USD")
            
            counter += 1
            time.sleep(args.delay)
    except KeyboardInterrupt:
        print("\n\n🛑 Transmisión detenida por el usuario.")
    finally:
        producer.flush()
        producer.close()
        print("Conexión con Kafka cerrada.")

if __name__ == "__main__":
    main()
