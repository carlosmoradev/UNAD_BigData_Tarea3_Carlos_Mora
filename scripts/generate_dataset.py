#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generador de AWS VPC Flow Logs 100% Sintéticos y Simulador de Telemetría FinOps.
Genera un conjunto de datos completamente simulado y genérico para benchmarks
académicos, utilizando identificadores ficticios (cuenta 123456789012, ENIs genéricas,
direcciones IP documentales RFC 5737 y subredes privadas RFC 1918).
"""

import csv
import os
import random
from datetime import datetime

def generate_synthetic_vpc_flow_logs(output_path: str, num_records: int = 50000, seed: int = 42):
    random.seed(seed)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Cuenta de prueba estándar de documentación de AWS
    account_id = "123456789012"
    base_epoch = int(datetime(2026, 10, 1, 0, 0, 0).timestamp())

    # Interfaces y microservicios sintéticos genéricos de arquitectura empresarial
    interfaces = [
        {"eni": "eni-0a11223344", "service": "ETL-Data-Sync-Agent", "subnet": "subnet-app-private-1a", "ip": "10.100.10.15"},
        {"eni": "eni-0b22334455", "service": "Analytics-Spark-Worker-01", "subnet": "subnet-app-private-1b", "ip": "10.100.10.50"},
        {"eni": "eni-0c33445566", "service": "Core-API-Gateway", "subnet": "subnet-public-web-1a", "ip": "10.100.1.20"},
        {"eni": "eni-0d44556677", "service": "Auth-Identity-Service", "subnet": "subnet-app-private-1a", "ip": "10.100.10.30"},
        {"eni": "eni-0e55667788", "service": "PostgreSQL-Database-Primary", "subnet": "subnet-db-private-1a", "ip": "10.100.20.5"},
        {"eni": "eni-0f66778899", "service": "Redis-Cache-Cluster", "subnet": "subnet-db-private-1b", "ip": "10.100.20.25"},
        {"eni": "eni-01778899aa", "service": "Ingress-ALB-Frontend", "subnet": "subnet-public-web-1b", "ip": "10.100.1.100"},
        {"eni": "eni-028899aabb", "service": "Storage-Backup-Sync-Agent", "subnet": "subnet-app-private-1c", "ip": "10.100.10.80"}
    ]

    # IPs públicas externas sintéticas (usando rangos reservados para documentación RFC 5737 y Anycast públicos)
    public_external_ips = [
        "198.51.100.25",   # Endpoint de almacenamiento en la nube externo (RFC 5737 TEST-NET-2)
        "198.51.100.80",   # Servicio de Data Warehouse en la nube (RFC 5737 TEST-NET-2)
        "203.0.113.50",    # CDN y repositorio de artefactos externo (RFC 5737 TEST-NET-3)
        "192.0.2.140",     # API REST externa de terceros (RFC 5737 TEST-NET-1)
        "203.0.113.199",   # Origen de escaneo de seguridad simulado (RFC 5737 TEST-NET-3)
        "192.0.2.250"      # Escáner de vulnerabilidades simulado (RFC 5737 TEST-NET-1)
    ]

    # IPs privadas sintéticas dentro de la VPC (RFC 1918)
    internal_private_ips = [
        "10.100.10.10", "10.100.10.40", "10.100.20.10", "10.100.20.15", 
        "10.100.1.10", "10.100.1.50"
    ]

    headers = [
        "version",
        "account_id",
        "interface_id",
        "service_name",
        "src_addr",
        "dst_addr",
        "src_port",
        "dst_port",
        "protocol",
        "packets",
        "bytes",
        "start",
        "end",
        "action",
        "log_status"
    ]

    print(f">>> Generando {num_records:,} registros de AWS VPC Flow Logs 100% SINTÉTICOS en {output_path}...")

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for i in range(1, num_records + 1):
            iface = random.choice(interfaces)
            eni = iface["eni"]
            service = iface["service"]
            src_ip = iface["ip"]

            # Ventana de 60 segundos por flujo
            time_offset = random.randint(0, 7 * 86400)
            start_ts = base_epoch + time_offset
            end_ts = start_ts + 60

            # 1. Simulación de anomalía / Spike de costo FinOps (~3.5% de los flujos)
            # Servicios de sincronización de datos o analítica transfiriendo alto volumen a almacenamiento externo
            is_cost_spike = (random.random() < 0.035) and (service in ["ETL-Data-Sync-Agent", "Analytics-Spark-Worker-01", "Storage-Backup-Sync-Agent"])

            # 2. Simulación de tráfico bloqueado REJECT (~4% de los flujos)
            is_rejected = random.random() < 0.040

            if is_cost_spike:
                dst_ip = random.choice(public_external_ips[:3])
                src_port = random.randint(32768, 61000)
                dst_port = 443
                protocol = 6  # TCP
                # Ráfaga masiva entre 200 MB y 2.2 GB por flujo de 60s
                transferred_bytes = random.randint(200_000_000, 2_200_000_000)
                packets = transferred_bytes // 1460
                action = "ACCEPT"
            elif is_rejected:
                src_ip = random.choice(public_external_ips[-2:])  # IP externa de prueba
                dst_ip = iface["ip"]
                src_port = random.randint(1024, 65535)
                dst_port = random.choice([22, 23, 3389, 5432, 8080, 9000])
                protocol = 6
                packets = random.randint(1, 10)
                transferred_bytes = packets * 40  # Paquetes TCP SYN bloqueados
                action = "REJECT"
            else:
                # Tráfico normal operacional (microservicios internos o peticiones HTTPS hacia internet)
                is_outbound_https = random.random() < 0.35
                if is_outbound_https:
                    dst_ip = random.choice(public_external_ips[:4])
                    dst_port = 443
                    protocol = 6
                    packets = random.randint(50, 4000)
                    transferred_bytes = packets * random.randint(200, 1400)
                else:
                    dst_ip = random.choice(internal_private_ips)
                    dst_port = random.choice([5432, 6379, 8080, 53, 9092])
                    protocol = 17 if dst_port == 53 else 6
                    packets = random.randint(10, 2000)
                    transferred_bytes = packets * random.randint(100, 1200)
                
                src_port = random.randint(32768, 61000)
                action = "ACCEPT"

            log_status = "OK"

            writer.writerow([
                2,  # Version 2
                account_id,
                eni,
                service,
                src_ip,
                dst_ip,
                src_port,
                dst_port,
                protocol,
                packets,
                transferred_bytes,
                start_ts,
                end_ts,
                action,
                log_status
            ])

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"✅ Dataset de VPC Flow Logs 100% SINTÉTICO generado: {output_path} ({file_size_mb:.2f} MB)")

if __name__ == "__main__":
    generate_synthetic_vpc_flow_logs("data/aws_vpc_flow_logs.csv", num_records=50000)
