from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime
from prometheus_client import start_http_server, Gauge


# Métrica 1: contagem por operação
operation_counts = defaultdict(int)

brokers = ["10.100.7.51:9092", "10.100.7.61:9092", "10.100.6.99:9092"]

operation_gauge = Gauge(
    'kafka_operations',
    'Contagem das operações Kafka por minuto',
    ['operation']
)

# Métrica 2: Métricas de tempo (latência)

operation_latency_sums = defaultdict(float)
operation_latency_counts = defaultdict(int)

operation_latency_avg_gauge = Gauge(
    'kafka_operation_latency_avg_seconds',
    'Tempo médio (em segundos) entre incoming e outgoing por operação)',
    ['operation']
)

operation_latency_count_gauge = Gauge(
    'kafka_operation_latency_sample_count',
    'Quantidade de operações usadas no cálculo da latência média por operação',
    ['operation']
)

# Métrica 3: Acumuladores totais (sem filtrar por operação)

total_operation_count = 0
total_latency_sum = 0.0
total_latency_count = 0


# Métrica 4: Aprovadas ou negadas (por operação e total)


approved_counts = defaultdict(int)
denied_counts = defaultdict(int)
total_approved_count = 0
total_denied_count = 0

approved_operation_gauge = Gauge(
    'kafka_approved_operations',
    'Contagem de operações aprovadas por minuto',
    ['operation']
)
denied_operation_gauge = Gauge(
    'kafka_denied_operations',
    'Contagem de operações negadas por minuto',
    ['operation']
)

# Métrica 5: Contagem por internalResultCode (por operação e TOTAL)

internalcode_counts = defaultdict(lambda: defaultdict(int))
seen_internal_codes = set()

internalcode_operation_gauge = Gauge(
    'kafka_internalcode_operations',
    'Contagem de operações por internalResultCode por minuto',
    ['operation', 'internalResultCode']
)

# Métrica 6: taxas (% em forma de fração 0–1)

approval_rate_gauge = Gauge(
    'kafka_approval_rate',
    'Taxa de aprovação (0-1) das operações por minuto',
    ['operation']
)

denial_rate_gauge = Gauge(
    'kafka_denial_rate',
    'Taxa de negação (0-1) das operações por minuto',
    ['operation']
)

operation_share_gauge = Gauge(
    'kafka_operation_volume_share',
    'Participação (0-1) de cada operação no volume total do minuto',
    ['operation']
)


def parse_iso_with_nanos(ts: str):
    """Helper para parsear timestamp com nanos (ex: 2025-12-03T07:53:42.722617294)."""
    if not ts:
        return None
    try:
        if '.' in ts:
            base, frac = ts.split('.', 1)
            frac_digits = ''.join(ch for ch in frac if ch.isdigit())
            micro = (frac_digits + '000000')[:6]  # microsegundos
            fixed = f"{base}.{micro}"
        else:
            fixed = ts
        return datetime.fromisoformat(fixed)
    except Exception:
        return None


consumer = KafkaConsumer(
    bootstrap_servers=brokers,
    group_id="kafka-metrics-exporter-live-v1",
    auto_offset_reset='latest'
)

consumer.subscribe(pattern=".*elo-out.*")

# Garante que sempre pule para o fim ao iniciar (ignora histórico)
consumer.poll(timeout_ms=1000)
for tp in consumer.assignment():
    consumer.seek_to_end(tp)

# Expor métricas na porta 8000
start_http_server(8000)

last_minute = None
last_exported_minute = None
expected_operations = ["ALELO", "Onibank S/A"]

try:
    print("Consumindo mensagens dos tópicos 'elo-out' e expondo métricas para o Prometheus...")
    while True:
        for message in consumer:
            try:
                data = json.loads(message.value.decode('utf-8'))
                monitoring_data = data.get("monitoringData", None)

                if monitoring_data:
                    outgoing_data = monitoring_data.get("outgoingData", {}) or {}
                    incoming_data = monitoring_data.get("incomingData", {}) or {}

                    operation = outgoing_data.get('operation')
                    internal_result = outgoing_data.get('internalResultCode')
                    brand_result = outgoing_data.get('brandResultCode')

                    current_time = datetime.utcnow()
                    minute_timestamp = current_time.replace(second=0, microsecond=0)

                    # Controle da janela de 1 minuto
                    if last_minute != minute_timestamp:
                        if last_exported_minute != last_minute:
                            last_exported_minute = last_minute

                            # Métricas por operação
                            for op in expected_operations:
                                # 1) contagem por operação (todas as transações)
                                count = operation_counts.get(op, 0)
                                operation_gauge.labels(operation=op).set(count)

                                # 2) latência média por operação
                                lat_sum = operation_latency_sums.get(op, 0.0)
                                lat_count = operation_latency_counts.get(op, 0)
                                avg_latency = (lat_sum / lat_count) if lat_count > 0 else 0.0
                                operation_latency_avg_gauge.labels(operation=op).set(avg_latency)
                                operation_latency_count_gauge.labels(operation=op).set(lat_count)

                                # 3) aprovadas/negadas por operação
                                approved = approved_counts.get(op, 0)
                                denied = denied_counts.get(op, 0)
                                approved_operation_gauge.labels(operation=op).set(approved)
                                denied_operation_gauge.labels(operation=op).set(denied)

                            # 4) internalResultCode por operação (inclui TOTAL)
                            #    Para todos os (op,code) já vistos, setamos o valor do minuto (ou 0)
                            for (op, code) in seen_internal_codes:
                                count = internalcode_counts.get(op, {}).get(code, 0)
                                internalcode_operation_gauge.labels(
                                    operation=op,
                                    internalResultCode=code
                                ).set(count)

                            # Métricas totais (sem filtro por operação)
                            operation_gauge.labels(operation="TOTAL").set(total_operation_count)

                            if total_latency_count > 0:
                                total_avg_latency = total_latency_sum / total_latency_count
                            else:
                                total_avg_latency = 0.0
                            operation_latency_avg_gauge.labels(operation="TOTAL").set(total_avg_latency)
                            operation_latency_count_gauge.labels(operation="TOTAL").set(total_latency_count)

                            approved_operation_gauge.labels(operation="TOTAL").set(total_approved_count)
                            denied_operation_gauge.labels(operation="TOTAL").set(total_denied_count)
                            ops_for_rates = expected_operations + ["TOTAL"]

                            for op in ops_for_rates:
                                if op == "TOTAL":
                                    total = total_operation_count
                                    approved = total_approved_count
                                    denied = total_denied_count
                                else:
                                    total = operation_counts.get(op, 0)
                                    approved = approved_counts.get(op, 0)
                                    denied = denied_counts.get(op, 0)

                                if total > 0:
                                    approval_rate = approved / total
                                    denial_rate = denied / total
                                else:
                                    approval_rate = 0.0
                                    denial_rate = 0.0

                                approval_rate_gauge.labels(operation=op).set(approval_rate)
                                denial_rate_gauge.labels(operation=op).set(denial_rate)

                            # Participação de volume por operação (somente bandeiras)
                            for op in expected_operations:
                                if total_operation_count > 0:
                                    share = operation_counts.get(op, 0) / total_operation_count
                                else:
                                    share = 0.0
                                operation_share_gauge.labels(operation=op).set(share)

                            # Reseta acumuladores para o próximo minuto
                            operation_counts.clear()
                            operation_latency_sums.clear()
                            operation_latency_counts.clear()
                            approved_counts.clear()
                            denied_counts.clear()
                            internalcode_counts.clear()

                            total_operation_count = 0
                            total_latency_sum = 0.0
                            total_latency_count = 0
                            total_approved_count = 0
                            total_denied_count = 0

                        last_minute = minute_timestamp

                    # Atualiza métricas só para operações de interesse
                    if operation in expected_operations:
                        # Contagem por operação e total
                        operation_counts[operation] += 1
                        total_operation_count += 1

                        # Latência
                        in_ts_str = incoming_data.get("dateCreation")
                        out_ts_str = outgoing_data.get("dateCreation")

                        in_ts = parse_iso_with_nanos(in_ts_str) if in_ts_str else None
                        out_ts = parse_iso_with_nanos(out_ts_str) if out_ts_str else None

                        if in_ts and out_ts:
                            latency_seconds = (out_ts - in_ts).total_seconds()
                            if latency_seconds >= 0:
                                operation_latency_sums[operation] += latency_seconds
                                operation_latency_counts[operation] += 1
                                total_latency_sum += latency_seconds
                                total_latency_count += 1

                        # Aprovadas (00/00) x Negadas (restante)
                        if internal_result == "00" and brand_result == "00":
                            approved_counts[operation] += 1
                            total_approved_count += 1
                        else:
                            denied_counts[operation] += 1
                            total_denied_count += 1

                        # Contagem por internalResultCode (por operação e TOTAL)
                        if internal_result:
                            internalcode_counts[operation][internal_result] += 1
                            internalcode_counts["TOTAL"][internal_result] += 1

                            seen_internal_codes.add((operation, internal_result))
                            seen_internal_codes.add(("TOTAL", internal_result))

            except json.JSONDecodeError:
                print(f"Erro ao decodificar a mensagem: {message.value}")
except KeyboardInterrupt:
    print("Interrompido pelo usuário")
finally:
    consumer.close()
