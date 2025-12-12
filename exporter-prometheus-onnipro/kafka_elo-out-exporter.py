from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime
from prometheus_client import start_http_server, Gauge

print("Inicializando exporter Kafka -> Prometheus...")

# ---------------------------
# Métrica 1: contagem por operação
# ---------------------------
operation_counts = defaultdict(int)

brokers = [
    "ip-10-100-7-51.sa-east-1.compute.internal:9092",
    "ip-10-100-7-61.sa-east-1.compute.internal:9092",
    "ip-10-100-6-99.sa-east-1.compute.internal:9092"
]

operation_gauge = Gauge(
    'kafka_operations',
    'Contagem das operações Kafka por minuto',
    ['operation']
)

# ---------------------------
# Métrica 2: Métricas de tempo (latência)
# ---------------------------

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

# ---------------------------
# Métrica 3: Acumuladores totais (sem filtrar por operação)
# ---------------------------

total_operation_count = 0
total_latency_sum = 0.0
total_latency_count = 0

# ---------------------------
# Métrica 4: Aprovadas ou negadas (por operação e total)
# ---------------------------

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

# ---------------------------
# Métrica 5: Contagem por internalResultCode (por operação, cardBrand e TOTAL)
# ---------------------------
# internalcode_counts[operation][cardBrand][internalResultCode] = count

internalcode_counts = defaultdict(
    lambda: defaultdict(lambda: defaultdict(int))
)

# Guardamos todos os trios (operation, cardBrand, internalResultCode) já vistos
seen_internal_codes = set()

internalcode_operation_gauge = Gauge(
    'kafka_internalcode_operations',
    'Contagem de operações por internalResultCode por minuto, por operação e cardBrand',
    ['operation', 'internalResultCode', 'cardBrand']
)

# ---------------------------
# Métrica 6: taxas (% em forma de fração 0–1)
# ---------------------------

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

# ---------------------------
# Métrica 7: desfazimentos (transactionType = "0420")
# ---------------------------
# reversal_counts[operation][result] -> int
# onde result ∈ {"all", "approved", "denied"}

reversal_counts = defaultdict(lambda: defaultdict(int))

reversal_operation_gauge = Gauge(
    'kafka_reversal_operations',
    'Contagem de desfazimentos (transactionType=0420) por minuto, por operação e resultado',
    ['operation', 'result']
)

# ---------------------------
# Métrica 8: cancelamentos (transactionType = "0400")
# ---------------------------
# cancellation_counts[operation][result] -> int
# onde result ∈ {"all", "approved", "denied"}

cancellation_counts = defaultdict(lambda: defaultdict(int))

cancellation_operation_gauge = Gauge(
    'kafka_cancellation_operations',
    'Contagem de cancelamentos (transactionType=0400) por minuto, por operação e resultado',
    ['operation', 'result']
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


# ---------------------------
# Kafka consumer
# ---------------------------

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

                if not monitoring_data:
                    continue

                outgoing_data = monitoring_data.get("outgoingData", {}) or {}
                incoming_data = monitoring_data.get("incomingData", {}) or {}

                operation = outgoing_data.get('operation')
                internal_result = outgoing_data.get('internalResultCode')
                brand_result = outgoing_data.get('brandResultCode')
                transaction_type = outgoing_data.get('transactionType')

                current_time = datetime.utcnow()
                minute_timestamp = current_time.replace(second=0, microsecond=0)

                # ---------------------------
                # Controle da janela de 1 minuto
                # ---------------------------
                if last_minute != minute_timestamp:
                    if last_exported_minute != last_minute:
                        last_exported_minute = last_minute

                        # 1) Métricas por operação (ALELO / Onibank)
                        for op in expected_operations:
                            # contagem total
                            count = operation_counts.get(op, 0)
                            operation_gauge.labels(operation=op).set(count)

                            # latência
                            lat_sum = operation_latency_sums.get(op, 0.0)
                            lat_count = operation_latency_counts.get(op, 0)
                            avg_latency = (lat_sum / lat_count) if lat_count > 0 else 0.0
                            operation_latency_avg_gauge.labels(operation=op).set(avg_latency)
                            operation_latency_count_gauge.labels(operation=op).set(lat_count)

                            # aprovadas / negadas
                            approved = approved_counts.get(op, 0)
                            denied = denied_counts.get(op, 0)
                            approved_operation_gauge.labels(operation=op).set(approved)
                            denied_operation_gauge.labels(operation=op).set(denied)

                        # 2) internalResultCode por operação + cardBrand (inclui TOTAL e ALL)
                        for (op, brand, code) in seen_internal_codes:
                            count = (
                                internalcode_counts
                                .get(op, {})
                                .get(brand, {})
                                .get(code, 0)
                            )
                            internalcode_operation_gauge.labels(
                                operation=op,
                                internalResultCode=code,
                                cardBrand=brand
                            ).set(count)

                        # 3) Métricas totais (sem filtro por operação)
                        operation_gauge.labels(operation="TOTAL").set(total_operation_count)

                        if total_latency_count > 0:
                            total_avg_latency = total_latency_sum / total_latency_count
                        else:
                            total_avg_latency = 0.0
                        operation_latency_avg_gauge.labels(operation="TOTAL").set(total_avg_latency)
                        operation_latency_count_gauge.labels(operation="TOTAL").set(total_latency_count)

                        approved_operation_gauge.labels(operation="TOTAL").set(total_approved_count)
                        denied_operation_gauge.labels(operation="TOTAL").set(total_denied_count)

                        # 4) Taxas e participações
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

                        # participação de volume (somente bandeiras)
                        for op in expected_operations:
                            if total_operation_count > 0:
                                share = operation_counts.get(op, 0) / total_operation_count
                            else:
                                share = 0.0
                            operation_share_gauge.labels(operation=op).set(share)

                        # 5) Desfazimentos: exporta kafka_reversal_operations (0420)
                        for op in expected_operations + ["TOTAL"]:
                            for result in ["all", "approved", "denied"]:
                                count = reversal_counts.get(op, {}).get(result, 0)
                                reversal_operation_gauge.labels(
                                    operation=op,
                                    result=result
                                ).set(count)

                        # 6) Cancelamentos: exporta kafka_cancellation_operations (0400)
                        for op in expected_operations + ["TOTAL"]:
                            for result in ["all", "approved", "denied"]:
                                count = cancellation_counts.get(op, {}).get(result, 0)
                                cancellation_operation_gauge.labels(
                                    operation=op,
                                    result=result
                                ).set(count)

                        # 7) Reseta acumuladores para o próximo minuto
                        operation_counts.clear()
                        operation_latency_sums.clear()
                        operation_latency_counts.clear()
                        approved_counts.clear()
                        denied_counts.clear()
                        internalcode_counts.clear()
                        reversal_counts.clear()
                        cancellation_counts.clear()

                        total_operation_count = 0
                        total_latency_sum = 0.0
                        total_latency_count = 0
                        total_approved_count = 0
                        total_denied_count = 0

                    last_minute = minute_timestamp

                # ---------------------------
                # Atualiza métricas só para operações de interesse
                # ---------------------------
                if operation not in expected_operations:
                    continue

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
                is_approved = (internal_result == "00" and brand_result == "00")

                if is_approved:
                    approved_counts[operation] += 1
                    total_approved_count += 1
                else:
                    denied_counts[operation] += 1
                    total_denied_count += 1

                # Cancelamentos (transactionType = "0400")
                if transaction_type == "0400":
                    # sempre conta em "all"
                    cancellation_counts[operation]["all"] += 1
                    cancellation_counts["TOTAL"]["all"] += 1

                    if is_approved:
                        cancellation_counts[operation]["approved"] += 1
                        cancellation_counts["TOTAL"]["approved"] += 1
                    else:
                        cancellation_counts[operation]["denied"] += 1
                        cancellation_counts["TOTAL"]["denied"] += 1

                # Desfazimentos (transactionType = "0420")
                if transaction_type == "0420":
                    # sempre conta em "all"
                    reversal_counts[operation]["all"] += 1
                    reversal_counts["TOTAL"]["all"] += 1

                    if is_approved:
                        reversal_counts[operation]["approved"] += 1
                        reversal_counts["TOTAL"]["approved"] += 1
                    else:
                        reversal_counts[operation]["denied"] += 1
                        reversal_counts["TOTAL"]["denied"] += 1

                # Contagem por internalResultCode (por operação, cardBrand e TOTAL)
                if internal_result:
                    card_brand = incoming_data.get("cardBrand") or "UNKNOWN"

                    ops_to_update = [operation, "TOTAL"]
                    brands_to_update = [card_brand, "ALL"]

                    for op_val in ops_to_update:
                        for brand_val in brands_to_update:
                            internalcode_counts[op_val][brand_val][internal_result] += 1
                            seen_internal_codes.add((op_val, brand_val, internal_result))

            except json.JSONDecodeError:
                print(f"Erro ao decodificar a mensagem: {message.value}")
except KeyboardInterrupt:
    print("Interrompido pelo usuário")
finally:
    print("Fechando consumer Kafka...")
    consumer.close()
