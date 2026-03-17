package io.debezium.tracing;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Lightweight Kafka producer interceptor that injects W3C traceparent headers
 * into every produced record. No OpenTelemetry SDK required.
 *
 * <p>Each message gets a unique traceparent header in the format:
 * {@code 00-<32-hex-trace-id>-<16-hex-span-id>-01}
 *
 * <p>Downstream consumers (e.g., Kafka CDC consumer) parse this header
 * to correlate traces across Debezium → Kafka → Airflow DAG tasks.
 *
 * <p>Configure in Kafka Connect:
 * <pre>
 * CONNECT_PRODUCER_INTERCEPTOR_CLASSES: io.debezium.tracing.TraceparentInterceptor
 * </pre>
 */
public class TraceparentInterceptor implements ProducerInterceptor<Object, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(TraceparentInterceptor.class);
    private static final String VERSION = "00";
    private static final String FLAGS = "01";
    private static final String TRACEPARENT_KEY = "traceparent";

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        String traceId = generateTraceId();
        String spanId = generateSpanId();
        String traceparent = VERSION + "-" + traceId + "-" + spanId + "-" + FLAGS;

        Headers headers = record.headers();
        headers.add(TRACEPARENT_KEY, traceparent.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        LOG.info("Injected traceparent header: {} on topic={}, key={}",
                traceparent, record.topic(), record.key());

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            LOG.warn("Message delivery failed for topic={}, partition={}: {}",
                    metadata != null ? metadata.topic() : "unknown",
                    metadata != null ? metadata.partition() : -1,
                    exception.getMessage());
        }
    }

    @Override
    public void close() {
        LOG.info("TraceparentInterceptor closed");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.info("TraceparentInterceptor initialized for distributed tracing");
    }

    /**
     * Generate a 32-character hex trace ID (128-bit).
     */
    private static String generateTraceId() {
        UUID uuid = UUID.randomUUID();
        return String.format("%016x%016x",
                uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /**
     * Generate a 16-character hex span ID (64-bit).
     */
    private static String generateSpanId() {
        return String.format("%016x", ThreadLocalRandom.current().nextLong());
    }
}
