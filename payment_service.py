import os
import json
import logging
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("PaymentService")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SEAT_CONFIRMED = "seat-confirmed-events"

def start_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC_SEAT_CONFIRMED,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="payment-group",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        logger.info("[PaymentService] Subscribed and listening on topic 'seat-confirmed-events'")
    except Exception as e:
        logger.error(f"Kafka connectivity error: {e}")
        return

    for msg in consumer:
        try:
            # Step 3: Extract correlationId from headers
            headers = dict(msg.headers) if msg.headers else {}
            correlation_id_bytes = headers.get('correlationId')
            correlation_id = correlation_id_bytes.decode('utf-8') if correlation_id_bytes else 'UNKNOWN'
            
            cinema_booking_id = msg.key.decode('utf-8') if msg.key else "UNKNOWN"
            
            # Log transaction step 1
            logger.info(f"[PaymentService] Processing Payment for {cinema_booking_id}. CorrelationID: {correlation_id}")
            
            # Process payment simulation
            payload = msg.value
            total_price = payload.get("totalPrice", 0)
            
            # Log transaction step 2 (final result)
            logger.info(f"[PaymentService] Payment success: {total_price} VND. CorrelationID: {correlation_id}")
            
        except Exception as e:
            logger.error(f"Error handling incoming seat confirmed event: {e}")

if __name__ == '__main__':
    start_consumer()