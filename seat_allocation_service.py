import os
import json
import logging
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("SeatAllocationService")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_BOOKING = "booking-events"
TOPIC_SEAT_CONFIRMED = "seat-confirmed-events"

def start_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC_BOOKING,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="seat-group",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        logger.info("[SeatAllocationService] Subscribed and listening on topic 'booking-events'")
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
            
            # Log receipt
            logger.info(f"[SeatAllocationService] Received SeatRequest for {cinema_booking_id}. CorrelationID: {correlation_id}")
            
            # Perform allocation simulation
            payload = msg.value
            seats_str = ", ".join(payload.get("seatNumbers", []))
            
            # Log processing result with correlationId
            logger.info(f"[SeatAllocationService] Seat reserved: {seats_str}. CorrelationID: {correlation_id}")
            
            # Forward confirmation event back to Kafka with correlationId in header
            out_payload = {
                "cinemaBookingId": cinema_booking_id,
                "seatNumbers": payload.get("seatNumbers", []),
                "totalPrice": payload.get("totalPrice", 0),
                "customerEmail": payload.get("customerEmail", "")
            }
            out_headers = [('correlationId', correlation_id.encode('utf-8'))]
            
            producer.send(TOPIC_SEAT_CONFIRMED, key=cinema_booking_id, value=out_payload, headers=out_headers)
            producer.flush()
            
        except Exception as e:
            logger.error(f"Error handling incoming message: {e}")

if __name__ == '__main__':
    start_consumer()