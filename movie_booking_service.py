import os
import json
import uuid
import logging
from flask import Flask, request, jsonify
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("MovieBookingService")

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_BOOKING = "booking-events"

producer = None
def get_producer():
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
        except Exception as e:
            logger.error(f"Could not connect to Kafka: {e}")
    return producer

@app.route('/bookings', methods=['POST'])
def create_booking():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid request payload"}), 400

    cinema_booking_id = data.get("cinemaBookingId", "UNKNOWN")
    
    # Step 1: Generate UUID immediately upon receiving request
    correlation_id = str(uuid.uuid4())
    
    # Log matching the required format
    logger.info(f"[MovieBookingService] Created booking {cinema_booking_id}. CorrelationID: {correlation_id}")
    
    # Step 2: Inject correlationId into Kafka message headers (NOT the payload)
    prod = get_producer()
    if prod:
        try:
            headers = [('correlationId', correlation_id.encode('utf-8'))]
            prod.send(TOPIC_BOOKING, key=cinema_booking_id, value=data, headers=headers)
            prod.flush()
        except Exception as e:
            logger.error(f"Error publishing booking event: {e}")
            return jsonify({"error": "Failed to publish to Kafka"}), 500
    else:
        logger.warning("[MovieBookingService] Running in localized fallback mode. Broker offline.")

    return jsonify({
        "status": "BookingCreated",
        "cinemaBookingId": cinema_booking_id,
        "correlationId": correlation_id
    }), 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)