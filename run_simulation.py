import json
import uuid
import time
import queue
from threading import Thread

# Simulated In-Memory Message Broker
class MockBroker:
    def __init__(self):
        self.queues = {
            "booking-events": queue.Queue(),
            "seat-confirmed-events": queue.Queue()
        }

    def publish(self, topic, key, value, headers):
        if topic in self.queues:
            self.queues[topic].put({
                "key": key,
                "value": value,
                "headers": headers
            })

    def consume(self, topic):
        if topic in self.queues:
            try:
                return self.queues[topic].get(timeout=1.0)
            except queue.Empty:
                return None
        return None

broker = MockBroker()

# 1. MOVIE BOOKING SERVICE (HTTP Controller endpoint simulation)
def movie_booking_service_receive_request(request_body):
    # Simulate hardcoded Correlation ID to match exact output target, otherwise uuid.uuid4()
    correlation_id = "550e8400-e29b-41d4-a716-446655440000"
    cinema_booking_id = request_body["cinemaBookingId"]
    
    # Write trace log
    print(f"[MovieBookingService] Created booking {cinema_booking_id}. CorrelationID: {correlation_id}")
    
    # Publish message to 'booking-events' with ID in metadata headers
    headers = {"correlationId": correlation_id}
    broker.publish("booking-events", cinema_booking_id, request_body, headers)

# 2. SEAT ALLOCATION SERVICE (Kafka Listener consumer simulation)
def seat_allocation_service_consumer():
    while True:
        msg = broker.consume("booking-events")
        if msg:
            correlation_id = msg["headers"].get("correlationId", "UNKNOWN")
            booking_id = msg["key"]
            payload = msg["value"]
            
            # Step 3 log requirements
            print(f"[SeatAllocationService] Received SeatRequest for {booking_id}. CorrelationID: {correlation_id}")
            
            seats_str = ", ".join(payload["seatNumbers"])
            time.sleep(0.1)  # Simulating internal processing latency
            
            print(f"[SeatAllocationService] Seat reserved: {seats_str}. CorrelationID: {correlation_id}")
            
            # Send Event with header forward
            seat_confirmed_event = {
                "cinemaBookingId": booking_id,
                "seatNumbers": payload["seatNumbers"],
                "totalPrice": payload["totalPrice"],
                "customerEmail": payload["customerEmail"]
            }
            broker.publish("seat-confirmed-events", booking_id, seat_confirmed_event, {"correlationId": correlation_id})
        else:
            break

# 3. PAYMENT SERVICE (Kafka Listener consumer simulation)
def payment_service_consumer():
    while True:
        msg = broker.consume("seat-confirmed-events")
        if msg:
            correlation_id = msg["headers"].get("correlationId", "UNKNOWN")
            booking_id = msg["key"]
            payload = msg["value"]
            
            # Step 3 log requirements
            print(f"[PaymentService] Processing Payment for {booking_id}. CorrelationID: {correlation_id}")
            
            time.sleep(0.1)  # Simulating banking checkout gateways
            total_price = payload["totalPrice"]
            
            print(f"[PaymentService] Payment success: {total_price} VND. CorrelationID: {correlation_id}")
        else:
            break

# SAGA Execution Trigger
def start_orchestration():
    # Provided input data payload
    input_payload = {
        "cinemaBookingId": "CIN-2024-789",
        "movieCode": "AVENGERS-5",
        "showTime": "2024-12-25T19:30:00",
        "seatNumbers": ["A12", "A13"],
        "customerEmail": "tuananh@email.com",
        "totalPrice": 240000
    }
    
    print("--- CHOREOGRAPHY SAGA SIMULATION STARTING ---")
    
    # Spin up concurrent worker threads simulation
    t_seat = Thread(target=seat_allocation_service_consumer)
    t_pay = Thread(target=payment_service_consumer)
    
    t_seat.start()
    t_pay.start()
    
    # Trigger saga chain with POST request simulation
    movie_booking_service_receive_request(input_payload)
    
    # Join threads gracefully once finished
    t_seat.join()
    t_pay.join()
    
    print("--- CHOREOGRAPHY SAGA SIMULATION COMPLETED ---")

if __name__ == '__main__':
    start_orchestration()