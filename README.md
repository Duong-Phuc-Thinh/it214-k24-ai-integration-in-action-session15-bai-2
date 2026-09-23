# BÀI TẬP 2: XÂY DỰNG "BẢN ĐỒ DẪN ĐƯỜNG" VỚI CORRELATION ID & TRACING

## 1. Giới thiệu tổng quan
Hệ thống này áp dụng mô hình **Choreography Saga** để xử lý giao dịch phân tán cho việc đặt vé xem phim trực tuyến qua 3 dịch vụ:
1. **MovieBookingService** (Sinh Correlation ID, gửi sự kiện đặt vé)
2. **SeatAllocationService** (Nhận sự kiện, giữ chỗ và gửi sự kiện xác nhận)
3. **PaymentService** (Nhận sự kiện, xử lý thanh toán và hoàn tất)

### Vai trò của Correlation ID
Trong kiến trúc hướng sự kiện (Event-driven Architecture), luồng xử lý đi qua nhiều service độc lập có thể làm mất dấu vết giao dịch ban đầu (gây ra hiện tượng **Event Spaghetti**). 
**Correlation ID** hoạt động như một thẻ định danh duy nhất được sinh ra từ điểm đầu (MovieBookingService) và được truyền qua Header (metadata) của tất cả tin nhắn tiếp theo. Nhờ đó, các công cụ theo dõi (Jaeger/Zipkin) hoặc logs tập trung có thể xâu chuỗi toàn bộ hành trình của một yêu cầu.

### Tại sao nên truyền Correlation ID qua Header?
- **Tính rõ ràng (Separation of Concerns):** Payload của sự kiện chỉ chứa thông tin nghiệp vụ thực tế (business data) mà không bị lẫn lộn các thông tin phục vụ tracing kỹ thuật.
- **Tính linh hoạt:** Các công cụ API Gateway, Service Mesh hay Message Broker có thể đọc/ghi thông tin định danh này trực tiếp từ Header mà không cần tốn tài nguyên phân tích/giải nén cấu trúc JSON payload.

---

## 2. Luồng sự kiện của hệ thống

```
[Client Request]
       │
       ▼
[MovieBookingService]  ───(booking-events [Header: correlationId])───► [SeatAllocationService]
                                                                                │
                                                                                ▼ (Giữ ghế thành công)
[PaymentService]     ◄───(seat-confirmed-events [Header: correlationId])────────┘
       │
       ▼ (Thanh toán)
  [Kết thúc]
```

---

## 3. Cấu trúc thư mục dự án

```
movie-ticket-saga/
├── requirements.txt                   # Danh sách thư viện Python
├── docker-compose.yml                 # Cấu hình Zookeeper & Kafka Docker
├── run_simulation.py                  # Script giả lập chạy ngay lập tức không cần cài Kafka
├── movie_booking_service.py           # REST API Producer
├── seat_allocation_service.py         # Consumer & Producer trung gian
└── payment_service.py                 # Consumer cuối
```

---

## 4. Hướng dẫn chạy chương trình

Hệ thống hỗ trợ 2 cách chạy thử nghiệm:

### Cách 1: Chạy mô phỏng tích hợp ngay lập tức (Khuyến nghị để kiểm tra nhanh)
Bạn có thể chạy thử toàn bộ luồng Saga thông qua cơ chế Queue mô phỏng Broker trong Python mà không cần thiết lập hạ tầng Kafka bên ngoài:

```bash
python run_simulation.py
```

#### Kết quả mong đợi trên Log console:
```text
--- CHOREOGRAPHY SAGA SIMULATION STARTING ---
[MovieBookingService] Created booking CIN-2024-789. CorrelationID: 550e8400-e29b-41d4-a716-446655440000
[SeatAllocationService] Received SeatRequest for CIN-2024-789. CorrelationID: 550e8400-e29b-41d4-a716-446655440000
[SeatAllocationService] Seat reserved: A12, A13. CorrelationID: 550e8400-e29b-41d4-a716-446655440000
[PaymentService] Processing Payment for CIN-2024-789. CorrelationID: 550e8400-e29b-41d4-a716-446655440000
[PaymentService] Payment success: 240000 VND. CorrelationID: 550e8400-e29b-41d4-a716-446655440000
--- CHOREOGRAPHY SAGA SIMULATION COMPLETED ---
```

---

### Cách 2: Chạy với Apache Kafka thực tế (Sử dụng Docker)

#### Bước 1: Khởi động Kafka Broker
```bash
docker-compose up -d
```

#### Bước 2: Cài đặt thư viện Python
```bash
pip install -r requirements.txt
```

#### Bước 3: Khởi chạy các Service
Bạn hãy mở 3 terminal riêng biệt để chạy 3 service tương ứng:

```bash
# Terminal 1: Chạy PaymentService trước
python payment_service.py

# Terminal 2: Chạy SeatAllocationService
python seat_allocation_service.py

# Terminal 3: Khởi động API Gateway
python movie_booking_service.py
```

#### Bước 4: Kiểm thử bằng việc gửi POST Request
Gửi một yêu cầu đặt vé thông qua Client (Sử dụng `curl` hoặc Postman):

```bash
curl -X POST http://localhost:5001/bookings \
  -H "Content-Type: application/json" \
  -d '{
    "cinemaBookingId": "CIN-2024-789",
    "movieCode": "AVENGERS-5",
    "showTime": "2024-12-25T19:30:00",
    "seatNumbers": ["A12", "A13"],
    "customerEmail": "tuananh@email.com",
    "totalPrice": 240000
  }'
```