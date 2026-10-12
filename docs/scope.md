# Phạm vi MVP hai tuần đầu

Đơn vị đánh giá là một actor trong cửa sổ 5 phút `[window_start, window_end)` theo UTC. Hệ thống theo dõi riêng mục tiêu bot và lạm dụng ưu đãi; quy tắc nhãn nằm trong [label-policy.md](label-policy.md).

## Tuần 1

- Chọn sample REES46 liên tục theo thời gian, có warm-up và manifest.
- Chuẩn hóa, import vào PostgreSQL an toàn khi chạy lại.
- Cung cấp health, readiness và timeline có phân trang.

## Tuần 2

- Sinh bốn kịch bản: normal, bot đều nhịp, flash sale hợp lệ và multi-account demo.
- Tạo case mẫu có evidence, đăng nhập analyst/admin và lưu review cùng audit.
- Ghi giao dịch demo và outbox trong cùng transaction.

Case tuần 2 là fixture để thử workflow, chưa phải kết quả detection. Feature/rules bắt đầu tuần 3; Isolation Forest bắt đầu tuần 4. Không xây full e-commerce, full graph hoặc Kafka/Spark/MinIO trong hai tuần đầu.
