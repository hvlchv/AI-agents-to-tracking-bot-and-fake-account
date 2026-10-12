# Hợp đồng dữ liệu v1 — bản nháp

## EventV1

Các trường bắt buộc dự kiến: `event_id`, `source`, `dataset_version`, `event_time`, `event_type`, `user_id`. Các trường tùy chọn gồm session, product, category, price, device, IP token và coupon. `event_time` là UTC; `ingested_at` là thời điểm hệ thống nhận event. Thiếu trường ở nguồn phải giữ `null`, không suy diễn giá trị.

Event ID public phải tái lập từ định danh file nguồn và số dòng gốc. Public và synthetic dùng namespace ID khác nhau. Event không chứa ground truth hoặc thông tin kịch bản.

## FeatureV1

Khóa dự kiến: `source`, `user_id`, `window_start`, `window_end`, `feature_version`. Lưu `cutoff`, `computed_at`, số đo và cờ chất lượng. Một cửa sổ 5 phút dùng khoảng `[window_start, window_end)`; không đọc event sau cutoff.

## EvidenceV1

Lưu assessment/case tham chiếu, `reason_code`, khoảng thời gian, số đo quan sát, ngưỡng nếu có, event IDs và provenance (`fixture`, `rules` hoặc `model`). Case mẫu phải ghi `fixture` và không bịa score.

Các kiểu dữ liệu và validator cụ thể cần được chốt trong `apps/api/app/contracts.py` trước khi triển khai API/importer.
