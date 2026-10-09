# Bot & Fraud Detection Lab

Bộ khung thư mục cho lộ trình 5 tháng trong [roadmap](roadmap_bot_fraud_detection_5_months%20%281%29.md). Hiện repository chỉ có cấu trúc và tài liệu; các module, cấu hình triển khai, dữ liệu và mô hình sẽ được xây dựng theo từng tuần. Chưa có lệnh nào trong roadmap được triển khai hay kiểm chứng ở bộ khung này.

Xem [hướng dẫn từng thư mục và vị trí file Docker](docs/thu-muc-du-an.md).

## Nhóm thư mục

| Đường dẫn | Nội dung dự kiến |
| --- | --- |
| `apps/api/` | FastAPI, xác thực, ingestion, account, risk, case, review, action, investigation và health. |
| `apps/web/` | React/TypeScript dashboard, timeline, graph, case và admin UI. |
| `workers/` | Outbox publisher, detection worker và investigation worker. |
| `packages/` | Contracts, feature, detection, graph và Agent dùng chung. |
| `pipelines/` | Import/normalize dữ liệu, Spark batch và streaming. |
| `simulator/` | Kịch bản normal/bot/promo abuse và replay. |
| `collectors/` | Thu thập catalog demo nếu cần. |
| `experiments/` | Config, training, evaluation, ablation và benchmark. |
| `migrations/` | Alembic migrations. |
| `tests/` | Unit, integration, quyền, recovery và acceptance. |
| `infra/` | Docker, reverse proxy, metrics, dashboard và deploy. |
| `docs/` | Architecture, schema, data, API, runbook, báo cáo và demo. |
| `data/` | Raw, manifest, clean, sample, synthetic, labels, features và checkpoint. |
| `artifacts/` | Model, experiment, benchmark và release artifacts. |
| `backups/` | Backup và restore drill local. |
| `.secrets/` | Secret theo môi trường, chỉ có thư mục giữ chỗ. |

`data/`, `artifacts/`, `backups/` và `.secrets/` chỉ lưu `.gitkeep` trong Git. Dữ liệu thật, model binary, backup và credential không commit. Các file cấu hình, Dockerfile, Compose, source và migration cần được viết khi triển khai chức năng tương ứng; thư mục giữ chỗ không đồng nghĩa chức năng đã chạy.

## Lộ trình dùng thư mục

| Tháng | Khu vực triển khai chính |
| --- | --- |
| 1 | `data/`, `pipelines/ingestion/`, `simulator/`, `apps/api/`, `apps/web/`, `migrations/`, Docker nền tảng trong `infra/docker/`. |
| 2 | `packages/features/`, `packages/detection/`, `packages/graph/networkx/`, `workers/detection/`, `experiments/training/`. |
| 3 | `workers/outbox/`, `pipelines/batch/`, image Spark trong `infra/docker/`, `data/features/`, `data/checkpoints/`. |
| 4 | `pipelines/streaming/`, `packages/graph/graphframes/`, `packages/agents/`, `workers/investigation/`. |
| 5 | `experiments/evaluation/`, `experiments/benchmarks/`, `tests/recovery/`, `infra/deploy/`, `docs/report/`, `backups/`. |
