# Hướng dẫn cấu trúc thư mục

Tài liệu này giải thích **các thư mục đã tạo** theo [roadmap](../roadmap_bot_fraud_detection_5_months%20%281%29.md). Hiện chúng là khung giữ chỗ bằng `.gitkeep`; tên thư mục mô tả nơi sẽ đặt mã nguồn hoặc dữ liệu khi triển khai, không có nghĩa chức năng đã hoạt động.

## 1. Ứng dụng và giao diện

| Thư mục | Dùng để chứa |
| --- | --- |
| `apps/` | Các ứng dụng có thể chạy độc lập. |
| `apps/api/` | Backend FastAPI và điểm vào CLI nghiệp vụ. |
| `apps/api/auth/` | Đăng nhập, kiểm tra quyền analyst/admin, xác thực nguồn gửi event. |
| `apps/api/db/` | Kết nối PostgreSQL, ORM và repository truy cập dữ liệu. |
| `apps/api/routers/` | Các endpoint auth, events, accounts, risk, graph, cases, reviews, actions, investigations và health. |
| `apps/api/schemas/` | Kiểu dữ liệu request/response cho API. |
| `apps/api/services/` | Logic nghiệp vụ phía API như tạo case, review và phê duyệt action. |
| `apps/web/` | Ứng dụng React, TypeScript và Vite cho analyst/admin. |
| `apps/web/public/` | Tài nguyên tĩnh như favicon, ảnh công khai. |
| `apps/web/src/` | Mã nguồn giao diện. |
| `apps/web/src/api/` | Client gọi backend và cấu hình truy vấn dữ liệu. |
| `apps/web/src/components/` | Thành phần giao diện dùng lại. |
| `apps/web/src/features/` | Các tính năng giao diện theo nghiệp vụ. |
| `apps/web/src/features/accounts/` | Hồ sơ tài khoản, timeline và risk. |
| `apps/web/src/features/admin/` | Màn hình quản trị và phân quyền. |
| `apps/web/src/features/cases/` | Danh sách, chi tiết case và evidence. |
| `apps/web/src/features/graph/` | Hiển thị subgraph tài khoản. |
| `apps/web/src/features/reviews/` | Review của analyst và đề xuất xử lý. |
| `apps/web/src/pages/` | Các trang và route của dashboard. |
| `apps/web/src/types/` | Kiểu TypeScript dùng chung trong frontend. |

## 2. Xử lý nền và logic dùng chung

| Thư mục | Dùng để chứa |
| --- | --- |
| `workers/` | Các tiến trình chạy nền, tách khỏi request HTTP. |
| `workers/outbox/` | Đọc outbox, gửi event và retry an toàn. |
| `workers/detection/` | Tính score, tạo risk assessment và case. |
| `workers/investigation/` | Chạy Agent điều tra, lưu report và trace. |
| `packages/` | Logic Python dùng chung giữa API, workers và pipelines. |
| `packages/contracts/` | Schema/contract cho event, feature, score, evidence và report. |
| `packages/features/` | Định nghĩa feature, cutoff/window và giao diện tính feature. |
| `packages/features/sql/` | Cách tính feature bằng SQL/PostgreSQL. |
| `packages/features/spark/` | Cách tính cùng feature contract bằng Spark. |
| `packages/detection/` | Model inference, reason code và risk policy. |
| `packages/detection/rules/` | Rule nhận diện hành vi và giải thích cảnh báo. |
| `packages/detection/models/` | Mã nạp model và chạy dự đoán; file model đã huấn luyện để ở `artifacts/models/`. |
| `packages/graph/` | Tạo graph feature và phục vụ subgraph cho UI. |
| `packages/graph/networkx/` | Graph prototype trên mẫu dữ liệu nhỏ. |
| `packages/graph/graphframes/` | Xử lý graph phân tán khi cần. |
| `packages/agents/` | Điều phối Agent và tạo báo cáo điều tra. |
| `packages/agents/tools/` | Tool truy xuất evidence/case có giới hạn và kiểm tra quyền. |
| `packages/agents/prompts/` | Prompt template có phiên bản. |
| `packages/agents/validators/` | Kiểm tra schema, nguồn evidence và tính nhất quán của report. |

## 3. Dữ liệu đầu vào và pipeline

| Thư mục | Dùng để chứa |
| --- | --- |
| `pipelines/` | CLI nạp dữ liệu, ETL và job xử lý. |
| `pipelines/ingestion/` | Inspect, normalize, sample, import PostgreSQL và đối soát nguồn dữ liệu. |
| `pipelines/batch/` | Spark jobs theo lô như normalize, features và graph snapshot. |
| `pipelines/streaming/` | Jobs đọc Kafka, archive, window feature và checkpoint. |
| `simulator/` | Sinh hành vi mô phỏng và ground truth. |
| `simulator/scenarios/` | Mã kịch bản normal, bot và lạm dụng ưu đãi. |
| `simulator/replay/` | Phát lại event vào API hoặc Kafka theo tốc độ cấu hình. |
| `collectors/` | Công cụ thu thập metadata/catalog demo nếu cần crawl. |
| `data/` | Dữ liệu local phục vụ phát triển và kiểm chứng; không commit nội dung thực. |
| `data/raw/` | File tải về nguyên gốc, giữ checksum và nguồn. |
| `data/manifests/` | Hồ sơ nguồn, license, schema, số dòng và checksum. |
| `data/clean/` | Event đã chuẩn hóa và kiểm tra schema, thường ở Parquet. |
| `data/sample/` | Tập mẫu tái lập được cho MVP hoặc parity test. |
| `data/synthetic/` | Event mô phỏng theo run ID. |
| `data/labels/` | Ground truth/nhãn tách khỏi dữ liệu inference. |
| `data/features/` | Feature snapshot/bulk có version và cutoff. |
| `data/quarantine/` | Record lỗi hoặc không đạt contract để kiểm tra. |
| `data/crawl-state/` | Trạng thái crawl để có thể tiếp tục job. |
| `data/checkpoints/` | Checkpoint cho streaming; không xóa tùy tiện khi restart. |

## 4. Thực nghiệm, kiểm thử và phiên bản dữ liệu

| Thư mục | Dùng để chứa |
| --- | --- |
| `experiments/` | Mã/config tái lập thí nghiệm, train, evaluate và benchmark. |
| `experiments/training/` | Cấu hình train baseline và model ứng viên. |
| `experiments/evaluation/` | Protocol, cấu hình đánh giá và phân tích lỗi. |
| `experiments/ablation/` | Thí nghiệm so behavior, graph và các tập feature. |
| `experiments/benchmarks/` | Cấu hình workload, tải, tài nguyên và phép đo công bằng. |
| `experiments/scenarios/` | YAML cấu hình kịch bản simulator, ví dụ `mvp.yaml`. |
| `experiments/graph/` | Chính sách edge, snapshot và graph experiment. |
| `experiments/storage/` | Cấu hình storage cho môi trường lab. |
| `experiments/splits/` | Quy tắc chia train/validation/test và manifest tương ứng. |
| `migrations/` | Cấu hình Alembic và thay đổi schema PostgreSQL. |
| `migrations/versions/` | Từng revision migration có thứ tự. |
| `tests/` | Kiểm tra chức năng và tính đúng dữ liệu. |
| `tests/fixtures/` | Dữ liệu nhỏ, tính tay được, cho test. |
| `tests/fixtures/events/` | Event hợp lệ, lỗi, trùng và đến muộn. |
| `tests/fixtures/features/` | Feature kỳ vọng và trường hợp thiếu dữ liệu. |
| `tests/fixtures/graph/` | Graph nhỏ, shared IP và cluster mẫu. |
| `tests/fixtures/cases/` | Case/evidence/review mẫu. |
| `tests/unit/` | Test logic riêng lẻ. |
| `tests/integration/` | Test kết nối API, database, worker và pipeline. |
| `tests/security/` | Test quyền analyst/admin, approval và giới hạn dữ liệu. |
| `tests/recovery/` | Test retry, restart, idempotency, restore và rollback. |
| `tests/acceptance/` | Kiểm tra luồng demo từ event đến review/audit. |
| `artifacts/` | Đầu ra sinh ra từ thí nghiệm/triển khai; không commit binary. |
| `artifacts/models/` | Model file, metadata, checksum và dependency version. |
| `artifacts/experiments/` | Metrics, biểu đồ, config và kết quả run. |
| `artifacts/benchmarks/` | Kết quả đo thô, resource sheet và recovery report. |
| `artifacts/releases/` | Manifest/checksum của từng bản release. |

## 5. Hạ tầng, vận hành và tài liệu

| Thư mục | Dùng để chứa |
| --- | --- |
| `infra/` | Cấu hình đóng gói, triển khai và quan sát hệ thống. |
| `infra/docker/` | Dockerfile tạo image API/Python, web và Spark. |
| `infra/caddy/` | Caddyfile định tuyến SPA/API và HTTPS. |
| `infra/monitoring/` | Cấu hình giám sát. |
| `infra/monitoring/prometheus/` | Cấu hình thu thập metrics. |
| `infra/monitoring/grafana/` | Dashboard và nguồn dữ liệu metrics. |
| `infra/deploy/` | Script/manifest phục vụ staging, production và release. |
| `infra/scripts/` | Script hạ tầng, backup, restore và smoke test. |
| `docs/` | Tài liệu dự án, gồm file hướng dẫn này. |
| `docs/architecture/` | Sơ đồ và quyết định kiến trúc. |
| `docs/api/` | Contract, endpoint và ví dụ API. |
| `docs/data/` | Dataset, dictionary, nguồn/giấy phép và lineage. |
| `docs/database/` | ERD, bảng, index, partition và migration. |
| `docs/operations/` | Runbook deploy, backup, restore, rollback và sự cố. |
| `docs/experiments/` | Protocol, kết quả, giới hạn và khả năng tái lập. |
| `docs/demo/` | Kịch bản trình diễn, ảnh và video dự phòng. |
| `docs/report/` | Nội dung báo cáo khóa luận và tài liệu bảo vệ. |
| `backups/` | Bản sao phục hồi local; không commit dữ liệu backup. |
| `backups/postgres/` | PostgreSQL dump. |
| `backups/object-storage/` | Bản sao object/model/manifest cần khôi phục. |
| `backups/restore-drills/` | Đầu ra của lần diễn tập restore. |
| `.secrets/` | Secret local theo môi trường; `.gitignore` chặn nội dung. |
| `.secrets/dev/` | Credential phát triển. |
| `.secrets/staging/` | Credential staging. |
| `.secrets/prod/` | Credential production, chỉ tạo trên môi trường triển khai thích hợp. |

`.git/` là thư mục nội bộ của Git; không sửa thủ công. `.gitkeep` là file rỗng để Git ghi nhận thư mục chưa có nội dung.

## 6. File Docker sẽ đặt ở đâu?

Theo mục 19.3 của roadmap, **Docker Compose và các file môi trường nằm ở gốc repository**; Dockerfile và Caddyfile nằm trong `infra/`. Hiện các file trong bảng dưới đây **chưa được tạo**.

| Đường dẫn dự kiến | Vai trò |
| --- | --- |
| `compose.yaml` | Stack nền: PostgreSQL, API, web, tools và detection worker. |
| `compose.dev.yaml` | Cổng localhost và thiết lập phát triển. |
| `compose.bigdata.yaml` | Kafka, object storage, Spark và outbox. |
| `compose.realtime.yaml` | Redis, streaming, risk worker và Agent. |
| `compose.prod.yaml` | Thiết lập triển khai, reverse proxy HTTPS và restart policy. |
| `.dockerignore` | Loại secret, dữ liệu lớn, artifact và cache khỏi Docker build context. |
| `.env.example` | Mẫu tên biến môi trường, không chứa credential thật. |
| `.env.dev`, `.env.staging`, `.env.prod` | Cấu hình local theo môi trường; không commit credential. |
| `infra/docker/api.Dockerfile` | Image Python cho API, tools và workers. |
| `infra/docker/web.Dockerfile` | Build React và phục vụ file tĩnh. |
| `infra/docker/spark.Dockerfile` | Spark runtime và dependency đã chốt phiên bản. |
| `infra/caddy/web.Caddyfile` | Route web SPA và API nội bộ. |
| `infra/caddy/edge.Caddyfile` | Điểm vào HTTPS của môi trường triển khai. |
| `.secrets/{dev,staging,prod}/` | File secret được Compose mount lúc chạy, không đưa vào image hay Git. |

Ví dụ khi tạo cấu hình phát triển, chạy lệnh từ **gốc repository**: `docker compose --env-file .env.dev -f compose.yaml -f compose.dev.yaml up -d`. Lệnh này chỉ có thể dùng sau khi các file Compose, env, Dockerfile và mã nguồn liên quan đã được triển khai.
