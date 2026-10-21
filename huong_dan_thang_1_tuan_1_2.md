# Hướng dẫn thực hành tháng 1 — Tuần 1 và tuần 2

**Đề tài:** Xây dựng hệ thống phát hiện bot và hỗ trợ điều tra gian lận trong thương mại điện tử dựa trên phân tích dữ liệu lớn.

**Phạm vi:** nền tảng MVP trên PostgreSQL, REES46 multi-category sample, simulator có nhãn, API, dashboard case/review, audit và transactional outbox. Dữ liệu lớn và job lớn đặt trên server; laptop giữ code và sample nhỏ. Chưa triển khai Kafka/Spark/MinIO trong hai tuần này.

**Cách dùng:** làm theo ngày 1–10, mỗi tuần 5 ngày chính; cuối tuần dành cho đối soát và sửa lỗi. Giả định khoảng 25–35 giờ/tuần. Thời gian là ngân sách đề xuất, không phải cam kết tốc độ. Nếu chậm, giảm UI và số kịch bản; giữ kiểm tra tính đúng dữ liệu/quyền/transaction.

**Trạng thái tài liệu:** hướng dẫn thiết kế và các đoạn mã khởi đầu, không phải một ứng dụng hoàn chỉnh đã triển khai. Các script `make_sample.py`, `import_sample.py`, `bootstrap_role.py`, cấu hình Compose và API health/ready được cung cấp để bạn tạo file. Những endpoint/module khác có bảng yêu cầu để bạn implement. Lệnh bắt đầu bằng đường dẫn module riêng chỉ chạy sau khi module đó đã được tạo theo mô tả. Không coi case fixture là kết quả detection thật.

## 1. Đích đến sau 14 ngày

Bạn cần trình diễn được:

1. Đọc một đoạn REES46 → chuẩn hóa → import PostgreSQL.
2. Import lại cùng sample không tăng số event.
3. Đọc timeline đúng người dùng, đúng khoảng thời gian, có phân trang.
4. Sinh bốn kịch bản giả lập có seed, lưu event và nhãn riêng.
5. Đăng nhập analyst/admin; xem case mẫu và bằng chứng tham chiếu event có thật.
6. Gửi kết luận review; lưu người thực hiện, thời điểm, lý do và audit trong cùng transaction.
7. Restart container rồi đọc lại review.
8. Gửi yêu cầu redeem giả lập; lưu kết quả nghiệp vụ cùng outbox, chưa phát Kafka.
9. Có kế hoạch server và số đo dung lượng; nếu chưa có server vẫn tiếp tục được bằng sample tạo trên Kaggle.

Tuần 3 mới tính feature/rules thật; tuần 4 mới thêm Isolation Forest. Hai tuần đầu chuẩn bị đường đi dữ liệu và công cụ điều tra, không cần model ngay.

## 2. Thuật ngữ ở mức dễ hiểu

| Thuật ngữ | Nghĩa trong project |
|---|---|
| MVP | Phiên bản nhỏ nhất có thể chứng minh chức năng chính hoạt động |
| Vertical slice | Một lát cắt chạy xuyên suốt: event → dữ liệu → case → review; mỗi lớp làm ít nhưng nối được với nhau |
| Event | Một hành động ghi nhận được, ví dụ xem sản phẩm lúc 10:00:03 |
| Contract | Quy ước chung về tên trường, kiểu dữ liệu và ý nghĩa để các module hiểu giống nhau |
| Schema | Cấu trúc bảng, cột, kiểu và ràng buộc trong database |
| Migration | File có phiên bản dùng để tạo/thay đổi schema, giúp máy khác dựng cùng cấu trúc |
| User-window | Một người dùng trong một khoảng thời gian, ở đây 5 phút |
| Warm-up | Đoạn lịch sử trước thời gian đánh giá để tính feature quá khứ; không lấy dữ liệu tương lai |
| Feature | Số đo tổng hợp từ hành vi, ví dụ số lượt xem trong 5 phút |
| Assessment | Một lần hệ thống đánh giá một người/cửa sổ cho một mục tiêu |
| Evidence | Bằng chứng cụ thể gắn với assessment/case, có thời gian và event tham chiếu |
| Case | Hồ sơ điều tra gom đánh giá, bằng chứng và kết luận |
| Analyst | Người xem và kết luận case |
| Fixture | Dữ liệu/case được chuẩn bị trước để thử luồng ứng dụng, chưa phải model phát hiện |
| Manifest | File ghi nguồn, cách lấy mẫu, thời gian, checksum, count và phiên bản |
| Idempotency | Gửi lại cùng yêu cầu không tạo thêm một kết quả nghiệp vụ |
| Staging | Bảng tạm tiếp nhận dữ liệu trước khi đối soát và đưa vào bảng chính |
| COPY | Cách PostgreSQL nạp nhiều dòng hiệu quả hơn gửi từng INSERT |
| Transaction | Một nhóm thay đổi cùng thành công hoặc cùng rollback |
| Outbox | Bảng lưu sự kiện cần phát đi, được ghi cùng transaction với thay đổi nghiệp vụ |
| Audit | Lịch sử ai làm gì, lúc nào, lên đối tượng nào |
| Seed | Giá trị khởi tạo để chương trình giả lập tạo lại cùng dữ liệu |
| Cutoff | Mốc thời gian cuối cùng được phép dùng dữ liệu cho một lần đánh giá |
| Namespace | Tiền tố phân biệt ID từ các nguồn, tránh nhầm người dùng public với người dùng giả lập |

Ví dụ cửa sổ: `[10:00:00, 10:05:00)`. Event đúng 10:05 thuộc cửa sổ tiếp theo. Dấu `[` bao gồm đầu; `)` loại trừ cuối. Lưu timestamp UTC; UI có thể hiển thị giờ Việt Nam, nhưng query/window vẫn thống nhất UTC.

## 3. Công nghệ, máy chạy và giới hạn ban đầu

| Thành phần | Lựa chọn | Chạy ở đâu trong hai tuần đầu |
|---|---|---|
| API, chuẩn hóa, simulator | Python 3.12, FastAPI, Pydantic | Container API/tools |
| Database | PostgreSQL 16 | Một container, named volume bền vững |
| Kết nối DB | psycopg 3; SQLAlchemy dùng cho engine/migration | Backend/tools |
| Migration | Alembic | Tools, chạy thủ công trước khi mở API |
| Dataset | REES46 multi-category duy nhất | Dataset đầy đủ trên Kaggle hoặc server; sample trên máy phát triển |
| Đọc sample | Python `csv`/`gzip`; PyArrow khi cần Parquet | Tools/server/Kaggle |
| Dashboard | React, TypeScript, Vite | Dev trên laptop; cuối tuần 2 thử build |
| Đóng gói | Docker Compose | Laptop để phát triển, server để thử staging |
| Kiểm tra | pytest, HTTP client, SQL đối soát | Máy phát triển/tools |

Python/PostgreSQL ở đây là lựa chọn ổn định cho hướng dẫn, không phải yêu cầu dùng bản mới nhất. Dùng Node 22 bản đáp ứng Vite hiện hành (tài liệu Vite yêu cầu tối thiểu 22.12 trong nhánh 22), rồi chốt lockfile. Lưu image digest thực tế sau khi pull; không tuyên bố tag major là bất biến.

Giới hạn khởi đầu đề xuất: public sample 50.000–300.000 event, simulator 5.000–30.000 event. Đây là ngân sách để thử, không ép mọi sample phải đạt số dòng đó. Nếu đoạn thời gian phù hợp có ít dòng, vẫn dùng được. Sau import đo DB/index/WAL và dung lượng Docker thực tế.

Bạn có thể phát triển trên Windows bằng Docker Desktop + WSL2. Các lệnh Bash bên dưới chạy trong WSL hoặc terminal Linux của server, không dán nguyên vào PowerShell. Không chạy job đầy đủ trên laptop nếu mục tiêu là tiết kiệm ổ đĩa.

## 4. Lịch công việc

| Ngày | Thời gian tham khảo | Công việc chính | Sản phẩm cuối ngày |
|---|---:|---|---|
| 1 | 4–6 giờ | Chốt mục tiêu/nhãn, tạo repo, kiểm tra máy, bắt đầu tìm server | scope.md, label-policy.md, repo và inventory |
| 2 | 5–7 giờ | Tạo public sample liên tục theo thời gian và nhóm user | CSV + manifest + checksum |
| 3 | 5–7 giờ | Contract event/feature/evidence, Docker/Python/API nền | Compose hợp lệ, `/health` chạy sau ngày 4 |
| 4 | 5–7 giờ | Schema, Alembic, role DB, readiness | Migration 0001, DB/API khởi động |
| 5 | 5–7 giờ | Import, dedup, API ingestion/timeline và kiểm tra | Import retry đúng, timeline phân trang |
| Cuối tuần 1 | 2–4 giờ | Tổng hợp bằng chứng, sửa lỗi; không thêm UI | Báo cáo nghiệm thu tuần 1 |
| 6 | 5–7 giờ | Simulator bốn kịch bản, nhãn tách biệt | Event/labels/manifest tái lập |
| 7 | 5–7 giờ | Auth/RBAC, case fixture và API case | Login, quyền và case đọc được |
| 8 | 5–7 giờ | Dashboard login/list/detail/timeline | UI hiển thị case có thật trong DB |
| 9 | 5–7 giờ | Review/audit/idempotency/transaction | Review đúng, retry đúng, lỗi rollback |
| 10 | 5–7 giờ | Redeem demo + outbox, build và thử server | Demo nghiệp vụ, persistence, kế hoạch deploy |
| Cuối tuần 2 | 2–4 giờ | Demo toàn luồng, backup/restore thử và chốt | Báo cáo nghiệm thu tuần 2 |

## 5. Ngày 1 — Chốt bài toán và chuẩn bị project

### 5.1. Chốt hai mục tiêu riêng

Viết `docs/label-policy.md`:

| Trường nhãn | 1 | 0 | Unknown |
|---|---|---|---|
| bot_label | Actor giả lập đang hoạt động tự động trong cửa sổ được xét | Actor giả lập được tạo theo kịch bản thao tác người dùng | Public REES46 hoặc dữ liệu chưa xác minh |
| abuse_label | Kịch bản giả lập có hành vi lạm dụng ưu đãi được định nghĩa cụ thể | Kịch bản kiểm soát không thực hiện lạm dụng đó | Nguồn không có đủ thông tin nghiệp vụ |

Bot có thể không lạm dụng; lạm dụng có thể không có bot. `unknown` không chuyển thành 0. REES46 không có nhãn bot/fraud đủ để đánh giá classifier của bạn; đừng mặc định mọi người dùng trong đó là legitimate.

Nhãn mô phỏng chỉ là ground truth của mô phỏng; không chứng minh khả năng nhận diện gian lận ngoài thực tế. Review của analyst lưu riêng, có thể sai/không đủ bằng chứng; không ghi đè nhãn sealed của simulator.

### 5.2. Chốt đơn vị đánh giá

Mỗi assessment có `(source, user_id, target, window_start, window_end)`; target là `bot` hoặc `abuse`. Cửa sổ 5 phút, đầu bao gồm và cuối loại trừ. Tạo assessment cho cửa sổ đủ điều kiện; không bắt buộc tạo dòng cho mọi user không hoạt động.

Ví dụ user xem 40 sản phẩm từ 10:00 đến trước 10:05 → một cửa sổ để đánh giá bot. Đến 10:05 bắt đầu cửa sổ khác. Feature quá khứ 1 giờ, nếu có, chỉ dùng event trước cutoff 10:05. Warm-up thiếu thì gắn quality flag, không lấy dữ liệu sau cutoff để bù.

### 5.3. Chốt 4 case demo

| Case | Câu hỏi điều tra | Nguồn |
|---|---|---|
| Bot đều nhịp | Nhịp tương tác có dấu hiệu tự động hóa không? | Simulator |
| Flash sale | Tốc độ cao có thể do đợt sale hợp lệ không? | Simulator benign |
| Multi-account promo abuse | Nhóm tài khoản có phối hợp redeem một ưu đãi trái chính sách giả lập không? | Simulator có device/coupon |
| Public chưa đủ bằng chứng | Hành vi bất thường nhưng thiếu nhãn/device/IP thì kết luận thế nào? | REES46, giữ unknown |

Case demo không phải tạo bốn kết luận gian lận. Flash sale cần giúp phát hiện false positive; public case có thể kết thúc inconclusive. Cùng IP không đủ để kết luận phối hợp gian lận.

### 5.4. Tạo repo và các thư mục

```bash
mkdir -p apps/api/app apps/dashboard scripts configs docs tests
mkdir -p data/public/sample data/synthetic/events data/synthetic/labels data/manifests
mkdir -p reports backups
touch apps/__init__.py apps/api/__init__.py apps/api/app/__init__.py scripts/__init__.py
```

Các file cần tạo theo hướng dẫn:

| Đường dẫn | Vai trò |
|---|---|
| compose.yaml, Dockerfile, requirements.in, requirements.lock | Môi trường container/Python |
| .env.example, .env, .gitignore, .dockerignore | Cấu hình mẫu và cấu hình riêng |
| apps/api/app/main.py | Health/ready, đăng ký router |
| apps/api/app/contracts.py | EventV1/FeatureV1/EvidenceV1 |
| apps/api/app/routers/{events,timeline,auth,cases,reviews,business}.py | Các API cần implement |
| apps/api/app/services/{ingestion,review,redeem}.py | Logic transaction và idempotency |
| scripts/{make_sample,import_sample,bootstrap_role}.py | Script được cung cấp bên dưới |
| scripts/{simulate,seed_users,seed_cases}.py | Script bạn viết theo contract ngày 6–7 |
| migrations/0001_schema.sql | Schema SQL được cung cấp |
| migrations/versions/*, migrations/env.py, alembic.ini | Alembic tạo và quản lý |
| docs/{scope,label-policy,contracts,server-plan}.md | Quyết định thiết kế |
| reports/week01.md, reports/week02.md | Kết quả kiểm tra, số liệu, lỗi còn lại |

Trong `.gitignore`:

```gitignore
.env
.venv/
data/
backups/
node_modules/
dist/
__pycache__/
.pytest_cache/
*.pyc
```

Trong `.dockerignore`:

```text
.git
.env
.venv
data
backups
reports
**/node_modules
**/dist
**/__pycache__
```

### 5.5. Ghi inventory và bắt đầu tìm server

```bash
df -h
free -h
uname -m
docker version
docker compose version
docker system df
```

Ghi vào `docs/server-plan.md`: RAM, CPU, dung lượng trống, kiến trúc x86_64/ARM64, nơi đặt dữ liệu, người cung cấp server, trạng thái truy cập và deadline cuối tuần 2. Server trường là lựa chọn cần hỏi sớm; cloud chỉ tạo khi bạn đã kiểm tra điều kiện/quota thực tế. Hướng dẫn này không giả định có sẵn một gói free cụ thể.

Fallback: tạo sample trong Kaggle Notebook, tải sample nhỏ về; tiếp tục PostgreSQL local. Hạ tầng chưa có không làm dừng việc viết contract/API.

**Hoàn thành ngày 1 khi:** đọc scope biết rõ sẽ xây dashboard điều tra, không phải website bán hàng; nhãn public unknown; có repo và kế hoạch dữ liệu/server.

## 6. Ngày 2 — Lấy public sample mà không tải cả dataset xuống laptop

### 6.1. Chọn một trong hai đường

**Đường A — chưa có server:** tạo Kaggle Notebook, Add Input bộ REES46 multi-category, kiểm tra file bằng `Path('/kaggle/input').rglob('2019-Oct.csv')`. Chạy sampler trên input đó, ghi CSV/manifest vào `/kaggle/working/sample`. Chỉ tải thư mục output nhỏ. KaggleHub trong Kaggle Notebook dùng input được gắn, khác với chạy trên laptop và tải cache xuống laptop.

**Đường B — có server:** tải riêng tháng 10 bản gzip từ nguồn REES46. Không giải nén thành CSV lớn trên đĩa; script đọc gzip trực tiếp.

```bash
mkdir -p /srv/botguard/raw
curl --fail --location --retry 3 --continue-at - \
  'https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz' \
  --output /srv/botguard/raw/2019-Oct.csv.gz
gzip -t /srv/botguard/raw/2019-Oct.csv.gz
sha256sum /srv/botguard/raw/2019-Oct.csv.gz
```

Nếu chưa có quyền ghi `/srv`, chọn thư mục trong tài khoản server và đổi đường dẫn. `gzip -t` và sampler đều cần đọc file; đó là chi phí I/O có chủ đích, không sinh thêm bản CSV. SHA256 là kiểm tra nội bộ để phát hiện thay đổi bản đã tải; không coi là checksum được tác giả chứng thực khi tác giả không cung cấp checksum đối chiếu.

Nguồn: [Kaggle multi-category](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store), [REES46](https://data.rees46.com/). Không lấy file tháng 10 từ mục cosmetics có cùng tên. Tháng 12 chưa cần trong hai tuần này.

### 6.2. Lấy mẫu theo user và đoạn thời gian liên tục

Không lấy ngẫu nhiên từng dòng rồi dùng mẫu đó để đo nhịp hành vi: bạn sẽ làm mất các event giữa chuỗi. Cách khởi đầu:

- Chọn khoảng `[2019-10-01 00:00 UTC, 2019-10-02 01:00 UTC)`.
- Warm-up: 00:00–01:00 ngày 1.
- Khoảng đánh giá: 01:00 ngày 1 đến 01:00 ngày 2.
- Chọn ổn định một tỷ lệ user bằng hash; giữ tất cả event của các user đó trong cả khoảng trên.
- Bắt đầu 10% user, đo count rồi giảm tỷ lệ nếu vượt ngân sách. Không cắt đuôi CSV giữa một cửa sổ để ép vừa 300.000 dòng.

Mẫu này giữ chuỗi trong khoảng đã chọn, nhưng chưa giữ toàn lịch sử hoặc toàn quan hệ giữa mọi user. Không dùng nó để kết luận graph đầy đủ; session vượt biên phải gắn cờ thiếu lịch sử. Sau này làm model, split theo thời gian/user/run để tránh cùng actor xuất hiện ở train/test không kiểm soát.

### 6.3. Tạo `scripts/make_sample.py`

Script sau chỉ dùng thư viện chuẩn Python, đọc lần lượt từng dòng. Nó quét toàn file để không tự giả định file đã được sắp xếp theo thời gian. Đổi phiên bản dataset khi bản gốc thay đổi nội dung/thứ tự; CSV và gzip tương ứng phải giữ cùng dòng gốc để event ID khớp.

```python
import argparse
import csv
import gzip
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

FIELDS = [
    'event_id', 'source', 'dataset_version', 'event_time', 'user_id',
    'user_session', 'event_type', 'product_id', 'category_id',
    'category_code', 'brand', 'price', 'currency', 'device_id',
    'ip_token', 'coupon_id', 'attributes',
]
# ham utc dung de dong bo thoi gian
def utc(value):
    result = datetime.fromisoformat(value.replace(' UTC', '+00:00').replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Timestamp must include timezone')
    return result.astimezone(timezone.utc)
# ham digest dung de 
def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True)
    p.add_argument('--output-dir', required=True)
    p.add_argument('--source-file', default='2019-Oct.csv')
    p.add_argument('--dataset-version', default='rees46-multi-oct2019-v1')
    p.add_argument('--start', default='2019-10-01T00:00:00Z')
    p.add_argument('--evaluation-start', default='2019-10-01T01:00:00Z')
    p.add_argument('--end', default='2019-10-02T01:00:00Z')
    p.add_argument('--buckets', type=int, default=1000)
    p.add_argument('--keep-buckets', type=int, default=100)
    p.add_argument('--max-rows', type=int, default=300000)
    p.add_argument('--seed', default='public-sample-v1')
    a = p.parse_args()
    start, evaluation_start, end = utc(a.start), utc(a.evaluation_start), utc(a.end)
    if not start < evaluation_start < end:
        raise ValueError('Expected start < evaluation-start < end')
    if not 0 < a.keep_buckets <= a.buckets or a.max_rows <= 0:
        raise ValueError('Invalid sampling limits')
    src, out = Path(a.input), Path(a.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    final, manifest_path = out / 'events.csv', out / 'manifest.json'
    if final.exists() or manifest_path.exists():
        raise FileExistsError('Use a new output directory; do not overwrite a completed sample')
    temp = out / 'events.csv.partial'
    opener = gzip.open if src.suffix == '.gz' else open
    scanned = selected = warmup = 0
    min_time = max_time = None
    began = time.monotonic()
    try:
        with opener(src, 'rt', encoding='utf-8', newline='') as f, temp.open('w', encoding='utf-8', newline='') as g:
            reader = csv.DictReader(f)
            required = {'event_time', 'event_type', 'product_id', 'user_id', 'user_session'}
            if not required.issubset(reader.fieldnames or []):
                raise ValueError('Unexpected source schema')
            writer = csv.DictWriter(g, fieldnames=FIELDS)
            writer.writeheader()
            for ordinal, row in enumerate(reader, start=1):
                scanned += 1
                t = utc(row['event_time'])
                if not start <= t < end:
                    continue
                raw_user = row['user_id']
                if not raw_user:
                    raise ValueError(f'Missing user_id at row {ordinal}')
                h = hashlib.sha256(f'{a.seed}|{raw_user}'.encode()).digest()
                if int.from_bytes(h[:8], 'big') % a.buckets >= a.keep_buckets:
                    continue
                selected += 1
                if selected > a.max_rows:
                    raise ValueError('Sample exceeds max-rows; reduce keep-buckets or change period and rerun')
                event_id = hashlib.sha256(
                    f'rees46_multi|{a.dataset_version}|{a.source_file}|{ordinal}'.encode()
                ).hexdigest()
                event = {k: '' for k in FIELDS}
                event.update({
                    'event_id': event_id,
                    'source': 'rees46_multi',
                    'dataset_version': a.dataset_version,
                    'event_time': t.isoformat(),
                    'user_id': f'rees46_multi:{raw_user}',
                    'user_session': row.get('user_session') or '',
                    'event_type': row['event_type'],
                    'attributes': json.dumps({'source_file': a.source_file, 'source_row': ordinal}),
                })
                for k in ['product_id', 'category_id', 'category_code', 'brand', 'price']:
                    event[k] = row.get(k) or ''
                writer.writerow(event)
                warmup += int(t < evaluation_start)
                min_time = t if min_time is None else min(min_time, t)
                max_time = t if max_time is None else max(max_time, t)
        if selected == 0:
            raise ValueError('Empty sample; check dates and user sampling')
        temp.replace(final)
        manifest = {
            'complete': True, 'source': 'rees46_multi',
            'dataset_version': a.dataset_version, 'event_contract': 'event.v1',
            'source_file': a.source_file, 'source_bytes': src.stat().st_size,
            'source_sha256': digest(src), 'sample_sha256': digest(final),
            'source_rows_scanned': scanned, 'sample_rows': selected,
            'warmup_rows': warmup, 'evaluation_rows': selected - warmup,
            'selection_start': start.isoformat(),
            'evaluation_start': evaluation_start.isoformat(), 'selection_end': end.isoformat(),
            'actual_min_event_time': min_time.isoformat(), 'actual_max_event_time': max_time.isoformat(),
            'sampling': {'unit': 'user', 'seed': a.seed, 'buckets': a.buckets, 'keep_buckets': a.keep_buckets},
            'label_status': 'unknown', 'sample_bytes': final.stat().st_size,
            'elapsed_seconds': round(time.monotonic() - began, 3),
        }
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
        print(json.dumps({'sample_rows': selected, 'sample_bytes': final.stat().st_size}))
    except Exception:
        temp.unlink(missing_ok=True)
        if not manifest_path.exists():
            final.unlink(missing_ok=True)
        raise

if __name__ == '__main__':
    main()
```

Chạy trên server bằng Python 3.12:

```bash
python scripts/make_sample.py \
  --input /srv/botguard/raw/2019-Oct.csv.gz \
  --output-dir data/public/sample/oct-v1
```

Trong Kaggle, lưu script vào `/kaggle/working/make_sample.py`, tìm đường dẫn file input thật, rồi gọi cùng flags; output đặt `/kaggle/working/sample/oct-v1`. Không đoán tên thư mục slug dưới `/kaggle/input`. Trong notebook dùng `sys.executable` khi gọi subprocess để sử dụng đúng Python kernel.

Event ID dựa trên nguồn + phiên bản + file gốc + số thứ tự dòng. Hai dòng giống nội dung nhưng ở hai vị trí vẫn là hai event; hash toàn nội dung để dedup có thể xóa nhầm event thật. Retry phải dùng cùng file gốc và cùng phiên bản; không đánh số lại từ sample sau khi lọc.

### 6.4. Kiểm tra và ghi số đo

Mở manifest, xác nhận `complete=true`, checksum, count warm-up/evaluation và biên thời gian. Đếm CSV bằng `csv.DictReader`, không mặc định `wc -l - 1` khi CSV có thể chứa newline trong trường quoted. Kiểm tra 20 dòng, user namespace, null category/brand, UTC, không có bot_label/device/IP được bịa thêm.

Đo riêng: bytes gzip nguồn; bytes sample; thời gian đọc gzip và thời gian import. Muốn biết kích thước CSV giải nén mà không lưu file lớn, có thể dùng `gzip -dc FILE | wc -c` trên server, nhưng việc đó vẫn đọc/giải nén toàn file và tốn CPU. Không cần làm nếu đã đủ số đo cho MVP.

**Hoàn thành ngày 2 khi:** chỉ có sample nhỏ trên laptop; sample có manifest, không cắt ngẫu nhiên chuỗi; dữ liệu public chưa có nhãn thật.

## 7. Ngày 3 — Contract và Docker nền

### 7.1. Event contract v1

Viết trong `docs/contracts.md`, sau đó tạo Pydantic `EventV1` trong `contracts.py`.

| Trường | Kiểu/điều kiện | Quy tắc |
|---|---|---|
| event_id | string ổn định, không rỗng | Public dùng ID sampler; collector dùng UUID tạo một lần và giữ khi retry |
| source | rees46_multi / synthetic / demo_app | Không suy nhãn từ source |
| dataset_version | string | Phiên bản nguồn/run, bắt buộc |
| event_time | datetime có timezone | Chuyển UTC, thời điểm hành động thực sự |
| user_id | string có namespace | Khác ID người đăng nhập dashboard |
| user_session | string hoặc null | Giữ null khi thiếu; không bịa session |
| event_type | enum | view/cart/remove_from_cart/purchase/register/login/redeem |
| product_id/category_id/category_code/brand | string hoặc null | Null là chưa biết, không đổi thành số 0 |
| price | Decimal hoặc null | Giữ số thập phân; chưa biết không thành 0 |
| currency | string hoặc null | REES46 không tự gán VND; synthetic có thể định nghĩa VND |
| device_id/ip_token/coupon_id | string hoặc null | Chỉ có khi nguồn cung cấp; IP dùng token phù hợp, không suy từ user_id |
| attributes | object JSON | Metadata; không chứa nhãn, mật khẩu, JWT, external warning score |

Các validator cần viết: timestamp không naive; ID độ dài giới hạn (ví dụ 256); source/enum hợp lệ; price hữu hạn; JSON không chứa `bot_label`, `abuse_label`, `ground_truth`, `isFraud`; unknown field bị từ chối (`extra='forbid'`). Không mặc định thêm nhãn trong event payload. HTTP nhận JSON; CSV importer cần cùng normalization để API và batch hiểu null/time/Decimal giống nhau.

`ingested_at` là timestamp DB nhận event; không thay `event_time` bằng `now()` khi import lịch sử. `received_at` của nguồn nếu không có thì unknown, không bịa để đo end-to-end latency nguồn.

### 7.2. Feature/evidence contract trước, tính sau

FeatureV1 thiết kế sẵn các trường: source/user/window_start/window_end/cutoff/computed_at/feature_version, event_count, view_count, cart_count, purchase_count, distinct_products, mean_gap_seconds, gap_cv, quality_flags và history_observed_seconds. Count là integer; mean/CV có thể null; cutoff bằng window_end cho cửa sổ đóng. Không thêm label/scenario_id vào vector feature.

EvidenceV1: evidence_id, assessment_id, kind, reason_code, observed_value, threshold nếu có, window, event_ids có giới hạn, message, provenance. Lý do viết bằng ngôn ngữ dễ hiểu; số liệu lấy từ dữ liệu thật. Tuần 2 ghi `provenance=fixture`; tuần 3 dùng rule/model version thật.

### 7.3. Dependencies

Tạo `requirements.in`:

```text
fastapi
uvicorn[standard]
pydantic-settings
sqlalchemy>=2,<3
alembic
psycopg[binary]>=3,<4
pyarrow
pwdlib[argon2]
PyJWT
python-multipart
pytest
httpx
ruff
```

Khóa phiên bản sau resolve:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip pip-tools
pip-compile --output-file requirements.lock requirements.in
python -m pip install -r requirements.lock
```

Commit lockfile. Kiểm tra ARM64 trong ngày 10; một lockfile tạo trên x86 không tự chứng minh mọi wheel có cho ARM. Không commit `.venv`.

Tạo `.env` bằng script sau trên máy của bạn; nó không in mật khẩu. `.env.example` chỉ giữ tên biến với placeholder.

```bash
python - <<'PY'
import os, secrets
from pathlib import Path
p = Path('.env')
if p.exists():
    raise SystemExit('.env exists; do not overwrite DB credentials')
p.write_text('\n'.join([
    'POSTGRES_DB=botguard',
    'POSTGRES_USER=fraud_owner',
    'POSTGRES_PASSWORD=' + secrets.token_hex(24),
    'APP_DB_PASSWORD=' + secrets.token_hex(24),
    'JWT_SECRET=' + secrets.token_hex(32),
    'LOCAL_UID=' + str(os.getuid()),
    'LOCAL_GID=' + str(os.getgid()),
]) + '\n')
p.chmod(0o600)
PY
```

Đổi mật khẩu trong `.env` không tự đổi mật khẩu của PostgreSQL đã có volume. Dùng thao tác ALTER ROLE phù hợp nếu cần đổi; không xóa volume để sửa lỗi credential.

### 7.4. Dockerfile và Compose

`Dockerfile`:

```dockerfile
FROM python:3.12-slim-bookworm
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app
COPY requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir -r requirements.lock
COPY . /app
USER 10001:10001
CMD ["uvicorn", "apps.api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

`compose.yaml`:

```yaml
services:
  postgres:
    image: postgres:16-bookworm
    environment:
      POSTGRES_DB: ${POSTGRES_DB:?set POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER:?set POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?set POSTGRES_PASSWORD}
    volumes:
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 5s
      timeout: 3s
      retries: 20
    restart: unless-stopped

  api:
    build: .
    environment:
      PG_DSN: postgresql://fraud_app:${APP_DB_PASSWORD}@postgres:5432/${POSTGRES_DB}
      JWT_SECRET: ${JWT_SECRET:?set JWT_SECRET}
      EXPECTED_SCHEMA_REVISION: "0001"
    ports:
      - "127.0.0.1:8000:8000"
    depends_on:
      postgres:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=3)"]
      interval: 10s
      timeout: 5s
      retries: 5
    init: true
    restart: unless-stopped

  tools:
    build: .
    profiles: [tools]
    user: "${LOCAL_UID:-1000}:${LOCAL_GID:-1000}"
    environment:
      PG_DSN: postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
      APP_DB_PASSWORD: ${APP_DB_PASSWORD:?set APP_DB_PASSWORD}
    volumes:
      - ./data:/data
      - ./reports:/reports
    depends_on:
      postgres:
        condition: service_healthy
    command: ["python", "--version"]

volumes:
  pg_data:
```

DB không publish port ra host; tools/API truy cập bằng hostname `postgres`. API bind localhost để thử qua tunnel. Mật khẩu dạng hex do script tạo giúp tránh vấn đề URL escaping; nếu bạn dùng mật khẩu có ký tự đặc biệt, tạo DSN bằng URL builder thay vì ghép chuỗi.

Healthcheck và `service_healthy` giúp chờ DB sẵn sàng; chúng không thay migration và không đảm bảo mọi chức năng nghiệp vụ đã đúng. Tuần 1 chưa cần frontend container, reverse proxy, Kafka, Spark hay MinIO.

**Hoàn thành ngày 3 khi:** có contract, lockfile, Compose/Dockerfile và thiết kế sample rõ. Chưa chạy `up api` trước khi main.py, migration và DB role đã được tạo ở ngày 4.

## 8. Ngày 4 — Schema, migration và readiness

### 8.1. Schema tối thiểu

Hai loại tài khoản khác nhau: `app_user` là người đăng nhập dashboard; `event_sample.user_id` là người mua/actor trong dữ liệu. Không nối hai ID này làm một.

Trước hết, trong virtualenv đã cài requirements, tạo môi trường Alembic (thư mục migrations chưa tồn tại):

```bash
alembic init migrations
```

Sau đó tạo `migrations/0001_schema.sql`:

```sql
CREATE TABLE app_user (
  id uuid PRIMARY KEY,
  username text NOT NULL UNIQUE,
  password_hash text NOT NULL,
  role text NOT NULL CHECK (role IN ('analyst','admin','viewer')),
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE event_sample (
  event_id text PRIMARY KEY,
  source text NOT NULL CHECK (source IN ('rees46_multi','synthetic','demo_app')),
  dataset_version text NOT NULL,
  event_time timestamptz NOT NULL,
  user_id text NOT NULL,
  user_session text,
  event_type text NOT NULL CHECK (event_type IN ('view','cart','remove_from_cart','purchase','register','login','redeem')),
  product_id text, category_id text, category_code text, brand text,
  price numeric(18,4), currency text, device_id text, ip_token text, coupon_id text,
  attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at timestamptz NOT NULL DEFAULT now(),
  CHECK (jsonb_typeof(attributes) = 'object')
);
CREATE INDEX event_timeline_idx ON event_sample(source, user_id, event_time, event_id);

CREATE TABLE feature_window (
  id uuid PRIMARY KEY,
  source text NOT NULL,
  user_id text NOT NULL,
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  cutoff timestamptz NOT NULL,
  computed_at timestamptz NOT NULL DEFAULT now(),
  feature_version text NOT NULL,
  features jsonb NOT NULL,
  quality_flags jsonb NOT NULL DEFAULT '[]'::jsonb,
  UNIQUE(source,user_id,window_start,window_end,feature_version),
  CHECK (window_end = window_start + interval '5 minutes'),
  CHECK (cutoff = window_end)
);

CREATE TABLE risk_assessment (
  id uuid PRIMARY KEY,
  assessment_key text NOT NULL UNIQUE,
  feature_id uuid REFERENCES feature_window(id),
  source text NOT NULL,
  user_id text NOT NULL,
  target text NOT NULL CHECK (target IN ('bot','abuse')),
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  scoring_method text NOT NULL,
  scoring_version text NOT NULL,
  policy_version text NOT NULL,
  provenance text NOT NULL CHECK (provenance IN ('fixture','rules','model')),
  raw_score double precision,
  risk_level text NOT NULL CHECK (risk_level IN ('LOW','MONITOR','REVIEW')),
  reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (window_end = window_start + interval '5 minutes')
);

CREATE TABLE evidence (
  id uuid PRIMARY KEY,
  assessment_id uuid NOT NULL REFERENCES risk_assessment(id),
  reason_code text NOT NULL,
  evidence_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE casefile (
  id uuid PRIMARY KEY,
  case_key text NOT NULL UNIQUE,
  source text NOT NULL,
  subject_id text NOT NULL,
  target text NOT NULL CHECK (target IN ('bot','abuse')),
  title text NOT NULL,
  status text NOT NULL DEFAULT 'open' CHECK (status IN ('open','in_review','needs_info','closed')),
  assigned_to uuid REFERENCES app_user(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX case_list_idx ON casefile(status,created_at,id);

CREATE TABLE case_assessment (
  case_id uuid NOT NULL REFERENCES casefile(id),
  assessment_id uuid NOT NULL REFERENCES risk_assessment(id),
  PRIMARY KEY(case_id,assessment_id)
);

CREATE TABLE review (
  id uuid PRIMARY KEY,
  case_id uuid NOT NULL REFERENCES casefile(id),
  reviewer_id uuid NOT NULL REFERENCES app_user(id),
  conclusion text NOT NULL CHECK (conclusion IN ('legitimate','confirmed_bot','confirmed_abuse','inconclusive')),
  reason text NOT NULL CHECK (length(reason) BETWEEN 10 AND 2000),
  request_key text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(reviewer_id,request_key)
);

CREATE TABLE audit_log (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  actor_id uuid REFERENCES app_user(id),
  action text NOT NULL,
  entity_type text NOT NULL,
  entity_id text NOT NULL,
  request_key text,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE idempotency_record (
  scope text NOT NULL,
  request_key text NOT NULL,
  payload_hash text NOT NULL,
  response_status integer NOT NULL,
  response_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(scope,request_key)
);

CREATE TABLE redeem_transaction (
  id uuid PRIMARY KEY,
  account_id text NOT NULL,
  coupon_id text NOT NULL,
  status text NOT NULL CHECK (status IN ('accepted','rejected')),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX accepted_redeem_once_idx
  ON redeem_transaction(account_id,coupon_id) WHERE status='accepted';

CREATE TABLE outbox (
  id uuid PRIMARY KEY,
  aggregate_type text NOT NULL,
  aggregate_id text NOT NULL,
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  attempts integer NOT NULL DEFAULT 0
);
CREATE INDEX outbox_pending_idx ON outbox(created_at,id) WHERE published_at IS NULL;

CREATE TABLE action_proposal (
  id uuid PRIMARY KEY,
  case_id uuid NOT NULL REFERENCES casefile(id),
  proposed_by uuid NOT NULL REFERENCES app_user(id),
  action_type text NOT NULL CHECK (action_type IN ('monitor','request_more_info','suggest_block')),
  reason text NOT NULL,
  status text NOT NULL DEFAULT 'proposed' CHECK (status='proposed'),
  created_at timestamptz NOT NULL DEFAULT now()
);
```

`action_proposal` chỉ lưu đề xuất, không thực hiện khóa. Label sealed không nằm trong schema phục vụ API. Feature table có thể trống đến tuần 3. `raw_score=NULL` cho fixture; không bịa score 0.99 hay gọi đó là xác suất bot.

Schema này ưu tiên đọc hiểu cho MVP. Các invariant như kết luận phải đúng target, cutoff/ID hợp lệ và trường labels bị cấm phải kiểm tra thêm ở service/contract. Không coi DDL là toàn bộ kiểm tra nghiệp vụ.

### 8.2. Alembic tạo và chạy migration

Trong virtualenv ngày 3:

```bash
alembic revision --rev-id 0001 -m 'mvp schema'
```

Nếu bạn đã có môi trường Alembic của project, dùng lại nó và chọn revision ID không trùng; cập nhật EXPECTED_SCHEMA_REVISION theo revision mới. Không init lại hoặc ghi đè migration đang có. Hướng dẫn dưới đây giả định project mới với revision 0001.

Thay `migrations/env.py` bằng cấu hình online tối thiểu (hai tuần này không dùng `--sql` offline):

```python
import os
from alembic import context
from sqlalchemy import create_engine

def run_migrations_online():
    url = os.environ['PG_DSN'].replace('postgresql://', 'postgresql+psycopg://', 1)
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=None)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()

if context.is_offline_mode():
    raise RuntimeError('This initial tutorial supports online migration only')
run_migrations_online()
```

Giữ `revision='0001'`, `down_revision=None` trong file revision đã tạo; thay phần upgrade/downgrade:

```python
from pathlib import Path
from alembic import op

def upgrade():
    schema = Path(__file__).resolve().parents[1] / '0001_schema.sql'
    op.execute(schema.read_text(encoding='utf-8'))

def downgrade():
    raise RuntimeError('Use a reviewed forward migration; this tutorial does not drop MVP data')
```

Sau này có SQLAlchemy models thì dùng autogenerate, nhưng vẫn đọc migration trước khi chạy. Không vừa `create_all()` vừa Alembic; schema thay đổi qua migration versioned.

### 8.3. DB role của API

Tạo `scripts/bootstrap_role.py`; tools chạy bằng owner để tạo role app. API dùng role app ít quyền hơn; analyst/admin là vai trò ứng dụng, không phải DB superuser.

```python
import os
import psycopg
from psycopg import sql

with psycopg.connect(os.environ['PG_DSN']) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname='fraud_app'")
        if cur.fetchone() is None:
            cur.execute('CREATE ROLE fraud_app LOGIN')
        cur.execute(sql.SQL('ALTER ROLE fraud_app PASSWORD {}').format(sql.Literal(os.environ['APP_DB_PASSWORD'])))
        cur.execute('GRANT USAGE ON SCHEMA public TO fraud_app')
        cur.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO fraud_app')
        cur.execute('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO fraud_app')
        inserts = [
            'event_sample', 'feature_window', 'risk_assessment', 'evidence',
            'casefile', 'case_assessment', 'review', 'audit_log',
            'idempotency_record', 'redeem_transaction', 'outbox', 'action_proposal',
        ]
        for table in inserts:
            cur.execute(sql.SQL('GRANT INSERT ON {} TO fraud_app').format(sql.Identifier(table)))
        for table in ['casefile', 'outbox']:
            cur.execute(sql.SQL('GRANT UPDATE ON {} TO fraud_app').format(sql.Identifier(table)))
        cur.execute('REVOKE UPDATE, DELETE, TRUNCATE ON audit_log, review FROM fraud_app')
print('Application DB role configured')
```

Role app không sửa/xóa audit/review; tools owner dùng cho migration/import/seed đáng tin. Đây là bảo vệ khỏi lỗi ứng dụng, không phải hệ thống audit chống được DB superuser/host admin. Không tạo endpoint nhận SQL tùy ý.

### 8.4. API health/ready

Tạo `apps/api/app/main.py`:

```python
import os
import psycopg
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title='BotGuard MVP')

@app.get('/health')
def health():
    return {'status': 'ok'}

@app.get('/ready')
def ready():
    try:
        with psycopg.connect(os.environ['PG_DSN'], connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT version_num FROM alembic_version')
                revisions = {r[0] for r in cur.fetchall()}
                if revisions != {os.environ.get('EXPECTED_SCHEMA_REVISION', '0001')}:
                    return JSONResponse(status_code=503, content={'status': 'not_ready'})
                cur.execute('SELECT 1 FROM event_sample LIMIT 1')
        return {'status': 'ready'}
    except psycopg.Error:
        return JSONResponse(status_code=503, content={'status': 'not_ready'})
```

Chạy theo thứ tự:

```bash
docker compose config --quiet
docker compose build api tools
docker compose up -d postgres
docker compose run --rm tools alembic upgrade head
docker compose run --rm tools python -m scripts.bootstrap_role
docker compose up -d api
docker compose ps
curl --fail http://127.0.0.1:8000/health
curl --fail http://127.0.0.1:8000/ready
docker compose run --rm tools alembic current
```

`/health` chỉ chứng minh process sống. `/ready` kiểm tra truy cập DB và schema version. Khi sửa code, ví dụ thêm router, build lại API rồi recreate. Không dùng `docker compose down -v`: `-v` xóa volume database. `down` không có `-v` giữ named volume.

**Hoàn thành ngày 4 khi:** migration revision 0001, app role đọc được DB, `/ready` 200, container DB healthy.

## 9. Ngày 5 — Import, retry và API đọc dữ liệu

### 9.1. Import vào staging rồi merge

`COPY` phù hợp bulk import. API ingestion phục vụ collector/batch nhỏ; không gửi 300.000 event qua từng request. Dữ liệu public gốc không phải giao dịch nghiệp vụ nên không sinh outbox cho mọi dòng lịch sử trong tuần này.

Tạo `scripts/import_sample.py` dưới đây. Đây là importer cho CSV chuẩn do sampler tạo và fixture hợp lệ; contract validation/quarantine cho dữ liệu bẩn cần bổ sung theo mục 9.3. Nó không nạp cả CSV vào RAM.

```python
import argparse
import csv
import json
import os
from datetime import datetime
from decimal import Decimal
import psycopg
from psycopg.types.json import Jsonb
from scripts.make_sample import FIELDS

p = argparse.ArgumentParser()
p.add_argument('--input', required=True)
a = p.parse_args()
cols = ','.join(FIELDS)  # Fixed trusted column list, never from request input.
compared = ','.join(f's.{k}' for k in FIELDS)
existing = ','.join(f'e.{k}' for k in FIELDS)
seen = 0
with psycopg.connect(os.environ['PG_DSN']) as conn:
    with conn.cursor() as cur:
        cur.execute('CREATE TEMP TABLE ingest_stage (LIKE event_sample INCLUDING DEFAULTS) ON COMMIT DROP')
        with open(a.input, encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != FIELDS:
                raise ValueError('Unexpected normalized CSV schema')
            with cur.copy(f'COPY ingest_stage ({cols}) FROM STDIN') as copy:
                for row in reader:
                    values = []
                    for key in FIELDS:
                        value = row[key]
                        if key == 'attributes':
                            value = Jsonb(json.loads(value or '{}'))
                        elif key == 'event_time':
                            value = datetime.fromisoformat(value)
                            if value.tzinfo is None:
                                raise ValueError('event_time needs timezone')
                        elif key == 'price':
                            value = Decimal(value) if value else None
                            if value is not None and not value.is_finite():
                                raise ValueError('price must be finite')
                        else:
                            value = value if value != '' else None
                        values.append(value)
                    copy.write_row(values)
                    seen += 1
        cur.execute('SELECT count(DISTINCT event_id) FROM ingest_stage')
        unique = cur.fetchone()[0]
        if unique != seen:
            raise ValueError('Input contains repeated event IDs; validate before importing')
        cur.execute(f'''SELECT s.event_id FROM ingest_stage s JOIN event_sample e USING(event_id)
                        WHERE ROW({compared}) IS DISTINCT FROM ROW({existing}) LIMIT 1''')
        if cur.fetchone() is not None:
            raise ValueError('Existing event_id has different payload; refusing silent overwrite')
        cur.execute(f'''INSERT INTO event_sample ({cols}) SELECT {cols} FROM ingest_stage
                        ON CONFLICT(event_id) DO NOTHING''')
        inserted = cur.rowcount
print(json.dumps({'input_rows': seen, 'inserted_rows': inserted, 'existing_rows': seen - inserted}))
```

Chạy:

```bash
docker compose run --rm tools python -m scripts.import_sample \
  --input /data/public/sample/oct-v1/events.csv
```

Lần 1 trên DB chưa có sample: inserted = sample_rows. Lần 2: inserted = 0. Event trùng ID nhưng payload khác phải báo lỗi, không tự update event lịch sử. Script này dùng owner/tools và giả định một importer chạy tại một thời điểm. Trước khi có nhiều ingestion writer, xử lý race bằng upsert + kiểm tra payload khi conflict trong transaction; không suy rằng pre-check đơn lẻ đủ bảo đảm dưới concurrent write.

### 9.2. Đối soát bằng SQL

```bash
docker compose exec postgres psql -U fraud_owner -d botguard
```

Trong psql:

```sql
SELECT source, dataset_version, count(*) AS events,
       min(event_time), max(event_time), count(DISTINCT user_id) AS users
FROM event_sample GROUP BY source,dataset_version;

SELECT event_type,count(*) FROM event_sample GROUP BY event_type ORDER BY event_type;
SELECT count(*) FROM event_sample WHERE source='rees46_multi' AND device_id IS NOT NULL;
SELECT count(*) FROM event_sample WHERE source='rees46_multi' AND ip_token IS NOT NULL;

SELECT pg_size_pretty(pg_database_size(current_database()));
SELECT pg_size_pretty(pg_total_relation_size('event_sample'));
```

Hai count device/IP của public phải 0 nếu nguồn không có các trường đó. Nhập `\q` để thoát. Truy vấn theo `source,dataset_version` để count simulator/public không lẫn nhau. Manifest count là số sample, không phải tổng dataset gốc.

### 9.3. API ingestion cần implement

`POST /events/batch`: body `events: list[EventV1]`, giới hạn tối đa 1.000 event/request làm điểm bắt đầu; giới hạn bytes request ở proxy/app khi deploy. Tuần 1 chỉ localhost/tunnel; tuần 2 yêu cầu service credential hoặc admin role riêng cho write, không để viewer/analyst tùy ý nạp event.

Service xử lý:

1. Validate toàn batch; bản MVP từ chối toàn batch nếu có record sai, trả 422 với vị trí lỗi; không im lặng bỏ dòng.
2. Kiểm tra ID lặp trong batch: cùng payload có thể dedup và báo count; khác payload trả 409.
3. Ghi trong transaction; conflict cùng payload → existing; khác payload → 409 và rollback.
4. Trả `received`, `inserted`, `existing`, `rejected` theo quy ước đã chốt. Không trả success khi có phần dữ liệu mất không được báo.
5. Test concurrent retry bằng hai request cùng event_id; tổng DB chỉ tăng 1.

Với file bẩn sau này, thêm quarantine có source_row/error_code/raw_reference, tổng `accepted+rejected=scanned`. Hai tuần đầu có thể fail-fast để dễ debug, nhưng phải nói rõ chế độ đó trong report.

### 9.4. API timeline có keyset pagination

`GET /users/{user_id}/timeline?source=...&start=...&end=...&limit=50&cursor=...`.

- Bắt buộc source và khoảng thời gian; limit 1–200.
- Sắp tăng `(event_time,event_id)` để event cùng giây không bị đổi thứ tự giữa các trang.
- Cursor chứa timestamp + event_id cuối cùng; validate cursor và gắn với filter. Dùng parameter binding, không ghép SQL từ input.
- Lấy `limit+1` dòng để biết còn trang; chỉ trả limit dòng.
- Snapshot của MVP test trên dữ liệu import cố định. Khi collector chạy liên tục, thêm watermark/snapshot để event đến muộn không làm bỏ sót qua các trang.

SQL khi đã có cursor:

```sql
SELECT event_id,event_time,event_type,product_id,user_session,price
FROM event_sample
WHERE source = %(source)s AND user_id = %(user_id)s
  AND event_time >= %(start)s AND event_time < %(end)s
  AND (event_time,event_id) > (%(cursor_time)s,%(cursor_id)s)
ORDER BY event_time,event_id
LIMIT %(fetch_limit)s;
```

Trang đầu bỏ điều kiện cursor. Chọn user từ SQL để thử, không đoán user_id có tồn tại. Tạo fixture 6 event trong cùng một giây, limit=2; ghép 3 trang phải đúng 6 ID, không lặp/thiếu. Event đúng end không được xuất hiện.

**Hoàn thành tuần 1 khi:** import lại không tăng count; ID/payload conflict bị phát hiện; UTC và null đúng; health/ready hoạt động; timeline khớp fixture và có inventory. Commit code/config mẫu/migration/report, không commit data/secrets.

## 10. Ngày 6 — Simulator có nhãn và benign hard case

### 10.1. Thiết kế bốn kịch bản

| Kịch bản | Sinh hành vi | Nhãn kiểm soát |
|---|---|---|
| Normal | Gap ngẫu nhiên, xem/cart/purchase theo session; có người mua nhanh | bot=0, abuse=0 trong phạm vi giả lập |
| Bot đều nhịp | Chuỗi view nhiều sản phẩm, gap gần đều; có jitter, pause, biến thể tốc độ | bot=1, abuse=0 |
| Flash-sale benign | Nhiều người hoạt động nhanh trong cùng đợt sale; có thể cùng IP NAT | bot=0, abuse=0 |
| Multi-account promo abuse | Nhiều account phối hợp register/login/redeem, có device/coupon và thời gian gần nhau | abuse=1 ở cửa sổ lạm dụng; bot theo actor thực tế của kịch bản |

Không gán abuse=1 chỉ vì nhiều account chung IP/device. Định nghĩa chính sách giả lập, ví dụ một ưu đãi chỉ được nhận một lần cho một đối tượng kiểm soát, nhóm tạo nhiều account để lách hạn mức. Normal và benign nên có một số trường hợp dùng chung IP để tránh detector học quy tắc sai “shared IP = fraud”.

Không để mọi bot luôn đúng 1 giây và mọi normal luôn 10 giây; đó chỉ là fixture minh họa. Sinh nhiều seed, tốc độ chồng lấn, timestamp cùng giây và bot nghỉ để tuần 3–4 kiểm tra giới hạn baseline.

### 10.2. Các file phải xuất

`scripts/simulate.py` cần CLI: `--seed`, `--run-id`, `--output-dir`, `--start`, `--duration-minutes`, `--users-per-scenario`. Implement CLI này trước khi chạy ví dụ:

```bash
docker compose run --rm tools python -m scripts.simulate \
  --seed 42 --run-id sim-w02-seed42 \
  --start 2026-01-01T00:00:00Z --duration-minutes 30 \
  --users-per-scenario 20 --output-dir /data/synthetic
```

Các đường output theo run:

- `events/sim-w02-seed42/events.csv`: chỉ EventV1, đúng 17 cột của sampler.
- `labels/sim-w02-seed42/labels.csv`: source/run/user_id/target/window_start/window_end/label/label_reason.
- `manifests/sim-w02-seed42.json`: seed, generator version, tham số, checksum, count từng scenario, khoảng thời gian và contract version.

Tạo `user_id=synthetic:sim-w02-seed42:u0001`; `dataset_version=sim-w02-seed42`; event_id hash run + actor + event ordinal. Device/IP/coupon giả phải có namespace. Không trộn ID user giữa run khác nhau. Cùng seed và run-id + tham số tạo cùng event bytes; muốn run mới đổi run-id. Nếu chứa `now()` hoặc UUID random không kiểm soát thì không còn tái lập.

Tạo nhãn theo user-window/target. Cửa sổ không có actor hoạt động thì có thể không xuất nhãn. Một actor bắt đầu abuse ở phút 20 không được bị gán abuse=1 cho tất cả cửa sổ trước đó.

### 10.3. Ngăn label leakage

Simulator biết scenario/label, nhưng feature builder không được đọc labels. Event CSV không chứa scenario_name, bot_label, abuse_label hay ground_truth. Manifest/labels có thể chứa thông tin đó và nằm ở thư mục riêng; không mount thư mục labels vào API/dashboard. Tools trong Compose có thể nhìn toàn data để phục vụ tác vụ offline, nhưng không cung cấp HTTP endpoint đọc labels.

UI chỉ thấy source/provenance và bằng chứng, không thấy câu trả lời sealed trước review. Khi đánh giá model sau này, evaluator mới join labels theo target/user-window. `source`, prefix user_id, run-id, seed, scenario cũng không dùng làm feature để classifier phân biệt synthetic/public.

### 10.4. Kiểm tra simulator

1. Sinh hai lần vào hai thư mục trống, cùng seed/run/params: checksum event và labels giống nhau.
2. Seed khác: dữ liệu khác; contract và khoảng thời gian vẫn đúng.
3. ID không trùng; event có UTC; nhãn có biên 5 phút và target riêng.
4. Import event bằng importer ngày 5; không import labels vào event table.
5. In report count event/user/window theo scenario từ manifest/evaluator offline.

**Hoàn thành ngày 6 khi:** bốn kịch bản có dữ liệu, nhãn độc lập và kết quả tái lập; chưa cần detector nhận đúng chúng.

## 11. Ngày 7 — Login, quyền và case fixture

### 11.1. Tạo người dùng dashboard

Viết `scripts/seed_users.py` để tạo analyst, admin và viewer thử quyền. Password đọc bằng `getpass` hoặc secret file, không truyền password qua flag/log. Hash bằng `pwdlib`/Argon2; lưu hash, không lưu plaintext. Script chạy qua tools bằng owner; không mở self-registration công khai.

Phân biệt:

- `analyst01` là người đăng nhập xem case.
- `rees46_multi:12345` hoặc `synthetic:...` là subject đang bị đánh giá.
- Không dùng mật khẩu dashboard làm credential DB/Kafka/server.

### 11.2. Auth flow

Implement `POST /auth/token` theo tutorial FastAPI OAuth2 password + JWT; response có access_token/token_type, expiry khoảng 30 phút là cấu hình thử nghiệm. `GET /auth/me` trả id/username/role, không trả hash.

JWT có sub=UUID app_user, exp, issuer và audience; dùng secret đủ mạnh, thuật toán allowlist cố định. Mỗi request đọc user active/role từ DB; không tin role gửi trong body hoặc token để bỏ qua việc thu hồi quyền. Sai password trả thông báo chung; token hết hạn/sai chữ ký trả 401. Thêm rate limit login trước khi expose public.

Frontend MVP giữ access token trong memory, yêu cầu login lại khi refresh; đó là lựa chọn đơn giản cho demo. Nếu chuyển sang cookie, thiết kế HttpOnly/Secure/SameSite và CSRF tương ứng; không đổi cơ chế nửa chừng mà bỏ kiểm tra.

### 11.3. Ma trận quyền

| Thao tác | Viewer | Analyst | Admin |
|---|---|---|---|
| Đọc case/timeline được cấp | Có | Có | Có |
| Claim/review case | Không | Case được gán cho mình hoặc claim hợp lệ | Có |
| Xem case người khác nếu policy hạn chế | Theo policy đọc | Theo policy đọc | Có |
| Gán case sang analyst khác | Không | Không | Có |
| Import/seed qua HTTP | Không | Không | Có, nếu endpoint được mở |
| Tạo action proposal | Không | Có theo quyền case | Có |
| Thực thi khóa account | Không | Không | Chưa triển khai |

Backend kiểm tra quyền; ẩn nút trên UI không đủ. Một cấu hình MVP dễ làm là mọi analyst đọc được case, chỉ analyst được gán hoặc admin mới review. Claim unassigned phải dùng conditional UPDATE/lock để hai analyst không cùng claim thành công.

### 11.4. Định nghĩa risk/reason trước model

`LOW`: chưa có tín hiệu ưu tiên; không chứng nhận user an toàn. `MONITOR`: cần theo dõi thêm. `REVIEW`: cần người xem xét. Kết luận `inconclusive` khác với risk level: đó là kết luận của analyst khi chưa đủ bằng chứng.

Reason codes dự kiến: `HIGH_EVENT_RATE`, `REGULAR_EVENT_INTERVAL`, `SHARED_DEVICE_REDEEM_BURST`, `MISSING_IDENTITY_CONTEXT`. Tuần 2 fixture dùng mã riêng `FIXTURE_REVIEW_DEMO` và dữ liệu minh họa; tuần 3 rules mới phát những reason code detector thực tế.

### 11.5. Tạo case fixture trong DB

Viết `scripts/seed_cases.py`: chọn event có thật trong một cửa sổ 5 phút, tạo risk_assessment provenance=fixture, raw_score=null, evidence tham chiếu những event đó, casefile và case_assessment trong một transaction. Case public có source public; case synthetic có source synthetic; UI hiển thị “Case mẫu — chưa phải kết quả mô hình”.

Seed key ổn định: assessment key = hash(source/user/target/window/fixture_version); case key = hash(source/subject/target/demo_case_key). Seed lại không tạo thêm case. Thêm UUID deterministic nếu cần giữ evidence ID ổn định hoặc skip transaction khi case đã có; không mỗi lần random UUID rồi chèn thêm assessment/evidence.

Evidence không có số đo bịa. Nếu ghi “60 lượt xem” phải đếm đúng event trong cửa sổ; chưa tính mean/CV thì không hiển thị CV. Với multi-account, dùng `subject_id` nhóm synthetic và evidence chứa actor_ids/event_ids có giới hạn; không dựng full graph UI.

### 11.6. API case phải có

| Endpoint | Chức năng |
|---|---|
| GET /cases?status=&source=&target=&limit=&cursor= | List có filter/keyset pagination |
| GET /cases/{case_id} | Case, assessments, evidence, review history; score có thể null |
| POST /cases/{case_id}/claim | Claim nếu chưa gán; quyền kiểm tra ở backend |
| GET /cases/{case_id}/timeline | Timeline subject hoặc actor_ids cho case nhóm, có giới hạn |
| POST /cases/{case_id}/reviews | Ngày 9 implement đầy đủ |
| POST /cases/{case_id}/proposals | Chỉ lưu đề xuất, không thực thi |

List trả source/target/provenance/status/assigned_to/time; không expose labels. Case UUID sai định dạng → 422; không tồn tại → 404; không đủ quyền → 403. CORS dev chỉ cho origin dashboard cụ thể, không bật wildcard credential tùy tiện.

**Hoàn thành ngày 7 khi:** login được, viewer bị từ chối write, seed fixture tái chạy không tăng case, chi tiết case đọc đúng evidence.

## 12. Ngày 8 — Dashboard đủ dùng

### 12.1. Khởi tạo React/TypeScript

Trong root project, với Node đáp ứng Vite:

```bash
npm create vite@latest apps/dashboard -- --template react-ts
cd apps/dashboard
npm install
npm run dev -- --host 127.0.0.1
```

Nếu thư mục đã có nội dung thì init vào thư mục mới, không ghi đè code đang có. Commit package.json/package-lock.json. Chưa cần Tailwind/TanStack Query nếu chưa quen; fetch + CSS đơn giản đủ cho hai tuần đầu. Không mất cả ngày chọn design system.

### 12.2. Cấu trúc UI và gọi API

Tạo `src/api/client.ts`, `src/pages/Login.tsx`, `src/pages/CaseList.tsx`, `src/pages/CaseDetail.tsx`, `src/components/Timeline.tsx`, `src/components/ReviewForm.tsx`.

Dev dùng Vite proxy `/api` → `http://127.0.0.1:8000`, rewrite bỏ `/api`. Frontend gọi `/api/auth/token`, `/api/cases` để tránh hard-code IP server. Proxy dev không tự tồn tại trong build production; ngày 10 dùng reverse proxy hoặc cấu hình URL/CORS rõ ràng.

### 12.3. Màn hình tối thiểu

| Màn hình | Nội dung |
|---|---|
| Login | Username/password, lỗi chung, trạng thái đang gửi |
| Case list | ID ngắn, title, source, target, risk, status, fixture badge, thời gian; filter và next page |
| Case detail | Subject/target/window, người được gán, reason, evidence, timeline, review history |
| Review form | Conclusion dropdown, lý do, nút submit; chặn click khi đang gửi |

Score fixture null hiển thị “Chưa chấm bằng mô hình”, không hiển thị 0% hay 99%. Thời gian UI có timezone rõ “UTC+7” hoặc UTC; không để người xem nhầm giờ nguồn. Public thiếu device/IP hiển thị “Không có trong nguồn”, không là 0 thiết bị.

Xử lý loading/empty/error/401/403. 401 quay lại login, 403 báo không đủ quyền. Không render evidence bằng raw HTML; React text rendering mặc định phù hợp. Có link event/timeline để kiểm tra số đo.

**Hoàn thành ngày 8 khi:** UI đọc backend thật, không hard-code case ở React; xem đúng fixture và timeline; login/token hoạt động.

## 13. Ngày 9 — Review và audit đúng transaction

### 13.1. Contract review

`POST /cases/{case_id}/reviews`, bearer token, header `Idempotency-Key` do frontend tạo UUID một lần cho một lần submit. Body:

```json
{
  "conclusion": "inconclusive",
  "reason": "Nguồn public không có device/IP và chưa đủ bằng chứng kết luận bot."
}
```

Không nhận reviewer_id hoặc created_at từ client; backend lấy actor từ token/DB và thời gian DB. Reason dài 10–2000 ký tự theo schema. Target bot chỉ nhận confirmed_bot; target abuse chỉ nhận confirmed_abuse. legitimate/inconclusive dùng được cho cả hai, nhưng chỉ kết luận phạm vi case/target/window đó, không gán toàn lịch sử account thành normal.

### 13.2. Transaction service

Thứ tự cần implement trong `services/review.py`:

1. Xác thực user active và vai trò.
2. Bắt đầu transaction.
3. Serialize theo idempotency scope `review:<actor UUID>` + key bằng transaction advisory lock hoặc cơ chế row lock/unique reservation đã thiết kế. Tránh hai request cùng key đồng thời đều vượt pre-check.
4. Nếu key đã có: payload hash giống → trả response đã lưu; khác → 409. Hash gồm operation + case_id + canonical body, không chỉ reason.
5. `SELECT ... FROM casefile WHERE id=%s FOR UPDATE` để khóa case; kiểm tra assignment, target và state. Case closed không tự cho sửa kết luận; muốn mở lại cần workflow riêng.
6. Insert review; cập nhật case closed cho legitimate/confirmed_* hoặc needs_info cho inconclusive.
7. Insert audit `CASE_REVIEWED`, actor, case ID, conclusion, review ID; không ghi token/password.
8. Insert idempotency_record chứa response/status trong cùng transaction.
9. Commit; lỗi bất kỳ rollback toàn bộ.

Retry phải kiểm tra idempotency trước kiểm tra “case đã closed” để request đã thành công trả lại kết quả cũ. Unique `(reviewer_id,request_key)` trong DB là lớp bảo vệ cuối, nhưng vẫn phải xử lý conflict đúng response.

Audit/review append-only: sửa kết luận bằng một lần review có workflow mở lại/historical link sau này, không UPDATE review cũ. Đừng cho người dùng xóa audit qua API. Đây là audit nghiệp vụ MVP, chưa phải kho log chống sửa từ quản trị hạ tầng.

### 13.3. Kiểm tra có ý nghĩa

| Kiểm tra | Kết quả bắt buộc |
|---|---|
| Submit hợp lệ | 1 review + 1 audit + case state đổi đúng |
| Retry cùng key/body | Không thêm review/audit; trả cùng review_id |
| Cùng key nhưng khác body/case | 409, không thay dữ liệu |
| Hai request đồng thời cùng key | Chỉ một kết quả nghiệp vụ |
| Viewer submit | 403, DB không đổi |
| Analyst review case gán người khác | 403, DB không đổi |
| Target bot nhưng confirmed_abuse | 422/409 theo contract, DB không đổi |
| Cố tình lỗi insert audit trong test transaction | Review và update case cũng rollback |
| Restart api/postgres | Review vẫn đọc được |
| App role thử UPDATE audit_log | DB từ chối |

Đặt lỗi giả lập rollback trong test/service injection, không tạo endpoint production để gây lỗi. Dùng DB test riêng cho thử lỗi/quyền; không xóa DB đang chứa demo để reset test.

**Hoàn thành ngày 9 khi:** các kiểm tra này có report, không chỉ thấy UI báo “success”.

## 14. Ngày 10 — Business demo, outbox và chuẩn bị server

### 14.1. Demo redeem nhỏ

Implement `POST /demo/redeems`, chỉ dùng account/coupon giả của demo. Quy tắc nhỏ: một account chỉ được accepted một lần cho một coupon. Account ID phải thuộc namespace demo/synthetic cho endpoint này. Không viết catalog, giỏ hàng, thanh toán hay tích hợp cửa hàng thật.

Request có Idempotency-Key và body account_id/coupon_id. Demo endpoint chỉ admin/service credential; không coi JWT analyst là JWT người mua. Cấu hình `DEMO_MODE` rõ khi deploy.

Transaction:

1. Validate account/coupon demo và credential.
2. Serialize idempotency như review, scope gồm operation/service actor.
3. Insert redeem_transaction; unique partial index chặn accepted thứ hai dù hai request key khác nhau chạy đồng thời.
4. Nếu accepted, tạo outbox event `coupon.redeemed.v1`, UUID event ổn định cho retry, aggregate_id=transaction_id.
5. Lưu response idempotency rồi commit.

Nếu conflict chính sách, trả 409 hoặc kết quả rejected theo contract bạn chọn. Với MVP dễ kiểm tra, dùng 409 và không tạo outbox success; retry của một accepted request vẫn trả response accepted trước đó. Nếu muốn lưu rejected transaction thì làm trong transaction riêng đã thiết kế, không để exception unique violation làm commit một transaction đã aborted.

Outbox payload ví dụ:

```json
{
  "event_id": "UUID-duoc-tao-mot-lan",
  "schema_version": "coupon.redeemed.v1",
  "source": "demo_app",
  "transaction_id": "UUID-giao-dich",
  "account_id": "demo_app:account01",
  "coupon_id": "demo:WELCOME",
  "event_time": "2026-01-01T00:00:00Z"
}
```

Ví dụ trên là hình dạng payload, thay UUID/thời gian bằng giá trị thật do backend tạo. Không đưa label vào event. Tuần 2 outbox `published_at=NULL`; publisher Kafka bắt đầu tuần 7. Việc có outbox không đồng nghĩa đã phát event hoặc đã có exactly-once toàn pipeline. Publisher sau này có thể gửi lặp, consumer dedup theo event_id.

### 14.2. Kiểm tra outbox

Accepted mới → 1 transaction + 1 outbox. Retry cùng key → không thêm dòng. Hai key khác nhau cùng account/coupon → tối đa 1 accepted. Lỗi insert outbox → transaction accepted rollback. Không tạo outbox success cho request bị từ chối. Ghi count và transaction ID vào report.

### 14.3. Staging riêng trên server bằng Docker

Server là nơi triển khai API/DB và sau này storage/jobs lớn. Trong hai tuần đầu không cần ba container Spark, không cần ba VM.

1. Kiểm tra `uname -m`, RAM/disk, Docker/Compose; clone code, tạo `.env` mới riêng server.
2. Không copy image chứa dataset/labels/secrets. Clone chỉ code; chuyển sample nhỏ và manifest vào `data/public/sample` nếu cần.
3. Chạy build/migrate/bootstrap/API theo ngày 4. Tools và API phải đọc cùng DB revision.
4. Seed user riêng cho staging bằng password riêng; import sample và seed fixture.
5. API vẫn bind localhost; từ laptop tạo SSH tunnel để xem `/docs`:

```bash
ssh -N -L 8000:127.0.0.1:8000 USER@SERVER_IP
```

Thay USER/SERVER_IP bằng giá trị thật. Khi API local đã dùng cổng 8000, dùng `-L 18000:127.0.0.1:8000` và mở localhost:18000; đổi Vite proxy tương ứng. Không mở PostgreSQL 5432 ra Internet.

Chưa có server: ghi trạng thái fallback, vẫn demo trên máy với sample. Không hứa free server đã được cấp khi chưa có tài khoản/quota.

### 14.4. ARM64 và build smoke

Trên server ARM64, build image native từ Dockerfile và chạy:

```bash
docker compose run --rm tools python -c "import platform, pyarrow, psycopg; print(platform.machine()); print(pyarrow.__version__)"
docker compose run --rm tools python -m pip check
docker compose up -d api
curl --fail http://127.0.0.1:8000/ready
```

Nếu native wheel thiếu, chọn phiên bản có wheel hoặc base/runtime được hỗ trợ; không giả định buildx emulation sửa được mọi dependency. Cross-build multi-architecture chỉ khi cần; lưu kết quả chạy thực tế ARM, không chỉ build thành công trên x86.

### 14.5. Frontend build và cách phục vụ

```bash
cd apps/dashboard
npm ci
npm run build
```

Dev server dùng cho phát triển. Staging public cần Caddy/Nginx phục vụ dist và proxy `/api` đến API nội bộ, HTTPS/domain và auth. Trong hai tuần đầu tunnel/private demo đủ; không bắt buộc mua domain. Khi dùng HTTPS, không gửi login/token qua HTTP public. Proxy build cần cấu hình riêng; Vite dev proxy không đi kèm dist.

### 14.6. Backup và restart

Tại root project:

```bash
docker compose exec -T postgres pg_dump -U fraud_owner -d botguard -Fc > backups/week02.dump
docker compose restart api postgres
docker compose ps
curl --fail http://127.0.0.1:8000/ready
```

`pg_dump -Fc` tạo dump, chưa chứng minh restore thành công. Thử restore vào DB khác trên cùng server/local, không đè DB demo:

```bash
docker compose exec postgres createdb -U fraud_owner botguard_restore_test
docker compose exec -T postgres pg_restore -U fraud_owner \
  -d botguard_restore_test --no-owner --no-acl < backups/week02.dump
docker compose exec postgres psql -U fraud_owner -d botguard_restore_test \
  -c "SELECT count(*) FROM review;"
```

Chọn tên DB test mới nếu đã tồn tại; không dùng `--clean` lên DB demo. Đối chiếu count event/case/review/outbox trước và sau. Dump có password hash và dữ liệu review nên giữ riêng, không commit/public. Cần sao lưu roles/credentials bằng quy trình riêng khi phục hồi sang host mới; dump này không bao gồm toàn bộ thiết lập server.

**Hoàn thành tuần 2 khi:** simulator/labels tái lập; UI thật và quyền đúng; review/audit transaction đúng; restart giữ dữ liệu; outbox chưa phát nhưng ghi atomically; server hoặc fallback được ghi rõ.

## 15. Checklist nghiệm thu và bằng chứng cần lưu

### 15.1. Tuần 1

- [ ] Scope/label policy có target bot và abuse riêng, public unknown.
- [ ] Có sample liên tục, warm-up, user sampling, manifest/checksum/size.
- [ ] Event ID ổn định theo dòng gốc; null/time/currency không bị bịa.
- [ ] Migration 0001 và app role tồn tại.
- [ ] Import lần 1 count khớp; lần 2 inserted=0; payload mismatch bị từ chối.
- [ ] `/health` 200; `/ready` 200 khi DB/schema đúng, 503 khi DB không sẵn sàng.
- [ ] Timeline boundary/cùng giây/pagination khớp fixture.
- [ ] Có disk inventory, server plan và fallback.

`reports/week01.md` ghi: commit, image tag/digest, Python/PG/Compose versions, manifest path/hash, source/sample bytes, count, import seconds, DB size, test outcome và lỗi chưa sửa. Không ghi password/token.

### 15.2. Tuần 2

- [ ] Simulator có normal/bot/flash-sale/multi-account và nhiều seed.
- [ ] Labels không nằm trong event payload/feature/API/UI.
- [ ] Seed cùng tham số tái lập; seed fixture retry không tăng case.
- [ ] JWT expiry/invalid signature/active user và role được kiểm tra.
- [ ] Viewer/analyst sai assignment không write được.
- [ ] UI hiển thị fixture badge, null score, source/target/time/evidence.
- [ ] Review hợp lệ, retry, concurrent retry, payload mismatch và target mismatch đúng.
- [ ] Review/audit rollback cùng nhau; app role không sửa/xóa audit.
- [ ] Redeem accepted cùng outbox; retry không thêm; lỗi outbox rollback nghiệp vụ.
- [ ] Restart giữ review; backup restore thử vào DB khác count khớp.
- [ ] Build frontend và smoke server/ARM hoặc fallback có bằng chứng.

`reports/week02.md` thêm scenario count, seed/run/version, test case/response IDs, case/review/outbox count và kết quả demo. Giữ test cần thiết cho idempotency/transaction/quyền/biên dữ liệu; không viết test chỉ để xác nhận một component render đúng text hard-code.

## 16. Kịch bản demo cuối tuần 2, khoảng 5–8 phút

1. Mở report sample/manifest, giải thích public unknown và synthetic có nhãn riêng.
2. Chạy importer lần 2: inserted=0; đối chiếu count.
3. Đăng nhập analyst, mở case bot fixture; xem timeline/evidence và badge fixture.
4. Mở flash-sale case để cho thấy tốc độ cao có thể hợp lệ, chưa kết luận tự động.
5. Review một case; gửi lại cùng key, số review/audit không tăng.
6. Dùng viewer thử submit: 403.
7. Redeem demo accepted; xem 1 outbox pending, gửi lại không tăng.
8. Restart API/DB, đọc lại review. Kết thúc bằng việc tuần 3 sẽ thay fixture bằng rules có feature thật.

## 17. Lỗi thường gặp

| Hiện tượng | Nguyên nhân hay gặp | Cách kiểm tra/sửa |
|---|---|---|
| Docker chiếm nhiều ổ | Build cache, volume và raw/sample cùng giữ nhiều bản | `docker system df`, inventory; chỉ dọn phần đã xác định, không xóa pg_data |
| API không kết nối DB | Dùng localhost trong container | DSN dùng hostname postgres, kiểm tra network/env |
| Ready 503 dù DB healthy | Chưa migration/role sai/revision sai | alembic current, logs đã mask secret, quyền fraud_app |
| Đổi .env rồi login DB lỗi | Volume giữ credential cũ | ALTER ROLE có kiểm soát; không xóa volume |
| Import lại tăng count | UUID mới mỗi lần/dòng sample đánh số lại | ID từ nguồn gốc ổn định và PK event_id |
| Retry “thành công” nhưng payload khác | DO NOTHING che mất conflict nội dung | So sánh payload và trả lỗi rõ |
| Count không khớp manifest | Lẫn source/version, mẫu incomplete, reject không báo | Filter đúng source/version; đối soát accepted/rejected |
| Timeline thiếu/lặp cùng giây | Cursor chỉ timestamp | Cursor timestamp + event_id, limit+1 |
| Nhịp public nhìn bất thường | Lấy mẫu ngẫu nhiên từng event/biên session | Giữ chuỗi theo user/thời gian; quality flags |
| Detector đo rất tốt trên synthetic | Label/source/run/scenario leakage hoặc pattern quá dễ | Whitelist feature, split run/user/time, thêm benign khó |
| UI success nhưng DB mất review | Transaction/audit riêng, không await request | Cùng transaction, kiểm tra DB và restart |
| npm build khác dev | Lockfile/runtime/proxy khác | npm ci, Node version, cấu hình reverse proxy production |
| ARM build lỗi dependency | Wheel/image không tương thích | Native smoke, pin bản hỗ trợ, ghi kiến trúc |

## 18. Những việc chuyển sang tuần 3–4

Tuần 3: SQL feature v1, kiểm tra biên và warm-up, rules cấu hình được, risk policy và dedup/cooldown case. Tuần 4: Isolation Forest trên feature table, score direction, threshold sơ bộ, đánh giá false positive và chốt vertical slice thật.

Không dùng labels sealed trong feature job. Không biến fixture case thành “model đã phát hiện bot” trong slide/báo cáo. Không nạp full REES46 vào PostgreSQL để chứng minh Big Data; tháng 2 chuyển raw/clean/features lớn sang storage server và Spark/Kafka theo roadmap.

## 19. Nguồn kỹ thuật để đối chiếu

Các nguồn chính thức được kiểm tra ngày 12/10/2026; code trong hướng dẫn là thiết kế riêng của project, không sao chép nguyên tutorial.

- [REES46 multi-category — Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store): nguồn và ý nghĩa dataset.
- [REES46 data portal](https://data.rees46.com/): các file gzip theo tháng; chọn mục marketplace.
- [KaggleHub](https://github.com/Kaggle/kagglehub): khác biệt tải cache ngoài Kaggle và attach input trong Kaggle Notebook.
- [PostgreSQL 16 COPY](https://www.postgresql.org/docs/16/sql-copy.html): bulk transfer và STDIN.
- [PostgreSQL 16 INSERT](https://www.postgresql.org/docs/16/sql-insert.html): ON CONFLICT và RETURNING.
- [psycopg 3 COPY](https://www.psycopg.org/psycopg3/docs/basic/copy.html): write_row và transaction.
- [Alembic tutorial](https://alembic.sqlalchemy.org/en/latest/tutorial.html): migration versioned.
- [Docker Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/): healthcheck và depends_on.
- [FastAPI OAuth2/JWT](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/): password hashing và access token.
- [Vite guide](https://vite.dev/guide/): runtime và khởi tạo React/TypeScript.

**Giới hạn kiểm chứng của tài liệu:** sampler đã được thử bằng CSV/gzip fixture nhỏ, gồm boundary/warm-up/user sampling/retry ID và trường hợp vượt ngân sách; các file Python được kiểm tra cú pháp và YAML được parse. Môi trường tạo hướng dẫn không có Docker/PostgreSQL nên chưa chạy integration của Compose, migration, COPY, auth/review/outbox hoặc ARM64. Chỉ ghi “đạt” cho các bước đó sau khi bạn chạy và có report trên máy/server của mình.
