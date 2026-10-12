# Kế hoạch server và dung lượng

## Trạng thái hiện tại

Hiện **chưa có server**. Cần hỏi khả năng cấp server của trường sớm trong tuần 1; chỉ xem xét cloud sau khi kiểm tra điều kiện sử dụng, quota và chi phí thực tế. Hạn chốt lựa chọn là **cuối tuần 2 tính từ ngày bắt đầu kế hoạch**. Chưa có thông số CPU/RAM/disk hay tài khoản truy cập server để xác nhận.

| Mục | Máy phát triển hiện tại | Server dự kiến |
| --- | --- | --- |
| Người/đơn vị cung cấp | Máy cá nhân | Chưa xác định; cần hỏi trường |
| Trạng thái truy cập | Đang làm việc tại repo local | Chưa có server/tài khoản |
| OS/kiến trúc | Windows/AMD64; Docker Engine chạy Linux/AMD64 | AMD64 dự kiến, chưa xác nhận trên server |
| CPU/RAM | Intel Core i7-12700H, 14 core/20 luồng; RAM 15,73 GiB | Chưa biết |
| Disk dữ liệu/dung lượng trống | Repo ở ổ `G:`; còn 72,5 GiB theo `Get-PSDrive`. Nơi lưu Docker image/volume trên host chưa xác nhận | Chưa biết nơi mount data disk |
| Docker/Compose | Docker Desktop 4.85.0; Engine/CLI 29.6.2; Compose v5.3.1; Engine kết nối được | Chưa kiểm tra |
| Truy cập staging | Localhost trên máy phát triển | Chưa xác định |

## Lệnh kiểm tra trên PowerShell của bạn

Chạy trong PowerShell thông thường trên máy của bạn, tại root repo:

```powershell
$cpu = Get-CimInstance Win32_Processor
$machine = Get-CimInstance Win32_ComputerSystem
$cpu | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors
[math]::Round($machine.TotalPhysicalMemory / 1GB, 2) # RAM, GiB
$env:PROCESSOR_ARCHITECTURE                       # AMD64 hoặc ARM64
Get-PSDrive -Name G | Select-Object Name, Root, @{Name='FreeGiB';Expression={[math]::Round($_.Free / 1GB, 2)}}
Get-Location
Resolve-Path .\data
docker version
docker compose version
docker system df
```

Các số liệu máy phát triển trong bảng được lấy từ PowerShell của chủ máy. `Get-PSDrive` đo dung lượng trống của ổ chứa repo/sample; nếu Docker Desktop lưu image/volume trên ổ khác, kiểm tra cả ổ đó trong Docker Desktop và bằng `Get-PSDrive -PSProvider FileSystem`. Đừng ghi giá trị `0` nếu lệnh bị hạn chế quyền hoặc không trả kết quả đúng. Nếu `Get-CimInstance` bị từ chối, xem CPU/RAM trong **Task Manager → Performance**.

Khi được cấp server Linux, chạy trực tiếp trên server: `uname -m`, `nproc`, `free -h`, `df -h`, `lsblk`, `docker version` và `docker compose version`; xác nhận data disk đã mount trước khi tải raw dataset. Ghi cả người cấp server, cách SSH và quyền truy cập, không ghi mật khẩu/khóa vào tài liệu.

## Fallback trong hai tuần đầu

Gắn REES46 vào Kaggle Notebook, tạo sample liên tục có warm-up và manifest ở đó. Chỉ tải sample nhỏ cùng manifest về `data/sample/` và `data/manifests/`; tiếp tục PostgreSQL local cho API và case/review. Giữ raw dataset lớn ở Kaggle hoặc server về sau. Chưa có server không chặn việc viết contract, migration và API.
