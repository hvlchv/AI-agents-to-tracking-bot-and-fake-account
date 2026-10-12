# Chính sách nhãn bot và lạm dụng ưu đãi

## Phạm vi

Hệ thống đánh giá hai mục tiêu riêng: `bot_label` (tự động hóa hành vi) và `abuse_label` (lạm dụng ưu đãi). Đơn vị gán nhãn là một actor trong một cửa sổ 5 phút, có dạng `[window_start, window_end)`. Mốc thời gian được lưu theo UTC; event đúng `window_end` thuộc cửa sổ tiếp theo. Mỗi nhãn chỉ nói về actor và cửa sổ tương ứng, không mặc nhiên áp dụng cho toàn bộ lịch sử tài khoản.

| Trường nhãn | `1` — positive | `0` — negative đã biết | `unknown` — chưa xác minh |
| --- | --- | --- | --- |
| `bot_label` | Actor giả lập hoạt động tự động trong cửa sổ được xét. | Actor giả lập được tạo theo kịch bản thao tác người dùng trong cửa sổ được xét. | Public REES46 hoặc dữ liệu chưa có căn cứ xác minh việc tự động hóa. |
| `abuse_label` | Kịch bản giả lập có hành vi lạm dụng ưu đãi theo chính sách đã định nghĩa, trong cửa sổ được xét. | Kịch bản kiểm soát không thực hiện hành vi lạm dụng đó trong cửa sổ được xét. | Nguồn không có đủ thông tin nghiệp vụ hoặc chưa được xác minh. |

Hai nhãn độc lập: bot có thể không lạm dụng ưu đãi, và hành vi lạm dụng có thể do người thao tác. Không suy `abuse_label` từ `bot_label` hoặc ngược lại. Riêng `abuse_label = 1` chỉ hợp lệ khi manifest kịch bản ghi rõ quy tắc ưu đãi bị vi phạm; nhiều tài khoản dùng chung IP hoặc thiết bị, tự nó, chưa đủ để kết luận lạm dụng.

## Nguồn nhãn và cách sử dụng

- **REES46 public:** giữ cả hai nhãn là `unknown` nếu chưa có xác minh độc lập. Không đổi `unknown` thành `0`, không coi mọi user public là legitimate và không dùng các dòng này như nhãn âm để tính precision/recall cho bot hay abuse.
- **Simulator:** lưu ground truth trong `data/labels/<run_id>/`, tách khỏi `data/synthetic/<run_id>/` chứa event. Mỗi nhãn gắn `run_id`, actor ID, target, `window_start`, `window_end`, giá trị nhãn và lý do/kịch bản. Chỉ gán `0` hoặc `1` cho target và cửa sổ mà kịch bản thật sự xác định; các trường hợp còn lại là `unknown` hoặc không tạo nhãn theo quy ước của bộ dữ liệu đánh giá.
- **Review của analyst:** lưu trong bảng review cùng người thực hiện, thời gian, kết luận và lý do. Review là nhận định điều tra, có thể sai hoặc chưa đủ bằng chứng; không ghi đè ground truth của simulator. `inconclusive` không được chuyển thành nhãn `0`.

Event payload, feature phục vụ inference, case và API dashboard không chứa `bot_label`, `abuse_label`, `scenario_id`, seed hoặc ground truth. Chỉ tác vụ đánh giá được cấp quyền mới đọc vùng nhãn và ghép theo đúng `run_id`/actor/target/cửa sổ. Báo cáo kết quả trên dữ liệu mô phỏng phải ghi rõ phạm vi mô phỏng; kết quả đó không chứng minh khả năng nhận diện gian lận ngoài thực tế.
