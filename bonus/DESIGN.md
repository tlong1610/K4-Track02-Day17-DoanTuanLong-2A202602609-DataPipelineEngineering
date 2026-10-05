# Bonus B2 — Flywheel dữ liệu cho chatbot CSKH tiếng Việt

## 1. Bài toán và ràng buộc

Một công ty SaaS Việt Nam (khoảng 50.000 khách hàng doanh nghiệp nhỏ) chạy chatbot CSKH
trả lời bằng RAG trên kho hướng dẫn sử dụng và ticket cũ. Đội sản phẩm muốn mỗi tuần
chatbot tốt hơn nhờ chính dữ liệu nó sinh ra: trace hội thoại, nút 👍/👎, ticket được
chuyển cho nhân viên, và câu trả lời nhân viên viết lại.

**Người dùng dữ liệu:** (a) đội ML — cần eval set ổn định và dữ liệu SFT/DPO hằng tuần;
(b) RAG index — cần tài liệu và ticket đã giải quyết, cập nhật trong ngày;
(c) agent định tuyến — cần feature theo người dùng (số lần 👎 gần đây, gói cước).

**Vì sao khó:**
- Khoảng 30.000 hội thoại/ngày, tiếng Việt có/không dấu lẫn lộn, teencode, tiếng Anh xen kẽ.
- Hội thoại chứa PII (tên, số điện thoại, mã số thuế, số tài khoản) và chịu Nghị định
  13/2023 về bảo vệ dữ liệu cá nhân: khách có quyền yêu cầu xoá.
- Feedback đến muộn: app mobile offline, nhân viên đóng ticket sau 1–5 ngày.
- Đội data chỉ có 2 người, ngân sách hạ tầng dưới 1.500 USD/tháng.

## 2. Sơ đồ kiến trúc

```
App/Web chat ──OTel traces──┐
Postgres tickets ──CDC─────┼──▶ Bronze (Parquet, theo ngày, bất biến)
Feedback events (Kafka) ───┘          │
                                      ▼  PII gate: regex + NER tiếng Việt
                             Silver (MERGE theo khoá, tombstone khi xoá,
                                     quarantine cho record sai schema)
              ┌───────────────────────┼─────────────────────────┐
              ▼                       ▼                         ▼
   gold_eval_golden (đóng băng,  gold_sft_dpo_vYYYYWW     gold_doc_chunks
   version theo quý)             (decontaminated,          (embedding cache
                                  snapshot theo tuần)       hash + model)
              │                       │                         │
              └──▶ eval harness ◀─────┘                    vector index ──▶ chatbot
```

## 3. Năm câu hỏi then chốt

### Q2 — Batch hay streaming?
**Quyết định:** batch hằng ngày (DAG giống lab này) cho Silver/Gold, cộng một đường
micro-batch 15 phút chỉ cho RAG index khi có tài liệu hướng dẫn mới.
**Đánh đổi:** streaming (Kafka → Flink) cho dữ liệu tươi trong vài giây nhưng tốn chi phí
vận hành, và đội 2 người không trực on-call nổi. Dữ liệu train chỉ dùng mỗi tuần, nên độ tươi
"ngày" là đủ. Chỉ RAG index thật sự cần tươi hơn, vì khách hỏi về tính năng vừa ra mắt.
**Chọn batch** vì chạy lại một ngày cũ là idempotent và kiểm chứng được bằng checksum.
Với streaming, việc backfill khó hơn nhiều.

### Q4 — Hợp đồng dữ liệu và chất lượng
**Quyết định:** Pydantic contract ở cửa Bronze→Silver; record lỗi vào `quarantine_*` kèm
lý do, run không dừng. Cảnh báo Slack khi tỷ lệ quarantine của một ngày vượt 3 lần trung vị
14 ngày gần nhất.
**Đánh đổi:** dừng toàn bộ pipeline khi gặp record lỗi (fail-fast) thì an toàn hơn nhưng một
event hỏng từ một phiên bản app cũ sẽ chặn cả ngày dữ liệu. Quarantine giữ pipeline chạy,
đổi lại có nguy cơ không ai đọc bảng quarantine. Vì vậy ngưỡng cảnh báo là bắt buộc.

### Q5 — Train/serve parity và point-in-time
**Quyết định:** feature cho agent định tuyến dựng bằng `ASOF JOIN` theo `event_time`, với
lookback bằng ceil(P99 độ trễ) đo từ Bronze (giống lab: 3 ngày). Snapshot training có
version theo tuần, dựng lại từ Bronze "as of" ngày chốt, không bao giờ sửa snapshot cũ.
**Đánh đổi:** join "giá trị mới nhất" đơn giản hơn và nhanh hơn, nhưng rò rỉ tương lai: model
học từ số 👎 xảy ra *sau* thời điểm dự đoán, nên offline metric đẹp còn online thì tệ. Point-in-time
tốn thêm lưu trữ, đổi lại metric offline đáng tin.

### Q7 — Flywheel không tự đầu độc
**Quyết định:** eval golden set (khoảng 500 câu, nhân viên duyệt) đóng băng theo quý. Mỗi cặp
DPO `(prompt, chosen, rejected)` phải qua decontamination hai lớp: khớp chính xác sau khi chuẩn
hoá (bỏ dấu, lowercase), sau đó so 13-gram và cosine embedding > 0.92 với eval set.
**Đánh đổi:** chỉ khớp chính xác thì rẻ nhưng bỏ lọt câu viết lại ("trả hàng" và "hoàn hàng").
Embedding similarity bắt được paraphrase nhưng loại nhầm cả câu hỏi phổ biến hợp lệ. Tôi chấp
nhận mất khoảng 5% dữ liệu train để eval không "nói dối".

### Q10 — PII và quyền xoá trong bối cảnh Việt Nam
**Quyết định:** chốt PII ở Bronze→Silver: regex (email, SĐT +84/0, CCCD 12 số, MST) cộng
một mô hình NER tiếng Việt cho tên người và địa chỉ. Bronze mã hoá theo khoá riêng của từng
khách hàng. Khi khách yêu cầu xoá, Silver thành tombstone, Gold được dựng lại, và khoá của khách
bị huỷ (crypto-shredding) nên Bronze và các snapshot cũ không còn đọc được văn bản của họ.
**Đánh đổi:** xoá cứng trong Bronze phá vỡ tính bất biến và khả năng tái lập. Không xoá thì vi
phạm pháp luật. Crypto-shredding giữ cấu trúc bất biến nhưng tăng độ phức tạp quản lý khoá (KMS).
Chất lượng NER được đo bằng recall trên 1.000 câu gán nhãn tay mỗi quý, mục tiêu ≥ 0.95.

## 4. Phương án bị loại

**Lambda architecture với Spark Structured Streaming và Delta Lake trên cluster riêng.**
Bị loại vì: (1) quy mô 30.000 hội thoại/ngày chỉ khoảng vài GB, nên DuckDB hoặc dbt trên một
máy xử lý trong vài phút; (2) chi phí cluster và công vận hành vượt ngân sách 1.500 USD và sức
của đội 2 người; (3) hai code path (batch và speed layer) làm mất tính chất "backfill = cùng code
path" và rất khó chứng minh bằng checksum. Tôi sẽ xem xét lại khi dữ liệu tăng khoảng 100 lần,
hoặc khi agent định tuyến cần feature dưới 1 phút.

## 5. Điểm sẽ vỡ đầu tiên khi scale

Ở 10 lần quy mô, bottleneck đầu tiên là **chi phí embedding và LLM gán nhãn**, không phải compute
SQL. Cache theo `hash(text) + model_version` (như `gold_doc_chunks` và bonus B1) giúp lần chạy
lại không tốn tiền. Ở 100 lần, bottleneck chuyển sang small files trong Bronze theo giờ; cần
compaction hằng đêm và một định dạng bảng có snapshot (Iceberg/Delta, Ngày 18).
