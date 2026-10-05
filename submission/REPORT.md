# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Đoàn Tuấn Long / 2A202602609
**Repo:** https://github.com/tlong1610/K4-Track02-Day17-Data-Pipeline-Engineering
**Commit bài nộp:** `5998f60` (code + output; commit kế tiếp chỉ ghi hash này vào REPORT)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5): đọc code, đề xuất cách sửa 3 lỗi và bonus B1, soạn nháp REPORT và `bonus/DESIGN.md`. Tôi đã review từng dòng sửa và tự chạy lại toàn bộ kiểm tra; output bên dưới lấy từ lần chạy đó.
**Nguồn tham khảo khác (nếu có):** slide Ngày 17; tài liệu Debezium (định dạng event), DuckDB `MERGE INTO`, dbt microbatch.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: `24 rows for 12 tickets`; T-91 có 3 hàng (`low/open`, `high/open`, `high/closed/bug`); `gold_doc_chunks` 22 hàng / 9 chunk; rerun3 FAIL (`gold_doc_chunks` đổi checksum sau mỗi lần chạy lại) | verify: `gold_feature_daily` ≠ full recompute (`c50b8851affe != 8630e04a61d1`); u05 ngày 08-12 = `(2, 0)` thay vì `(5, 1)`; `LOOKBACK_DAYS=0 < 3` | T-97 vẫn `is_deleted = False`, còn `user_id`, tên, nội dung; còn 1 hàng trong snapshot `v2026-08-16` và 2 chunk trong RAG index |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT` mỗi batch: không có khoá, không so LSN, nên chạy lại batch cũ chèn thêm một hàng trạng thái cũ | `LOOKBACK_DAYS = 0` dựa trên giả định "event tới trong vài giây". Thực tế event offline của u05 tới trễ 3 ngày, nên run 08-15 không tính lại partition 08-12 | `staging.py` lấy `ticket_id` chỉ từ `after`. Với `op='d'` thì `after = null`, nên `ticket_id` NULL và bị lọc ở `WHERE ticket_id IS NOT NULL`: bản ghi xoá biến mất trước khi tới Silver |
| **Cách sửa** | `pipeline/silver.py`: `MERGE INTO silver_tickets ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE` toàn bộ cột, `WHEN NOT MATCHED THEN INSERT` | `pipeline/config.py`: `LOOKBACK_DAYS = 3` = ceil(P99 = 3.00) đo bằng `main.py --lateness`; mỗi run overwrite-partition `[day-3, day]` | `pipeline/staging.py`: `ticket_id = coalesce(after.ticket_id, before.ticket_id, key.ticket_id)`. Các cột khác vẫn lấy từ `after` (NULL), nên MERGE ghi đè hàng thành tombstone |
| **Khái niệm trên slide** | Silver có khoá, MERGE theo khoá, "bản mới hơn thắng" bằng LSN guard, idempotent | Data về muộn: event time, lookback = ceil(P99) đo từ Bronze | CDC log-based (`before`/`after`/`op`), tombstone, "xoá phải lan" xuống Gold |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (p50 = 0, p95 = 2.90, max = 3, 43 record) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS**. Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY** (`silver_tickets` 3c15dfd43701, `gold_feature_daily` 8630e04a61d1)

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- **MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`:** ticket là thực thể thay đổi trạng thái nên cần upsert theo khoá kèm LSN guard để batch cũ không đè batch mới. Feature theo ngày là aggregate, tính lại cả partition `[day-3, day]` từ Silver vừa đơn giản vừa tự sửa được dữ liệu đến muộn.
- **Tombstone thay vì xoá hẳn hàng trong Silver:** tombstone là một trạng thái có LSN, nên khi chạy lại batch cũ chứa `op='c'` của T-97, MERGE thấy LSN cũ hơn và không "hồi sinh" ticket. Xoá hẳn sẽ để batch cũ chèn lại hàng. Đổi lại, hàng tombstone (chỉ còn khoá và LSN, không còn PII) tồn tại mãi.
- **Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ:** mô hình đã train trên `v2026-08-14` phải tái lập được. Feedback đến muộn tạo `v2026-08-15` mới thay vì sửa lịch sử (point-in-time, chống rò rỉ tương lai).
- **DuckDB (lite) / dbt (track dbt) thay vì Spark:** dữ liệu cỡ KB–GB chạy trên một máy trong vài giây, không cần cluster. dbt cho `merge`/`microbatch`, contract và unit test mà vẫn chạy trên DuckDB. Spark chỉ đáng dùng khi dữ liệu vượt RAM một máy.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot bất biến và quyền được xoá:** bất biến là cam kết kỹ thuật, còn quyền xoá là nghĩa vụ pháp lý, nên quyền xoá thắng. Cách xử lý: (a) đánh dấu các snapshot `v08-12..v08-14` là *revoked* và tạo version mới không có T-97 bằng cách dựng lại từ Bronze có áp dụng danh sách xoá; mô hình train trên snapshot cũ được lên lịch train lại. (b) Lâu dài thì dùng crypto-shredding: văn bản trong Bronze/snapshot được mã hoá bằng khoá của từng khách, huỷ khoá là xoá được mà không phải ghi lại file bất biến. Kèm theo retention (ví dụ 90 ngày) cho snapshot cũ.
2. **Chốt PII cho tên người:** đặt ở cửa Bronze→Silver (cùng chỗ `mask_pii`), vì đây là điểm duy nhất mọi dữ liệu đi qua trước khi tới Gold, RAG hay LLM. Dùng regex cộng mô hình NER tiếng Việt (PER/LOC) và thay bằng `<NAME>`. Đo bằng recall và precision trên một tập vài trăm câu gán nhãn tay (có/không dấu), mục tiêu recall ≥ 0.95. Thêm một contract kiểu `verify` quét Gold bằng NER và cảnh báo khi số lần phát hiện > 0.

## Phụ lục (ngoài giới hạn 1 trang) — Extensions: câu hỏi suy ngẫm

Output: flywheel đưa 21 span vào Bronze, sinh 2 dòng eval, 3 cặp DPO thô còn 1 cặp sạch (2 cặp bị decontamination loại), ASOF join phát hiện 2 hàng bị rò rỉ tương lai. KG: `widget → accessory → hanoi fulfillment center` (2 hop), trong khi không có chunk nào chứa cả "widget" lẫn "hanoi".

1. **Bước hỏng âm thầm nhất:** bước làm phẳng trace (`flatten`) và join `trace_id` / `split`. `traces.py` đọc bằng `attrs.get("input")`, `attrs.get("split")`. Nếu exporter OTel đổi tên attribute (ví dụ `input` thành `gen_ai.input.messages`), `.get()` trả `None` chứ không báo lỗi, nên `user_input` thành NULL và các cặp DPO giảm dần mà không có lỗi nào được báo. Cách phát hiện: contract số dòng (tỷ lệ span có `user_input` không NULL theo ngày), cảnh báo khi số cặp/eval giảm bất thường so với trung vị 7 ngày, và kiểm tra schema của attribute ở cửa Bronze.
2. **Bỏ decontamination thì metric "nói dối":** 2/3 cặp DPO có prompt nằm trong eval set, nên model được train trên chính câu sẽ bị chấm. Điểm eval tăng vì model thuộc lòng câu trả lời, không phải vì nó tổng quát tốt hơn. Khoảng cách giữa offline và production bị che đi cho tới khi người dùng hỏi bằng cách diễn đạt khác.
3. **Graph và chunk retrieval:** câu "Widget giao từ kho nào?" cần nối hai sự kiện nằm ở hai chunk khác nhau (widget IS_A accessory, accessory SHIPS_FROM Hà Nội); graph trả lời bằng 2 hop, còn chunk retrieval không có chunk nào chứa đủ. Ngược lại, "Widget được trả trong bao lâu?" là lookup một hop có sẵn trong một câu, nên vector retrieval là đủ và graph là thừa.

## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell theo lệnh tương đương trong [SUBMISSION.md](../docs/SUBMISSION.md).

```text
PS> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

PS> .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 2.43s

PS> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

PS> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

PS> .\.venv\Scripts\python.exe main.py --land-only; cd dbt_project; ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
03:14:02  Running with dbt=1.12.5
03:14:03  Registered adapter: duckdb=1.11.0
03:14:03  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
03:14:03  
03:14:03  Concurrency: 1 threads (target='dev')
03:14:03  
03:14:03  1 of 19 START sql view model main.stg_events ................................... [RUN]
03:14:03  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.07s]
03:14:03  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
03:14:03  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.02s]
03:14:03  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
03:14:04  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.11s]
03:14:04  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
03:14:04  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.10s]
03:14:04  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
03:14:04  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.16s]
03:14:04  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
03:14:04  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
03:14:04  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
03:14:04  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
03:14:04  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
03:14:04  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
03:14:04  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
03:14:04  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
03:14:04  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
03:14:04  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
03:14:04  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
03:14:04  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
03:14:04  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
03:14:04  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
03:14:04  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
03:14:04  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
03:14:04  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
03:14:04  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
03:14:04  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
03:14:04  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
03:14:04  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
03:14:04  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
03:14:04  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.04s]
03:14:04  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
03:14:04  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.02s]
03:14:04  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
03:14:04  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.02s]
03:14:04  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
03:14:04  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
03:14:04  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
03:14:04  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.02s]
03:14:04  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
03:14:04  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.02s]
03:14:04  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
03:14:04  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.02s]
03:14:04  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.20s]
03:14:04  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
03:14:04  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
03:14:04  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
03:14:04  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
03:14:04  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
03:14:04  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
03:14:04  
03:14:04  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.09 seconds (1.09s).
03:14:04  
03:14:04  Completed successfully
03:14:04  
03:14:04  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

PS> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree

PS> .\.venv\Scripts\python.exe -m scripts.bonus_llm   # Bonus B1
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS

```

**Bonus B1:** thay đổi trong `pipeline/llm_label.py` (cache theo hash(prompt) + model + prompt version, ước tính chi phí trước khi gọi, quarantine câu trả lời sai schema), output `BONUS PASS` ở trên.
**Bonus B2:** [`bonus/DESIGN.md`](../bonus/DESIGN.md), phiên brainstorm về flywheel dữ liệu cho chatbot CSKH tiếng Việt.
