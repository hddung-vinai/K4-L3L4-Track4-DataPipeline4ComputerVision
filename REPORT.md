# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Hoàng Đức Dũng |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `202602798` |
| GitHub | `hddung-vinai` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-03365` (= `list-trips[202602798 % 26]`) |

## Kết quả cuối

- `dataset_version` của bạn: `9d1af6bea320`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 710 / 192 / 117 (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt A ↔ B ở bronze (A 3021 dòng, B 3234 dòng):
1. **Thời gian**: A là chuỗi ISO UTC `ts_utc` (`2025-10-07T08:00:00.014Z`); B là số epoch ms `timestamp_ms` (`1759933801509`).
2. **Đơn vị tốc độ**: A `speed_mps` (m/s); B `spd_kmh` (km/h).
3. **Bàn đạp**: A `gas`/`brake` theo tỷ lệ 0–1; B `throttle_pct`/`brake_pct` theo %.
4. **Tên cột**: `heading_deg/lat/lon/alt_m/acc_x/acc_y/steer` ở A so với `hdg/latitude/longitude/altitude/ax/ay/steering` ở B.
5. **Đồng hồ**: đồng hồ thiết bị B lệch 1500 ms so với video (`telemetry_clock_offset_ms` = 1500 trong `bronze_video_files`), A lệch 0.

Bronze là bản ghi gốc, chỉ thêm: nếu ép kiểu ngay thì một ô trống hay giá trị hỏng sẽ làm lỗi lúc nạp hoặc bị biến thành NULL, và ta mất bằng chứng dữ liệu gốc trông thế nào. Giữ dạng chữ thì mọi quy tắc làm sạch nằm ở silver và có thể sửa, chạy lại từ bronze bất cứ lúc nào. Mỗi nguồn một bảng vì hai schema khác nhau; gộp sớm buộc phải khớp cột/đơn vị ngay ở tầng thô, và một nguồn đổi schema (drift) sẽ làm hỏng cả bảng chung. Nạp lại lần hai báo `already present` (nhận diện bằng sha256), và `history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, 3021 dòng).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Hai model staging đưa cả hai nguồn về cùng tên cột và đơn vị: B đổi `spd_kmh / 3.6` → m/s, bàn đạp % → tỷ lệ, `timestamp_ms` epoch → timestamp UTC, rồi **trừ 1500 ms** độ lệch đồng hồ khai báo trong `sources.json`. Bằng chứng: sau chuẩn hoá, `bat_dau` của B là `2025-10-08 14:30:00.009`, khớp thời điểm bắt đầu video `14:30:00` (lệch 9 ms; nếu chưa trừ sẽ lệch 1,5 s). Tốc độ tối đa sau quy đổi hợp lý: A 71 km/h, B 100 km/h.

| drive | duplicate | speed_missing | speed_out_of_range | sạch | sạch + cách ly |
|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 | 2968 + 53 = 3021 |
| drive_b | 20 | 30 | 3 | 3181 | 3181 + 53 = 3234 |

Dòng lỗi vào `silver_telemetry_rejects` (106 dòng) kèm lý do thay vì bị xoá, vì: (1) có thể **đối soát** — sạch + cách ly = số dòng nhận về, không dòng nào mất im lặng; (2) đo được chất lượng nguồn theo thời gian (ví dụ cảm biến nào hay mất tốc độ); (3) nếu luật làm sạch sai, có thể sửa luật và lấy lại dòng từ quarantine.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng trống > 1000 ms giữa hai mẫu) là `warn` vì mất tín hiệu cảm biến là chuyện **có thật ngoài đời**, không làm sai dữ liệu đang có; ta muốn biết để theo dõi, nhưng dừng pipeline thì cả dataset bị chặn vì một hiện tượng bình thường. Ở bước 2 test này cho WARN=1, các test còn lại PASS=6. `in_range` (tốc độ 0..70 m/s) là `error` vì giá trị ngoài khoảng là dữ liệu **sai**, nếu lọt xuống gold sẽ thành nhãn/đặc trưng hỏng cho mô hình.

Thí nghiệm `break-gate`: nới luật làm sạch từ 70 lên 1000 m/s → `dbt test` cho PASS=5, WARN=1, **ERROR=1**: test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng** (đúng 3 dòng tốc độ vô lý × 2 drive). Khôi phục luật → PASS=6 WARN=1 ERROR=0. Ý nghĩa: luật làm sạch và test là **hai lớp độc lập**; khi ai đó vô tình nới luật ở bước biến đổi, cổng chất lượng vẫn chặn lại và các task phía sau trong DAG không chạy, nên dữ liệu xấu không đi tiếp xuống gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Chạy lại partition `trip-00003` hai lần: trước **1246 dòng**, dấu vân tay `6c7480dfde7e`; sau 2 lần chạy lại vẫn **1246 dòng**, vân tay `6c7480dfde7e` → không đổi. Ghi kiểu `append` sai, chạy 2 lần cho một trip có 60 khung → **120 dòng** (nhân đôi 2×).

Delta `overwrite` với predicate `trip_id = ...` là **một commit nguyên tử**: file mới được ghi xong rồi mới có một dòng log thay thế file cũ của đúng partition đó. Nếu sập giữa chừng, commit không xảy ra và người đọc vẫn thấy bản cũ đầy đủ. Ngược lại "xoá rồi ghi" là hai thao tác: sập sau khi xoá mà trước khi ghi thì trip đó **mất dữ liệu**, và người đọc giữa hai bước thấy bảng thiếu. Overwrite theo partition còn chỉ chạm một trip, nên chạy lại (backfill) một trip hỏng không ảnh hưởng 25 trip kia, và vẫn time travel về bản trước được.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung **đen** là khung hỏng (camera mất hình): gần như không có thông tin, pipeline **loại** (A 6 khung, B 4 khung). Khung **tối** là cảnh thật nhưng thiếu sáng (đường hầm, bóng râm): pipeline **gắn cờ nhưng giữ lại** (B có 61 khung tối được giữ, A có 4). Tôi đồng ý: mô hình xe tự lái phải học được cả cảnh tối, bỏ đi sẽ làm dataset lệch về điều kiện đẹp; cờ vẫn cho phép lọc hoặc cân bằng lại sau.

Ngưỡng (đầy đủ: A 61/109/149/269/363/438, B 8/23/53/137/244/348 cho 0/1/2/4/6/8 bit): tôi chọn **2**. Ngưỡng 0 chỉ bắt khung giống y hệt, bỏ sót camera đứng hình có nhiễu nén; ngưỡng 6 đã coi 363/602 khung của A (60%) là trùng, tức loại cả khung khác nhau thật. Ngưỡng phụ thuộc tốc độ: A chạy chậm (test trung bình 19 km/h) nên hai khung cách nhau 0,5 s rất giống nhau và bị coi trùng nhiều (149 ở ngưỡng 2), còn B chạy nhanh (59–77 km/h) cảnh đổi nhiều nên chỉ 53. Với xe nhanh có thể nới ngưỡng, với xe chậm/đứng yên nên giữ chặt hoặc lấy mẫu theo quãng đường thay vì theo thời gian.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Trên 1019 khung đáng dùng thuộc 26 trip: chia **theo khung hình** ngẫu nhiên 70/15/15 làm **26/26 trip** nằm ở nhiều hơn một split, và cả 26 trip có khung ở **cả train lẫn test**. Chia **theo trip** (gold của pipeline): **0** trip bị chia cắt.

Các khung liền nhau trong một trip (cách 0,5 s) gần như giống hệt: cùng đường, cùng thời tiết, cùng ánh sáng, cùng xe phía trước. Khi một nửa nằm trong train và nửa kia trong test, mô hình chỉ cần "nhớ" cảnh là đoán đúng, nên điểm test cao nhưng đo khả năng ghi nhớ chứ không phải khả năng tổng quát hoá sang **cảnh mới**. Khi chạy trên đường thật (cảnh chưa từng thấy), hiệu năng sẽ thấp hơn nhiều so với con số báo cáo.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` = 12 ký tự đầu của sha256 trên danh sách `frame_id|split` đã sắp xếp (`pipeline/publish.py`). Nó là băm nội dung nên **cùng nội dung → cùng version, khác một khung hay một split → khác version**. Ngày giờ hay số đếm thì đổi mỗi lần chạy dù dữ liệu y nguyên (không biết hai bản có giống nhau không), và hai người chạy song song có thể ra cùng số nhưng nội dung khác.

Hai lần chạy liên tiếp: `step all` ban đầu, `step 4` và `cli export-dataset` chạy lại đều cho **`9d1af6bea320`** (1019 khung, 710/192/117), cùng `manifest_sha256` `3920b02c…`.

Khác bạn bè vì `gold_dataset_frames.sql` xếp trip theo `md5(trip_id || LAB_STUDENT_ID)`: MSSV `202602798` là "muối" làm đổi thứ tự trip, nên trip nào vào train/val/test đổi theo. Danh sách `frame_id|split` đổi → băm đổi → version đổi, dù dữ liệu nguồn giống hệt.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/9d1af6bea320/manifest.json`, phần `lineage`:

- Bảng bronze: `raw_telemetry_a` v0, `raw_telemetry_b` v0, `trips_raw` v0, `video_files` v1.
- Bảng silver: `frames` v27, `telemetry` v0, `frames_flagged` v0, `frame_telemetry` v0.
- Video đầu vào: drive_a sha256 `636ba1a98dfc5ba6…e7e9aa`, drive_b sha256 `bfa62703f645ff77…bbee9fdc`.

Tức là dataset được tạo từ hai file video có đúng sha256 trên, cùng telemetry/trips ở các phiên bản Delta liệt kê. `silver.frames` v27 vì bảng này nhận 26 commit (mỗi trip một partition) cộng các lần chạy lại của thí nghiệm idempotent. Manifest còn ghi `rules` (bỏ `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`; chia "per trip, stratified by drive, md5 order") và `created_at`.

3 tháng sau, để tái tạo đúng: sha256 hai video (đảm bảo đúng file gốc), số phiên bản từng bảng Delta (time travel về đúng commit đó), quy tắc lọc/chia và **MSSV làm muối**, cùng commit git của mã dbt/pipeline. Chạy lại phải ra cùng `dataset_version` `9d1af6bea320`, và `manifest_sha256` dùng để kiểm tra danh sách ảnh.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

<!-- TODO: điền sau khi làm bài tập trong Airflow (GUIDE bước 5). Đếm số task trên Graph. -->
Trip của tôi: `trip-03365`. Đặt `LAB_FAIL_ONCE=trip-03365`, trigger `day09_reset` rồi `day09_video_pipeline`: **1** task `silver_frames` (map index ứng với `trip-03365`) chuyển đỏ, log có dòng `simulated crash while extracting trip-03365`; **<điền số>** task phía sau chuyển `upstream_failed`; 25 task `silver_frames` còn lại vẫn xanh. Tôi chỉ **clear task đỏ** đó (không clear cả DAG): nó chạy lại thành công, các task `upstream_failed` tự chạy tiếp, DAG xanh. Dữ liệu không nhân đôi vì trip được ghi bằng Delta overwrite theo partition (giống thí nghiệm idempotent: 1246 dòng trước và sau). `verify` vẫn 27/27 và `dataset_version` vẫn `9d1af6bea320`. Bằng chứng: `report/screenshots/fail_task_log.png` (log task đỏ), `report/screenshots/airflow_graph.png` (Graph xanh sau khi clear).

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

1. **Giải mã video bằng FFmpeg trong một container Airflow** (slide 24, 23): bây giờ 2 video ~5 phút đã cần 26 task; 500 xe × vài giờ video/ngày là hàng chục nghìn giờ video, một worker CPU không kịp. Thay bằng hàng đợi + nhiều worker phân tán (Ray/Spark, Kubernetes executor của Airflow), giải mã bằng GPU (NVDEC), và chỉ trích khung ở cạnh xe/edge rồi gửi lên.
2. **DuckDB một máy chạy dbt** (slide 22): toàn bộ biến đổi và test chạy trong bộ nhớ một tiến trình; telemetry 500 xe × 10 Hz là hàng tỷ dòng/tháng. Thay bằng engine phân tán đọc Delta trực tiếp (Spark/Databricks, Trino) và dbt chạy trên engine đó, test theo partition mới thay vì quét lại toàn bảng.
3. **Điều phối kiểu "một task mỗi trip, chạy lại từ đầu"** (slide 12, 26, 27): mapping 26 task thành hàng chục nghìn task/ngày làm scheduler Airflow quá tải, và `step all --reset` không còn khả thi. Chuyển sang xử lý **incremental** theo ngày/xe (partition theo `date/vehicle_id`), ingest theo sự kiện khi file mới vào object storage, và backfill có chọn lọc.

Ngoài ra MinIO một node và Postgres/Metabase đọc trực tiếp cũng cần thay bằng object storage có nhân bản (S3) và warehouse cột (BigQuery/Snowflake/ClickHouse).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**Trip nào bị loại nhiều khung nhất, và vì sao?**

```sql
select trip_id, drive_id, lighting, conditions, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_loai,
       avg_speed_kmh, split
from marts.mart_trip_summary
order by pct_loai desc
limit 10;
```

Kết quả (5 trip đầu): `trip-04622` loại 56,7% (60 → 26 khung, 67,1 km/h, train), `trip-03904` 50,0% (22 → 11, test), `trip-02086` 46,7% (17,6 km/h), `trip-08888` 40,0% (6,5 km/h), `trip-07774` 38,2% (12,8 km/h). Cả 5 đều thuộc drive_a, trời ban ngày nhiều mây, nên không phải do ánh sáng. Phần lớn là xe **chạy chậm** (6–18 km/h) nên khung liền nhau bị dHash coi là trùng. Riêng `trip-04622` chạy nhanh mà vẫn mất 57%, phù hợp với đoạn camera đứng hình 6 giây và khung mờ được cấy vào drive_a. Hệ quả vận hành: dataset ít khung từ cảnh xe chậm/đô thị, nên cân nhắc lấy mẫu theo quãng đường hoặc ngưỡng trùng theo tốc độ.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Bất ngờ nhất**: chia theo khung hình làm rò rỉ ở **cả 26/26** trip, không phải chỉ vài trip. Và chỉ đổi MSSV (muối) là đổi toàn bộ split và `dataset_version`.
- **Chưa rõ**: ngưỡng `sync_tol_ms` và ngưỡng dHash nên chỉnh theo tốc độ xe tự động thế nào, thay vì một hằng số cho mọi trip.
- **Lỗi pipeline chưa bắt**: telemetry có giá trị **trong khoảng hợp lệ nhưng sai** (ví dụ tốc độ bị kẹt ở một giá trị, GPS nhảy vài trăm mét giữa hai mẫu liên tiếp): `in_range` chỉ kiểm tra từng dòng, không kiểm tra **tính liên tục** (gia tốc suy ra từ tốc độ vượt giới hạn vật lý). Cũng chưa có kiểm tra độ lệch đồng hồ **thật** so với giá trị khai báo trong `sources.json`: nếu khai báo sai, pipeline trừ sai mà không báo.
