# NYC Taxi Incremental ETL — BDM Project 1

**University of Tartu · Big Data Management · 2026**  
**Group member:** Joseph Jacob Kulathinal

---

## How to Run

```bash
# Start the environment (first time)
docker compose up --build

# Subsequent runs
docker compose up

# Open Jupyter at http://localhost:8888  (token: bdm2026)
# Open the notebook: etl_pipeline.ipynb
# Run all cells top to bottom (Kernel > Restart & Run All)

# Spark Web UI at http://localhost:4040
```

**Second run (incremental test):** drop a new `.parquet` file into `data/inbox/` and run all cells again. Only the new file will be processed; the existing output will be extended without duplicates.

---

## Architecture

```
data/inbox/
  yellow_tripdata_2025-01.parquet   ─┐
  yellow_tripdata_2025-02.parquet   ─┤─► ETL ─► data/outbox/trips_enriched.parquet
  taxi_zone_lookup.parquet          ─┘
state/manifest.json  (tracks processed files)
```

The manifest is read at startup. Any file whose name already appears in it is skipped. After a successful write the manifest is updated — making reruns idempotent. The output merge + dedup step guarantees no duplicates even if state is manually reset.

---

## 1. Correctness

### Row Counts

| File | Raw rows | After cleaning | After dedup | Final output |
|------|----------|----------------|-------------|--------------|
| yellow_tripdata_2025-01.parquet | 3,475,226 | 3,253,310 (−221,916) | 3,253,111 (−199) | — |
| yellow_tripdata_2025-02.parquet | 3,577,543 | 3,307,153 (−270,390) | 3,306,914 (−239) | — |
| **Combined output** | — | — | — | **6,560,025** |

### Type Casting

Before cleaning, all fields are cast to their correct types:

| Field | Cast to |
|-------|---------|
| `tpep_pickup_datetime`, `tpep_dropoff_datetime` | timestamp |
| `passenger_count`, `PULocationID`, `DOLocationID` | integer |
| `trip_distance`, `fare_amount` | double |

### Cleaning Rules

All rules are applied in a single filter pass per file.

| # | Field | Rule | Action |
|---|-------|------|--------|
| 1 | `trip_distance` | null or ≤ 0 | Drop row |
| 2 | `fare_amount` | null or < 0 | Drop row |
| 3 | `PULocationID` / `DOLocationID` | null | Drop row |
| 4 | `tpep_pickup_datetime` / `tpep_dropoff_datetime` | null | Drop row |
| 5 | `tpep_dropoff_datetime` | ≤ `tpep_pickup_datetime` | Drop row |
| 6 | `passenger_count` | null or 0 | **Impute** with per-month median (Custom Scenario) |

### Bad Row Examples

**Rule 1 — zero trip distance (row dropped):**
```
tpep_pickup_datetime  tpep_dropoff_datetime  trip_distance  fare_amount  passenger_count
2025-01-01 00:49:48   2025-01-01 00:49:48    0.0            20.06        1
2025-01-01 00:37:43   2025-01-01 00:37:53    0.0            12.0         1
2025-01-01 00:57:08   2025-01-01 00:57:16    0.0            30.0         3
```
Trip distance is 0.0 — these are likely meter errors or cancelled trips. Fare amount is non-zero in all cases, but without a valid distance the record is unusable for analysis.

**Rule 2 — negative fare amount (row dropped):**
```
tpep_pickup_datetime  tpep_dropoff_datetime  trip_distance  fare_amount  passenger_count
2025-01-01 00:01:41   2025-01-01 00:07:14    0.71           -7.2         1
2025-01-01 00:55:54   2025-01-01 01:00:38    0.69           -6.5         1
2025-01-01 00:56:12   2025-01-01 01:15:00    0.97           -16.3        1
```
Negative fare amounts are data entry errors or system reversals. A fare cannot be negative in a valid trip record.

**Rule 5 — dropoff timestamp ≤ pickup timestamp (row dropped):**
```
tpep_pickup_datetime  tpep_dropoff_datetime  trip_distance  fare_amount  passenger_count
2025-01-01 00:49:48   2025-01-01 00:49:48    0.0            20.06        1
2025-01-01 01:42:36   2025-01-01 01:42:36    0.0            3.0          1
2025-01-01 02:13:25   2025-01-01 02:13:25    0.0            114.0        1
```
Pickup and dropoff timestamps are identical — zero-duration trips are physically impossible and indicate a meter or recording fault. Note the first row also violates Rule 1 (distance = 0); rows are dropped if they fail any rule.

**Custom Scenario — passenger_count null or 0 (imputed, not dropped):**
```
tpep_pickup_datetime  tpep_dropoff_datetime  trip_distance  fare_amount  passenger_count
2025-01-01 00:14:47   2025-01-01 00:16:15    0.4            4.4          0
2025-01-01 00:39:27   2025-01-01 00:51:51    1.6            12.1         0
2025-01-01 00:53:43   2025-01-01 01:13:23    2.8            19.1         0
```
These rows have `passenger_count = 0`, which is invalid but the trip itself is otherwise valid (positive distance, positive fare, valid timestamps). Per the custom scenario these are imputed with the monthly median (1) rather than dropped.

---

## 2. Custom Scenario — Passenger Count Imputation

**Rule:** if `passenger_count` is null or 0, replace it with the **median** passenger count for the same calendar month, computed only from valid rows (`passenger_count > 0`, not null) **in the same file**. If a month has no valid rows at all, the file-level global median is used. If the entire file has no valid rows, the value defaults to 1.

`percentile_approx(passenger_count, 0.5)` is used (Spark's approximate median over valid rows).

### Imputation Results

| File | Month | Median used | Rows imputed |
|------|-------|-------------|--------------|
| yellow_tripdata_2025-01.parquet | Jan 2025 | 1 | 436,945 |
| yellow_tripdata_2025-02.parquet | Feb 2025 | 1 | 649,507 |

The global median of 1 reflects that the majority of valid NYC taxi trips carry a single passenger. After imputation the output contains **zero** null or zero `passenger_count` values.

---

## 3. Performance

### Runtime

| Run | Files processed | Wall time |
|-----|----------------|-----------|
| Baseline (unoptimised) | 2 | 51.2s |
| After Optimisation 1 | 2 | 48.1s (−3.1s, −6%) |
| After Optimisation 2 | 2 | 46.2s (−1.9s, −4%) |
| **Total improvement** | | **−5.0s (−10%)** |

### Spark Web UI Screenshots

**Screenshot 1 — Stages overview (final optimised run)**

![Stages overview](docs/screenshots/Optimisation%202%20run/1.png)

Shows completed stages with durations. Top stages by shuffle read visible.

**Screenshot 2 — Dedup stage detail (shuffle metrics)**

![Dedup stage shuffle metrics](docs/screenshots/Optimisation%202%20run/3.png)

`dropDuplicates` stage on the merged 6,560,025-row dataset: shuffle read and write metrics confirm the dedup cross-partition shuffle. Total task time and task count visible.

---

### Optimisation Choices

#### Optimisation 1 — Persist `combined` before `count()` + `write()` (+6%)

**Root cause:** Spark's lazy evaluation means every action triggers a full recomputation of the DataFrame's entire lineage. In the original code, `combined.count()` (used for the summary print) and `combined.write()` each independently re-executed the full pipeline — reading the existing output, unioning with new data, running `dropDuplicates` over 6.5 M rows — **twice**.

**Fix:** Call `combined.persist()` before `count()`. Spark materialises the DataFrame once into memory/disk, and both the count and the write read from the cached copy.

**Evidence:**

| Metric | Before | After |
|--------|--------|-------|
| Wall time | 51.2s | 48.1s |
| Full pipeline executions for count+write | 2 | 1 |

The improvement is in wall time. Shuffle metrics are unchanged (persist does not reduce shuffle — it eliminates recomputation after the shuffle has already occurred).

---

#### Optimisation 2 — Remove redundant `new_data.count()` diagnostic action (+4%)

**Root cause:** Same principle as Optimisation 1. Cell 18 contained:

```python
print(f"New enriched rows ready: {new_data.count():,}")
```

This triggered a complete Spark job — reading all per-file DataFrames, executing both zone-enrichment joins, processing 6.5 M rows — purely to print a diagnostic number. `new_data` was then recomputed again inside `combined` in Cell 20. The diagnostic count was a second, entirely redundant execution of the enrichment pipeline.

**Fix:** Remove the `new_data.count()` call. The row count is already available from the per-file manifest entries (sum of `row_count_after_dedup`), so no information is lost.

**Evidence:**

| Metric | Before | After |
|--------|--------|-------|
| Wall time | 48.1s | 46.2s |
| Spark jobs triggered for enrichment | 2 | 1 |
| Zone-join stages in Spark UI | Present twice | Present once |

**Combined effect of both optimisations:** 51.2s → 46.2s (−10%), eliminating two full redundant pipeline executions over 6.5 M rows.

> **Note on broadcast hint + partition tuning:** These were also trialled (adding `F.broadcast()` to the zone lookup joins and setting `spark.sql.shuffle.partitions=16`). Neither produced a measurable improvement because Spark 3.5's AQE had already applied both automatically — the zone lookup (265 rows, ~7 KB) is auto-broadcast under the default 10 MB threshold, and AQE coalesces shuffle partitions at runtime regardless.

---

## Deduplication Key

```
(tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, fare_amount)
```

`fare_amount` is rounded to 2 decimal places before comparison to guard against floating-point representation differences between files. The key is applied both within each file during processing and again across the merged output on every run.

---

## Output Schema

| Column | Type | Source |
|--------|------|--------|
| `tpep_pickup_datetime` | timestamp | raw |
| `tpep_dropoff_datetime` | timestamp | raw |
| `PULocationID` | integer | raw |
| `DOLocationID` | integer | raw |
| `pickup_zone` | string | zone lookup |
| `pickup_borough` | string | zone lookup |
| `dropoff_zone` | string | zone lookup |
| `dropoff_borough` | string | zone lookup |
| `passenger_count` | integer | raw + imputed |
| `trip_distance` | double | raw |
| `fare_amount` | double | raw |
| `trip_duration_minutes` | double | derived |
| `pickup_date` | date | derived |
| `source_file` | string | metadata |
| `ingested_at` | string (ISO-8601) | metadata |

---

## Manifest

`state/manifest.json` is written **after** the output parquet is confirmed written. If the job crashes mid-write, the files remain unregistered and will be reprocessed on the next run — preventing partial writes from corrupting the output. Each entry records:

```json
{
  "filename": "yellow_tripdata_2025-01.parquet",
  "file_size_bytes": 59158238,
  "row_count_raw": 3475226,
  "row_count_after_clean": 3253310,
  "row_count_after_dedup": 3253111,
  "imputed_passenger_count": 436945,
  "global_median_used": 1,
  "processed_at": "2026-03-07T11:41:34+00:00"
}
```
