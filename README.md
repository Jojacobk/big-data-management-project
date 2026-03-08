# big-data-management-2026

# Big Data Management – Project 1  
## Taxi Trip Data ETL Pipeline using Apache Spark

**Student:** Buland Kumar Pradhan  
**Course:** Big Data Management 2026  
**Institution:** University of Tartu  

# Overview

This project implements a data engineering pipeline using **Apache Spark** to process New York City taxi trip data. The pipeline ingests raw taxi trip datasets, performs data cleaning and transformations, enriches the data using taxi zone metadata, and stores the processed dataset in **Parquet format** for efficient analytical queries.

The pipeline simulates a **real-world incremental ETL workflow**, where new data files arrive in an inbox directory and are processed only once. A **manifest file** tracks previously processed files to prevent duplicate processing.

The final dataset contains **cleaned and enriched taxi trip records** ready for analysis.


# Project Structure

project1/
│
├── data/
│ ├── inbox/
│ │ ├── yellow_tripdata_2025-01.parquet
│ │ ├── yellow_tripdata_2025-02.parquet
│ │
│ ├── outbox/
│ │ └── trips_enriched.parquet
│ │
│ └── taxi_zone_lookup.parquet
│
├── state/
│ └── manifest.json
│
├── Project1.ipynb
├── .gitignore
└── README.md


### Description

| Folder/File | Description |
|---|---|
| `data/inbox` | Raw taxi trip datasets |
| `data/outbox` | Final processed dataset |
| `taxi_zone_lookup.parquet` | Taxi zone lookup dataset |
| `state/manifest.json` | Tracks processed files |
| `Project1.ipynb` | Main ETL pipeline implementation |
| `.gitignore` | Prevents large datasets from being pushed to Git |

# Dataset

The project uses **NYC Yellow Taxi trip data**, which contains information about taxi rides including pickup and drop-off locations, passenger counts, trip distances, and timestamps.

Each trip record contains attributes such as:

- pickup and drop-off timestamps  
- passenger count  
- pickup and drop-off location IDs  
- trip distance  
- fare information  

A **Taxi Zone Lookup dataset** is used to enrich taxi trips with additional metadata such as:

- borough  
- zone name  
- service area  

# ETL Pipeline Architecture

The pipeline consists of several stages.

# 1. File Discovery

The pipeline scans the **inbox directory** and identifies available `.parquet` files.

Example logic:

```python
list_parquet_files(INBOX_DIR)

# 2. Incremental Processing

To avoid reprocessing previously ingested files, the pipeline maintains a manifest file:

state/manifest.json

The manifest records:

processed file names

processing timestamp

number of rows written

This enables incremental ingestion.

3. Data Loading

New files are loaded into Spark DataFrames using:

spark.read.parquet(file_path)

Apache Spark allows the dataset to be processed in a distributed and scalable manner.

4. Data Transformations

Several transformations are applied to prepare the dataset.

Extract Month Feature

A new column pickup_month is created from the pickup timestamp.

This supports temporal analysis and potential partitioning.

Passenger Count Imputation

If passenger_count is:

NULL

equal to zero

it is replaced with the median passenger count for the same calendar month.

This ensures valid passenger values in the dataset.

5. Data Cleaning

Invalid rows are removed from the dataset.

Examples include:

missing pickup timestamps

missing drop-off timestamps

invalid passenger counts

Row counts are tracked before and after cleaning.

6. Deduplication

Duplicate taxi trips are removed based on key trip attributes.

This prevents duplicate records from affecting downstream analysis.

7. Data Enrichment

Taxi trip records are enriched using the Taxi Zone Lookup dataset.

Example join:

df.join(zone_lookup, "PULocationID", "left")

This adds additional metadata including:

borough

zone name

8. Output Generation

The final dataset is written to:

data/outbox/trips_enriched.parquet

The dataset is stored using Parquet format with Snappy compression.

Advantages of Parquet:

columnar storage

efficient compression

faster analytics queries

9. Manifest Update

After successful processing, the manifest file is updated.

Example entry:

{
  "processed_files": {
    "yellow_tripdata_2025-01.parquet": {
      "processed_at": "timestamp",
      "rows_written": 6951037
    }
  }
}

This ensures the pipeline does not reprocess files in future runs.

Final Output

The final processed dataset contains:

Total rows: 6951037

These rows represent cleaned and enriched taxi trip records ready for analysis.

Performance Evaluation

The performance of the pipeline was evaluated by measuring the runtime required to read the final dataset and compute the row count.

Baseline measurement:

Rows: 6951037
Runtime: 3.7 seconds
Optimisation Experiments

Two optimisation strategies were evaluated.

Optimisation 1 — DataFrame Caching

The dataset was cached in memory:

df.cache()

Caching stores the dataset in memory to avoid recomputation.

However, since the dataset was used only once, Spark first had to load and cache the dataset, resulting in slower performance.

Runtime with caching: 24.09 seconds

This demonstrates that caching is beneficial primarily when a dataset is reused multiple times.

Optimisation 2 — Reduced Shuffle Partitions

Spark uses 200 shuffle partitions by default, which can introduce unnecessary overhead.

The number of shuffle partitions was reduced:

spark.conf.set("spark.sql.shuffle.partitions", "8")

Runtime after optimisation:

Runtime: 1.03 seconds

Reducing shuffle partitions significantly improved execution performance.

Performance Comparision

| Run                        | Files Processed | Runtime       |
| -------------------------- | --------------- | ------------- |
| Baseline                   | 2               | 3.7 seconds   |
| DataFrame Cache            | 2               | 24.09 seconds |
| Reduced Shuffle Partitions | 2               | 1.03 seconds  |

Reducing shuffle partitions improved runtime by approximately 72% compared to the baseline.

Technologies Used

Apache Spark

PySpark

Python

Parquet Storage Format

Jupyter Notebook

Limitations

Some limitations of the current pipeline include:

The pipeline processes a limited number of monthly taxi trip files.

Optimisation experiments were conducted on a moderate dataset size.

Additional Spark optimisations such as broadcast joins could further improve performance.

Future improvements may include:

processing larger datasets

implementing advanced Spark optimisations

automating the ETL pipeline with scheduling tools

Acknowledgement

This project was developed with assistance from ChatGPT for guidance on Spark implementation, debugging, and documentation preparation.
