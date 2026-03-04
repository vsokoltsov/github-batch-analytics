# Github Batch Analytics

## Diagram

```
┌───────────────────────────────────────────────────────────────┐
│                 GH Archive (hourly JSON .gz)                   │
│  https://data.gharchive.org/YYYY-MM-DD-H.json.gz               │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (1) Airflow: download_hourly_files
                           ▼
┌───────────────────────────────────────────────────────────────┐
│            S3 Landing / Raw Zone (immutability)                │
│  s3://<bucket>/raw/gharchive/dt=YYYY-MM-DD/hr=HH/*.json.gz     │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (2) Spark (or Glue Spark) : parse + flatten
                           ▼
┌───────────────────────────────────────────────────────────────┐
│              Bronze: flattened events (Parquet)                │
│  s3://<bucket>/bronze/gh_events_flat/dt=YYYY-MM-DD/hr=HH/      │
│  - base validation, types, ingestion_ts, source_file           │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (3) Spark: build candidates (distinct repos)
                           ▼
┌───────────────────────────────────────────────────────────────┐
│             Silver: repo_candidates (Parquet)                  │
│  s3://<bucket>/silver/repo_candidates/dt=YYYY-MM-DD/           │
│  - distinct repo_full_name + repo_id + activity metrics        │
│  - filter: top N + new + missing/expired metadata            │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (4) dlt: GitHub API enrichment
                           ▼
┌───────────────────────────────────────────────────────────────┐
│     S3 Landing / Raw Zone (GitHub API responses)               │
│  s3://<bucket>/raw/github_api/repos/dt=YYYY-MM-DD/...json       │
│  s3://<bucket>/raw/github_api/orgs/dt=YYYY-MM-DD/...json        │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (5) Spark: extract stable snapshot schema
                           ▼
┌───────────────────────────────────────────────────────────────┐
│      Bronze: repo/org snapshots (Parquet, stable schema)       │
│  s3://<bucket>/bronze/github_repo_snapshot/dt=YYYY-MM-DD/      │
│  s3://<bucket>/bronze/github_org_snapshot/dt=YYYY-MM-DD/       │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (6) Spark: dedupe + latest snapshot
                           ▼
┌───────────────────────────────────────────────────────────────┐
│     Silver: dims ready (Parquet)                               │
│  s3://<bucket>/silver/dim_repo_current/dt=YYYY-MM-DD/          │
│  s3://<bucket>/silver/dim_org_current/dt=YYYY-MM-DD/           │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (7) Load to DWH (Redshift or Athena/Iceberg)
                           ▼
┌───────────────────────────────────────────────────────────────┐
│                     Data Warehouse / Lakehouse                 │
│  fact_events (from gh_events_flat)                             │
│  dim_repo_current + dim_org_current                            │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (8) Transformations layer
                           ▼
┌───────────────────────────────────────────────────────────────┐
│                 Analytics marts (for Streamlit)                │
│  mart_daily_repo_activity                                      │
│  mart_language_activity                                        │
│  mart_top_repos                                                │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (9) Streamlit dashboard
                           ▼
┌───────────────────────────────────────────────────────────────┐
│                         Streamlit                              │
│  read marts → plots/tables/filters                             │
└───────────────────────────────────────────────────────────────┘
```