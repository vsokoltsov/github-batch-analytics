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

```
┌───────────────────────────────────────────────────────────────┐
│                 GH Archive (hourly JSON .gz)                   │
│  https://data.gharchive.org/YYYY-MM-DD-H.json.gz               │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (1) Airflow task `get_github_events_archive`
                           │     Download hourly archive and upload as-is
                           ▼
┌───────────────────────────────────────────────────────────────┐
│            S3 Landing / Raw Zone (immutability)                │
│  s3://<bucket>/raw/gharchive/dt=YYYY-MM-DD/hr=HH/*.json.gz     │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (2) Spark task `parse_flatten_events`
                           │     Input: raw gzip JSON
                           │     Output: typed flattened parquet rows
                           ▼
┌───────────────────────────────────────────────────────────────┐
│              Bronze: flattened events (Parquet)                │
│  s3://<bucket>/bronze/gh_events_flat/dt=YYYY-MM-DD/hr=HH/      │
│  - base validation, types, ingestion_ts, source_file           │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (3) Spark task `build_repo_aggregates`
                           │     Input: bronze events
                           │     Output: distinct active repositories
                           ▼
┌───────────────────────────────────────────────────────────────┐
│             Silver: repo_candidates (Parquet)                  │
│  s3://<bucket>/silver/repo_candidates/dt=YYYY-MM-DD/           │
│  - distinct repo_full_name + repo_id + activity metrics        │
│  - filter: top N + new + missing/expired metadata            │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (4a) dlt task `enrich_repos_from_candidates`
                           │      Input: silver repo list
                           │      Output: normalized repo snapshot parquet
                           ├───────────────────────────────────────────────┐
                           │                                               │
                           │ (4b) dlt task `enrich_orgs_from_candidates`   │
                           │      Input: same silver repo list             │
                           │      Output: normalized org snapshot parquet  │
                           ▼                                               ▼
┌───────────────────────────────────────────────────────────────┐   ┌───────────────────────────────────────────────────────────────┐
│      Bronze: github_repo_snapshot (Parquet, stable schema)    │   │      Bronze: github_org_snapshot (Parquet, stable schema)     │
│  s3://<bucket>/bronze/github_repo_snapshot/dt=YYYY-MM-DD/      │   │  s3://<bucket>/bronze/github_org_snapshot/dt=YYYY-MM-DD/      │
└───────────────────────────────────────────────────────────────┘   └───────────────────────────────────────────────────────────────┘
                           │                                               │
                           └───────────────────┬───────────────────────────┘
                                               │
                           │ (5) Spark task `build_current_dims`
                           │     Input: both bronze snapshots
                           │     Output: deduped latest dimension tables
                           ▼
┌───────────────────────────────────────────────────────────────┐
│     Silver: dims ready (Parquet)                               │
│  s3://<bucket>/silver/dim_repo_current/dt=YYYY-MM-DD/          │
│  s3://<bucket>/silver/dim_org_current/dt=YYYY-MM-DD/           │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (6) Load to DWH (Redshift or Athena/Iceberg)
                           │     fact_events + dim_repo_current + dim_org_current
                           ▼
┌───────────────────────────────────────────────────────────────┐
│                     Data Warehouse / Lakehouse                 │
│  fact_events (from gh_events_flat)                             │
│  dim_repo_current + dim_org_current                            │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (7) Transformations layer
                           │     dbt/Spark SQL models for analytics marts
                           ▼
┌───────────────────────────────────────────────────────────────┐
│                 Analytics marts (for Streamlit)                │
│  mart_daily_repo_activity                                      │
│  mart_language_activity                                        │
│  mart_top_repos                                                │
└───────────────────────────────────────────────────────────────┘
                           │
                           │ (8) Streamlit dashboard
                           │     read marts and render charts/tables
                           ▼
┌───────────────────────────────────────────────────────────────┐
│                         Streamlit                              │
│  read marts → plots/tables/filters                             │
└───────────────────────────────────────────────────────────────┘
```
