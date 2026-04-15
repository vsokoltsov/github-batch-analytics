-- Warehouse optimization template:
-- final mart tables are partitioned by dt and hr and bucketed on the entity key.
-- repositories are bucketed by repo_id and organizations are bucketed by org_id.
CREATE TABLE "{database_name}"."{temp_table_name}"
WITH (
    format = 'PARQUET',
    external_location = '{table_root_location}',
    parquet_compression = 'SNAPPY',
    partitioned_by = ARRAY['dt', 'hr'],
    bucketed_by = ARRAY['{bucket_column}'],
    bucket_count = {bucket_count}
)
AS
SELECT
        {projection},
        DATE '{dt}' AS dt,
        {hr} AS hr
FROM "{database_name}"."{source_stage_table_name}"
WHERE dt = DATE '{dt}'
  AND hr = {hr}
