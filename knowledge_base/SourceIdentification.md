Your last question is the most important one. Every row needs to know where it came from. Here's how we'll stamp it:
source_file      → 'sample_data/csv/weather_stations.csv'
source_format    → 'csv'
source_system    → 'local_file'
ingested_at      → 2024-01-15 06:32:11 UTC
pipeline_run_id  → 'run-20240115-001'
That gets added by the loader automatically — you never have to think about it per-parser.
