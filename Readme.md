# About this repo
This repo will host the codebase for all the data processing and machine learning logic for recommending posts and profiles in the Lens ecosystem. 

## Useful Commands
Extracting data from BigQuery
```
bq extract --destination_format PARQUET --compression SNAPPY lens-public-data:polygon.public_profile gs://vijay-lens-bigquery-export/public_profile_05092023.pqt
```
