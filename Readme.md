# About this repo
This repo will host the codebase for all the data processing and machine learning logic for recommending posts and profiles in the Lens ecosystem. 

## Useful Commands
Verify that your local environment is setup properly to interact with GCS
```
gcloud config list
gcloud auth list

# authenticate CLI so you can interact with GCP from command-line
gcloud auth login

# generate Application Default Credentials so code that is run locally can interact with GCP
gcloud auth application-default login
```

Enabling required Google Cloud services
```
gcloud services enable aiplatform.googleapis.com
bigquery.googleapis.com
dataproc.googleapis.com
```

Running pyspark on Dataproc Serverless
```
gcloud resource-manager org-policies disable-enforce compute.requireOsLogin --project boxwood-well-386122
gcloud resource-manager org-policies disable-enforce compute.vmExternalIpAccess --project boxwood-well-386122

gcloud config set dataproc/region us-central1

gcloud compute networks subnets update default \
  --region=${REGION} \
  --enable-private-ip-google-access

gcloud compute firewall-rules create allow-internal-ingress \
  --network=default \
  --source-ranges="10.0.0.1/24" \
  --direction="ingress" \
  --action="allow" \
  --rules="all"

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:1181216607-compute@developer.gserviceaccount.com --role=roles/dataproc.worker

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:1181216607-compute@developer.gserviceaccount.com --role=roles/bigquery.user

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:1181216607-compute@developer.gserviceaccount.com --role=roles/bigquery.dataEditor

bq --location=US mk -d \
  --default_table_expiration 3600 \
  --description "Temp dataset for spark materializationDataset" \
  spark_materialization
```

```
gcloud dataproc batches submit pyspark bq_to_gcs.py --batch=bq-to-pqt-job --region=us-central1 --subnet=default-sub --version=2.1 --deps-bucket=gs://vijay-lens-dataproc-temp --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar -- "-b" "vijay-lens-bigquery-export"
```

Extracting data from BigQuery
```
bq extract --destination_format PARQUET --compression SNAPPY lens-public-data:polygon.public_profile gs://vijay-lens-bigquery-export/public_profile_05092023.pqt
```
