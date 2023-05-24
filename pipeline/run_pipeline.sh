#!/bin/bash

# TODO take credentials filepath as cli argument
gcloud auth activate-service-account \
--key-file=.eigen1-vijay-gcp.credentials.json

gcloud dataproc batches submit pyspark bq_to_gcs.py \
--account=eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com \
--project=boxwood-well-386122 \
--region=us-central1 \
--subnet=default-sub \
--batch=bq-to-pqt-job \
--version=2.1 \
--deps-bucket=gs://vijay-lens-dataproc-temp \
--jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar \
-- "--bucket" "vijay-lens-bigquery-export"

gcloud dataproc batches submit pyspark posts_lens_features.py \
--account=eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com \
--project=boxwood-well-386122 \
--region=us-central1 \
--container-image="gcr.io/boxwood-well-386122/lens-recommender:latest" \
--subnet=default-sub \
--batch=gcs-to-fs-job \
--version=2.0 \
--deps-bucket=gs://vijay-lens-dataproc-temp \
-- "--bucket" "vijay-lens-bigquery-export" \
"--staging" "vijay-lens-feature-store-temp" \
"-f" "lens_featurestore_d2"

source /home/ubuntu/venvs/lens-ml-env3/bin/activate
python -m pip install --no-cache-dir --upgrade -r requirements.txt
export GOOGLE_APPLICATION_CREDENTIALS=.eigen1-vijay-gcp.credentials.json
python profiles_eigentrust_features.py
deactivate

gcloud dataproc batches submit pyspark predict_posts.py \
--account=eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com \
--project=boxwood-well-386122 \
--region=us-central1 \
--region=us-central1 \
--container-image="gcr.io/boxwood-well-386122/lens-recommender:latest" \
--subnet=default-sub \
--batch=predict-job \
--version=2.0 \
--deps-bucket=gs://vijay-lens-dataproc-temp \
-- "--staging" "vijay-lens-feature-store-temp" \
"--source" "vijay-lens-bigquery-export" \
"--mlbucket" "vijay-lens-ml" \
"-f" "lens_featurestore_d2" \
"--modelversion" "20230522053757"