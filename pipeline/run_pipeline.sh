#!/bin/bash

printf "\n\n******Starting Pipeline*"
date

# TODO take credentials filepath as cli argument
PREV_GCP_ACCOUNT=$(gcloud config list account --format "value(core.account)")
gcloud auth activate-service-account \
--key-file=.eigen1-vijay-gcp.credentials.json

echo "*** BigQuery to GCS ***"
gcloud dataproc batches submit pyspark bq_to_gcs.py \
--account=eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com \
--project=boxwood-well-386122 \
--region=us-central1 \
--subnet=default-sub \
--version=2.1 \
--deps-bucket=gs://vijay-lens-dataproc-temp \
--jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar \
-- "--bucket" "vijay-lens-bigquery-export"

# sleep otherwise Dataproc will throw a "Insufficient CPU Quota" Error
echo "*** Sleeping zzzzzzzzzzzzzzzzzzzz ***"
sleep 2m

echo "*** GCS to Featurestore ***"
gcloud dataproc batches submit pyspark posts_lens_features.py \
--account=eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com \
--project=boxwood-well-386122 \
--region=us-central1 \
--container-image="gcr.io/boxwood-well-386122/lens-recommender:latest" \
--subnet=default-sub \
--version=2.0 \
--deps-bucket=gs://vijay-lens-dataproc-temp \
-- "--bucket" "vijay-lens-bigquery-export" \
"--staging" "vijay-lens-feature-store-temp" \
"-f" "lens_featurestore_t1"

echo "*** GlobalTrust to Featurestore ***"
source /home/vijay_karma3labs_com/venvs/lens-ml-venv/bin/activate
# Uncomment next line only for the first run
# python -m pip install --no-cache-dir --upgrade -r requirements.txt
export GOOGLE_APPLICATION_CREDENTIALS=.eigen1-vijay-gcp.credentials.json
python profiles_eigentrust_features.py -f lens_featurestore_t1
deactivate

echo "*** Run predictions ***"
gcloud dataproc batches submit pyspark predict_posts.py \
--account=eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com \
--project=boxwood-well-386122 \
--region=us-central1 \
--region=us-central1 \
--container-image="gcr.io/boxwood-well-386122/lens-recommender:latest" \
--subnet=default-sub \
--version=2.0 \
--deps-bucket=gs://vijay-lens-dataproc-temp \
-- "--staging" "vijay-lens-feature-store-temp" \
"--source" "vijay-lens-bigquery-export" \
"--mlbucket" "vijay-lens-ml" \
"-f" "lens_featurestore_t1" \
"--modelversion" "20230522053757"

# sleep otherwise Dataproc will throw a "Insufficient CPU Quota" Error
echo "*** Sleeping zzzzzzzzzzzzzzzzzzzz ***"
sleep 2m

echo "*** Insert recommendations into Feed ***"
source .env
gcloud dataproc batches submit pyspark generate_feed.py \
--account=eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com \
--project=boxwood-well-386122 \
--region=us-central1 \
--region=us-central1 \
--container-image="gcr.io/boxwood-well-386122/lens-recommender:latest" \
--subnet=default-sub \
--version=2.0 \
--deps-bucket=gs://vijay-lens-dataproc-temp \
-- "--gcspath" "gs://vijay-lens-ml/predictions/20230522053757_xgbcl/" \
"--pgsql-url" $PGSQL_URL \

gcloud config set account $PREV_GCP_ACCOUNT

printf "*Pipeline finished********\n\n"
date