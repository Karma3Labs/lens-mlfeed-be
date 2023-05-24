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
export GCP_PROJECT=boxwood-well-386122

gcloud resource-manager org-policies disable-enforce compute.requireOsLogin --project $GCP_PROJECT
gcloud resource-manager org-policies disable-enforce compute.vmExternalIpAccess --project $GCP_PROJECT

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

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:1181216607-compute@developer.gserviceaccount.com --role=roles/aiplatform.featurestoreUser

bq --location=US mk -d \
  --default_table_expiration 3600 \
  --description "Temp dataset for spark materializationDataset" \
  spark_materialization
```

```
gcloud dataproc batches submit pyspark bq_to_gcs.py --batch=bq-to-pqt-job --region=us-central1 --subnet=default-sub --version=2.1 --deps-bucket=gs://vijay-lens-dataproc-temp --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar -- "--bucket" "vijay-lens-bigquery-export"

gcloud dataproc batches submit pyspark posts_lens_features.py --container-image="gcr.io/boxwood-well-386122/lens-recommender:latest" --batch=gcs-to-fs-job --region=us-central1 --subnet=default-sub --version=2.0 --deps-bucket=gs://vijay-lens-dataproc-temp -- "--bucket" "vijay-lens-bigquery-export" "--staging" "vijay-lens-feature-store-temp" "-f" "lens_featurestore_d2" "-t" "2023-05-19 05:59:00 UTC"

gcloud dataproc batches submit pyspark predict_posts.py --container-image="gcr.io/boxwood-well-386122/lens-recommender:latest" --batch=predict-job --region=us-central1 --subnet=default-sub --version=2.0 --deps-bucket=gs://vijay-lens-dataproc-temp -- "--staging" "vijay-lens-feature-store-temp" "--source" "vijay-lens-bigquery-export" "--mlbucket" "vijay-lens-ml" "-f" "lens_featurestore_d2" "--modelversion" "20230522053757"
```

Run pipeline on eigen1
```

gcloud iam service-accounts create eigen1-vijay-gcp

gcloud iam service-accounts keys create eigen1-vijay-gcp-creds.json --iam-account=eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com --role=roles/dataproc.worker

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com --role=roles/dataproc.editor

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com --role=roles/bigquery.user

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com --role=roles/bigquery.dataEditor

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com --role=roles/aiplatform.featurestoreUser

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com --role=roles/roles/storage.objectCreator

gcloud iam service-accounts add-iam-policy-binding 1181216607-compute@developer.gserviceaccount.com --member=serviceAccount:eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com --role=roles/iam.serviceAccountUser

gcloud iam service-accounts add-iam-policy-binding 1181216607-compute@developer.gserviceaccount.com --member=serviceAccount:eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com --role=roles/iam.serviceAccountTokenCreator

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:eigen1-vijay-gcp@boxwood-well-386122.iam.gserviceaccount.com --role=roles/iam.serviceAccountTokenCreator
```




```
gcloud dataproc workflow-templates create lens-posts-rec-wflow --region=us-central1

gcloud dataproc workflow-templates set-managed-cluster lens-posts-rec-wflow \
--cluster-name=lens-posts-rec \
--no-address \
--region=us-central1 \
--subnet=default-sub \
--bucket=vijay-lens-dataproc-temp \
--image=my-custom-dataproc-image

gcloud dataproc workflow-templates add-job pyspark bq_to_gcs.py \
--workflow-template=lens-posts-rec-wflow \
--step-id=bq-to-pqt \
-- "--bucket" "vijay-lens-bigquery-export"

gcloud dataproc workflow-templates add-job pyspark posts_lens_features.py \
--workflow-template=lens-posts-rec-wflow \
--step-id=gcs-to-fs \
-- "--bucket" "vijay-lens-bigquery-export" \
"--staging" "vijay-lens-feature-store-temp" \
"-f" "lens_featurestore_d2" 

gcloud dataproc workflow-templates add-job pyspark predict_posts.py \
--workflow-template=lens-posts-rec-wflow \
--step-id=predict \
-- "--staging" "vijay-lens-feature-store-temp" \
"--source" "vijay-lens-bigquery-export" \
"--mlbucket" "vijay-lens-ml" \
"-f" "lens_featurestore_d2" \
"--modelversion" "20230522053757"

gcloud dataproc workflow-templates instantiate lens-posts-rec-wflow --region=us-central1
```


Extracting data from BigQuery
```
bq extract --destination_format PARQUET --compression SNAPPY lens-public-data:polygon.public_profile gs://vijay-lens-bigquery-export/public_profile_05092023.pqt
```


Building custom image for use in Dataproc
```
gcloud config set project boxwood-well-386122
mkdir pipeline
cd pipeline
mkdir lib
mkdir bin

wget -P lib https://jdbc.postgresql.org/download/postgresql-42.5.4.jar
gsutil cp gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.30.0.jar lib/
wget -P lib/ https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.13/gcs-connector-hadoop2-2.2.13-shaded.jar
wget -P bin/ https://repo.anaconda.com/miniconda/Miniconda3-py39_23.3.1-0-Linux-x86_64.sh

gsutil cp gs://vijay-lens-python-packages/Dockerfile .
gsutil cp gs://vijay-lens-python-packages/requirements.txt .

export IMAGE=gcr.io/boxwood-well-386122/lens-recommender
docker build -t "${IMAGE}" .
docker push "${IMAGE}"
```
