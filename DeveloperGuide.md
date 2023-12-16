# Running the Pipeline
The pipeine is designed to run on Google Cloud Platform. But, a lot of the code uses vendor-agnostic libraries like Apache PySpark, Pandas, and XGBoost. So, if you ever decide to run this pipeline on AWS, the migration should be straightforward.

The pipeline has a dependency on EigenTrust rankings being available in a SQL database like PostgreSQL database. For instructions on how to generate EigenTrust rankings, please refer to the repo at https://github.com/Karma3Labs/ts-lens or to the API at https://openapi.lens.k3l.io/

To get a high-level overview of all the steps in the pipeline, take a look at the [run_pipelin.sh](https://github.com/Karma3Labs/lens-recommendation-be/blob/main/pipeline/run_pipeline.sh) script.
- export data from Lens BigQuery to a datetime partitioned GCS bucket (_we avoid expensive BigQuery access costs_)
- read recent (using partition info) data from the GCS bucket, extract features and write to Vertex AI Featurestore (_featurestore allows us to go back in time and look at feature values_)
- read EigenTrust scores and rankings from a `globaltrust` table in a PostgreSQL database, and upload to Featurestore
- fetch features for recent posts from Featurestore and evaluate the model to generate labels (`NO/YES/MAYBE`) and store predictions in a GCS bucket
- load predictions from the GCS bucket and pick stratified sample of rows to insert into a `feed` table in PostgreSQL
- [ts-lens](https://github.com/Karma3Labs/ts-lens) can then be used to serve recommendations from the `feed` table

## Setting up local environment

Verify that your local environment is configured correctly (assuming you already have `gcloud` installed)
```
gcloud config list
gcloud auth list
```

Authenticate 
```
# authenticate CLI so you can interact with GCP from command-line
gcloud auth login

# generate Application Default Credentials so any code that is run locally can interact with GCP
gcloud auth application-default login
```

## Setting up GCP environment
### Enable required Google Cloud services
```
gcloud services enable aiplatform.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable dataproc.googleapis.com
```
### Setup compute and network policies to allow Dataproc Serverless to run PySpark jobs
```
export GCP_PROJECT=<<YOUR_GCP_PROJECT>

# Dataproc Serverless will startup internal executor nodes without login. 
# Disable policies that require OS login.
gcloud resource-manager org-policies disable-enforce compute.requireOsLogin --project $GCP_PROJECT

# Dataproc needs to be able to control executors running in your private VPC.
gcloud resource-manager org-policies disable-enforce compute.vmExternalIpAccess --project $GCP_PROJECT

# Set default region for Dataproc
gcloud config set dataproc/region us-central1

# Create default network if it doesn't already exist
gcloud compute networks create default

# Create default subnet if it doesn't already exist
gcloud compute networks subnets create default-sub \
--subnet-mode=custom \
--network=default
--range=10.0.0.0/24

# Allow Dataproc to access executors running in your private VPC
gcloud compute networks subnets update default \
  --region=us-cental1 \
  --enable-private-ip-google-access

gcloud compute firewall-rules create allow-internal-ingress \
  --network=default \
  --source-ranges="10.0.0.1/24" \
  --direction="ingress" \
  --action="allow" \
  --rules="all"

gcloud compute firewall-rules create outbound-pgsql \
--description="outbound traffic to PostgresSQL DB on eigne1" \
--direction=EGRESS \
--priority=1000 \
--network=default \
--action=ALLOW \
--rules=tcp:5432 \
--destination-ranges=65.21.77.173/32 \

# Dataproc serverless cannot access internet because it has only internal IPs
# so create a Cloud NAT
gcloud compute routers create default-router \
--network=default \
--region=us-central1

gcloud compute routers nats create dataproc-serverless-nat \
--router=default-router \
--region=us-central1 \
--auto-allocate-nat-external-ips \
--nat-all-subnet-ip-ranges
```
### Setup IAM permissions for Dataproc compute
```
export IAM_COMPUTE=<<default compute service account for your project>>

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$IAM_COMPUTE@developer.gserviceaccount.com --role=roles/dataproc.worker

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$IAM_COMPUTE@developer.gserviceaccount.com --role=roles/bigquery.user

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$IAM_COMPUTE@developer.gserviceaccount.com --role=roles/bigquery.dataEditor

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$IAM_COMPUTE@developer.gserviceaccount.com --role=roles/aiplatform.featurestoreUser
```

### Custom Docker Image with dependencies to be used by Dataproc Serverless
Building custom image for use in Dataproc
```
export IMAGE_NAME=<<choose a name>>
gcloud config set project $GCP_PROJECT
mkdir image
cd image
mkdir lib
mkdir bin

wget -P lib https://jdbc.postgresql.org/download/postgresql-42.5.4.jar
gsutil cp gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.30.0.jar lib/
wget -P lib/ https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.13/gcs-connector-hadoop2-2.2.13-shaded.jar
wget -P bin/ https://repo.anaconda.com/miniconda/Miniconda3-py39_23.3.1-0-Linux-x86_64.sh

cp ~/<<gitclonethisrepo>>/pipeline/Dockerfile .
cp ~/<<gitclonethisrepo>>/pipeline/requirements.txt .

export IMAGE=gcr.io/$GCP_PROJECT/$IMAGE_NAME
docker build -t "${IMAGE}" .
docker push "${IMAGE}"
```

### Setup IAM permissions for pipeline script
```
export IAM_PIPELINE = <<choose a name>>

gcloud iam service-accounts create $IAM_PIPELINE

gcloud iam service-accounts keys create $IAM_PIPELINE-creds.json --iam-account=$IAM_PIPELINE@$GCP_PROJECT.iam.gserviceaccount.com

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$IAM_PIPELINE@$GCP_PROJECT.iam.gserviceaccount.com --role=roles/dataproc.worker

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$IAM_PIPELINE@$GCP_PROJECT.iam.gserviceaccount.com --role=roles/dataproc.editor

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$IAM_PIPELINE@$GCP_PROJECT.iam.gserviceaccount.com --role=roles/bigquery.user

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$IAM_PIPELINE@$GCP_PROJECT.iam.gserviceaccount.com --role=roles/bigquery.dataEditor

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$IAM_PIPELINE@$GCP_PROJECT.iam.gserviceaccount.com --role=roles/aiplatform.featurestoreUser

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$IAM_PIPELINE@$GCP_PROJECT.iam.gserviceaccount.com --role=roles/roles/storage.objectCreator

gcloud projects add-iam-policy-binding $GCP_PROJECT --member=serviceAccount:$IAM_PIPELINE@$GCP_PROJECT.iam.gserviceaccount.com --role=roles/iam.serviceAccountTokenCreator
```

### Setup pipeline script user to impersonate the default compute service account
```

gcloud iam service-accounts add-iam-policy-binding $IAM_COMPUTE@developer.gserviceaccount.com --member=serviceAccount:$IAM_PIPELINE@$GCP_PROJECT.iam.gserviceaccount.com --role=roles/iam.serviceAccountUser

gcloud iam service-accounts add-iam-policy-binding $IAM_COMPUTE@developer.gserviceaccount.com --member=serviceAccount:$IAM_PIPELINE@$GCP_PROJECT.iam.gserviceaccount.com --role=roles/iam.serviceAccountTokenCreator
```

### Create a Google Compute Engine instance to run pipeline script
Running pipeline on free-tier GCE (**WARNING** when using the console, pay attention to the network tier that is being used for your GCE instance. Google defaults (sneaky and somewhat dishonest) to Premium Tier instead of Standard Tier, and Premium Tier networking bills can add up pretty quickly)
```
export INSTANCE_NAME=lens-recommender-gce
gcloud compute instances create $INSTANCE_NAME \
    --project=$GCP_PROJECT \
    --zone=us-central1-a \
    --machine-type=e2-micro \
    --network-interface=network-tier=STANDARD,stack-type=IPV4_ONLY,subnet=default-sub \
    --metadata=enable-oslogin=true \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=$IAM_COMPUTE@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --create-disk=auto-delete=yes,boot=yes,device-name=$INSTANCE_NAME,image=projects/debian-cloud/global/images/debian-11-bullseye-v20230509,mode=rw,size=10,type=projects/$GCP_PROJECT/zones/us-central1-a/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=goog-ec-src=vm_add-gcloud \
    --reservation-affinity=any
```

### Create Google Cloud Storage buckets
```
# bucket to export lens data from BigQuery to GCS for easier bulk processing
gsutil mb gs://vijay-lensv2-bigquery-export/

# bucket required by Dataproc for temporary artifacts
gsutil mb gs://vijay-lens-dataproc-temp/

# bucket required by Vertex AI Featurestore for temporary artifacts
gsutil mb gs://vijay-lens-feature-store-temp/

# bucket to persist different version of the ML model and predictions from the model versions
gsutil mb gs://vijay-lensv2-ml/

# a bucket to host pyspark dependencies
gsutil mb gs://vijay-lens-pyspark-deps/
```
## Generate EigenTrust Profile scores and rankings
You can either clone the [ts-lens repo](https://github.com/Karma3Labs/ts-lens) repo and generate scores and rankings yourself, or you can use the [ts-lens api](https://openapi.lens.k3l.io/) to fetch them. 
The scores and rankings must be hosted in a SQL database table with the following schema.
```
TABLE NAME: globaltrust

COLUMNS:
  strategy_name VARCHAR(255)  -- default value 'followship'
  i VARCHAR(255) -- lens profile_ids
  v FLOAT8 -- eigentrust scores
  date DATE -- date when scores were generated
```

## Start the pipeline
### Setup the Compute Engine instance you created in the previous step
```
gcloud compute ssh --zone "us-central1-a" $INSTANCE_NAME --project $GCP_PROJECT

sudo apt-get install python3-pip

git clone https://github.com/Karma3Labs/lens-recommendation-be

cd lens-recommendation-be/pipeline

python3 -m pip install -r requirements.txt
```

### Setup output table
The pipeline writes recommendations to a table in the same SQL database as the one with the EigenTrust scores and rankings. The output table must have the following schema.
```
TABLE NAME: feed

COLUMNS:
  strategy_name TEXT  -- ml-xgb-followship
  post_id TEXT -- post_id of the post
  v FLOAT8 -- ordering rank for each post
```

### Run the pipeline script
Setup environment variables and run pipeline script
```
PGSQL_URL="postgresql://username:password@dbhost:dbport/dbname"
JDBC_URL="jdbc:postgresql://dbhost:dbport/dbname?user=username&password=secret"
bash run_pipeline.sh
```

### Troubleshooting
**MEMORY PRESSURE ERROR**: if you run into 'memory pressure' errors, try increasing driver memory (_must be between 1024m and 7424m per driver core_):
```
--properties=spark.driver.cores=4,spark.driver.memory=20g 
```

**CPU QUOTA ERROR**: If you run into a "Insufficient 'CPUS' quota" error, try adding a sleep between the different dataproc batch submissions:
```
ERROR: (gcloud.dataproc.batches.submit.pyspark) Batch job is FAILED. Detail: Insufficient 'CPUS' quota. Requested 12.0, available 11.0.
```

**GCS Storage Utilization**: Dataproc batch jobs create a lot of temp objects that keep growing over time. If you don't cleanup periodically, your GCP bill keep growing. 
1. Run the `cleanup.sh` script in a scheduler like crontab. 
2. In addition the cleanup script, you also need to identify the `staging` and `temp` GCS buckets that Dataproc creates. For example, gs://dataproc-staging-us-central1-0123456789-abcdefgh and gs://dataproc-temp-us-central1-0123456789-xyz12345
    1. Set object lifecycle on staging to auto-delete after 30 days - `gsutil lifecycle set gs_staging_lifecycle.json gs://dataproc-staging-us-central1-CHANGE-ME`
    2. Set object lifecycle on temp to auto-delete after 30 days - `gsutil lifecycle set gs_staging_lifecycle.json gs://dataproc-temp-us-central1-CHANGE-ME`