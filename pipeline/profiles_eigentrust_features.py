from dotenv import load_dotenv
load_dotenv()

import os
connect_url = os.getenv("PGSQL_URL")

import argparse
parser = argparse.ArgumentParser()

from datetime import datetime, timezone

parser.add_argument("-f", "--featurestore",
                    help="Feature store id. For example, lens_featurestore_d2",
                    required=True)
parser.add_argument("-t", "--time",
                    help="Time in UTC to be recorded in Featurestore." 
                      " For example, '2023-05-18 22:59:59 UTC'",
                    default=datetime.utcnow().replace(microsecond=0),
                    type=lambda f: datetime.strptime(f, "%Y-%m-%d %H:%M:%S %Z"),
                    required=False)
args = parser.parse_args()

# FEATURE_TIME = datetime.datetime(year=2023, month=5, day=20, hour=0, minute=0, second=0)
FEATURE_TIME = args.time
print(FEATURE_TIME)

from sqlalchemy import create_engine
engine = create_engine(connect_url);
conn = engine.connect().execution_options(stream_results=True)

sql_query = """
WITH MAX_DATE AS (
  SELECT max(date) as maxdate
  FROM globaltrust
  WHERE strategy_name = 'followship'
)
SELECT 
    ROW_NUMBER() OVER(ORDER BY v DESC) AS rank, 
    g.v AS score, 
    p.handle AS profile_handle,
    p.profile_id
FROM globaltrust AS g
INNER JOIN profile AS p ON p.profile_id = g.i
WHERE 
  strategy_name = 'followship' 
  AND date = (SELECT maxdate FROM MAX_DATE)
"""
print(sql_query)

import pandas as pd
profile_trust_df = pd.read_sql(sql_query, conn)
profile_trust_df.head()
profile_trust_df.info()
# look for duplicate profiles
print(profile_trust_df["profile_id"].value_counts()[lambda x: x>1])

PROJECT_ID = "boxwood-well-386122"
REGION = "us-central1"
BUCKET_URI = "gs://vijay-lens-feature-store-temp"  
from google.cloud import aiplatform
aiplatform.init(project=PROJECT_ID, location=REGION, staging_bucket=BUCKET_URI)

# TODO need to create a service account and use service account credentials
# Credentials saved to file: [/Users/vijay/.config/gcloud/application_default_credentials.json]
# These credentials will be used by any library that requests Application Default Credentials (ADC).
# ! gcloud auth application-default login

FEATURESTORE_ID = args.featurestore
from google.cloud.aiplatform import Featurestore
fs = Featurestore(
    featurestore_name=FEATURESTORE_ID
)
print(fs.gca_resource)

profiles_entity_type = fs.get_entity_type(entity_type_id="profiles")
profiles_entity_type.list_features()
PROFILES_FEATURES_IDS = ['followship_rank', 'followship_score']
PROFILES_SRC_FIELDS = {
    'followship_rank': 'rank',
    'followship_score': 'score',
}
PROFILES_ENTITY_ID_FIELD = "profile_id"
PROFILES_DF = profile_trust_df
profiles_entity_type.ingest_from_df(
    feature_ids = PROFILES_FEATURES_IDS,
    feature_time = FEATURE_TIME,
    df_source = PROFILES_DF,
    feature_source_fields = PROFILES_SRC_FIELDS,
    entity_id_field = PROFILES_ENTITY_ID_FIELD,
)