import argparse, io, pickle
from datetime import datetime, timezone

from google.protobuf.timestamp_pb2 import Timestamp
from google.cloud import storage
from google.cloud import aiplatform
from google.cloud.aiplatform import Featurestore

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, max

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.preprocessing import OrdinalEncoder

parser = argparse.ArgumentParser()

parser.add_argument("-s", "--source",
                    help="GCS bucket to read source data from." \
                      " For example, vijay-lens-bigquery-export",
                    required=True)
parser.add_argument("-l", "--mlbucket",
                    help="GCS bucket for models and predictions." \
                      " For example, vijay-lens-ml",
                    required=True)
parser.add_argument("-g", "--staging",
                    help="GCS bucket for staging use by Vertex AI." \
                      " For example, vijay-lens-feature-store-temp",
                    required=True)
parser.add_argument("-f", "--featurestore",
                    help="Feature store id. For example, lens_featurestore_d2",
                    required=True)
parser.add_argument("-m", "--modelversion",
                    help="Version of the model to use. For example, 20230521220855",
                    required=True)

args = parser.parse_args()

spark = SparkSession.builder \
            .appName("LensFeatures") \
            .getOrCreate()
# Enable pyarrow to reduce memory pressure on driver when converting pyspark to pandas
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true") # deprecated in Spark 3.0

aiplatform.init(staging_bucket=f"gs://{args.staging}")
fs = Featurestore(featurestore_name=args.featurestore)
print(fs.gca_resource)

def df_info_to_string(df):
  buf = io.StringIO()
  df.info(buf=buf, memory_usage="deep")
  return buf.getvalue()

def get_checkpoint_blob(bucket_name:str, table_name: str) -> storage.Blob:
  gcs_client = storage.Client()
  bucket = gcs_client.get_bucket(bucket_name)
  checkpoint_filepath = f"checkpoints/xgb_{table_name}.txt"
  blob = bucket.get_blob(checkpoint_filepath)
  if not blob:
    blob = storage.Blob(checkpoint_filepath, bucket)
  return blob

def load_from_parquet(bucket_name:str, table_name: str) -> DataFrame:
  blob = get_checkpoint_blob(bucket_name, table_name)
  if blob.size and blob.size> 0:
    prev_checkpoint = blob.download_as_text()
  else:
    # no prev checkpoint- first run of job, harcode datetime
    # We started computing features on or after May 15, 2023
    prev_checkpoint = "20230515000000" 

  # TODO FIXME: risk of reading duplicate records when reading multiple days worth of data
  df = spark.read \
            .parquet(f"gs://{bucket_name}/{table_name}/")
  df = df.where(f"dtime > {prev_checkpoint}")
  return df 

def save_next_checkpoint(df:DataFrame, bucket_name:str, table_name: str):
  next_checkpoint = df.select(max(df.dtime).alias("dtime_max")).collect()[0]
  next_checkpoint = next_checkpoint.dtime_max
  print(f"next_checkpoint: {next_checkpoint}")
  blob = get_checkpoint_blob(bucket_name, table_name)
  blob.upload_from_string(f"{next_checkpoint}")

def load_model(bucket_name:str, model_version:str) -> xgb.Booster:
  model_filename = f"{model_version}_xgbcl.json"
  gcs_client = storage.Client()
  bucket = gcs_client.get_bucket(bucket_name)
  blob = bucket.get_blob(f"models/{model_filename}")
  blob.download_to_filename(model_filename)
  model = xgb.Booster()
  model.load_model(model_filename)
  print(model.feature_names)
  print(model.get_fscore())
  return model

def load_recommend_encoder(bucket_name:str, model_version:str) -> OrdinalEncoder:
  encoder_filename = f"{model_version}_recommend_encoder.pkl"
  gcs_client = storage.Client()
  bucket = gcs_client.get_bucket(bucket_name)
  blob = bucket.get_blob(f"models/{encoder_filename}")
  blob.download_to_filename(encoder_filename)
  enc = pickle.load(open(encoder_filename, 'rb'))
  print(enc.categories_)
  return enc  

def load_from_featurestore(posts_df:pd.DataFrame) -> pd.DataFrame :
  FEATURES_IDS = {"posts": ["*"], "profiles": ["*"]}

  posts_df = posts_df.select(
                      col("post_id").alias("posts"),
                      col("profile_id").alias("profiles"))
  INSTANCES_DF = posts_df.toPandas()
  # Vertex Featuretore timestamp should be millisecond precision.
  ts = datetime.utcnow().replace(microsecond=0).isoformat(sep='T', timespec='milliseconds')+'Z'
  INSTANCES_DF['timestamp'] = pd.Timestamp(ts)
  print(f"{'*' * 5}INSTANCES_DF{'*' * 5}")
  print(INSTANCES_DF.head())
  print(df_info_to_string(INSTANCES_DF))

  # CHANGE the date below if you want to ignore features before some date
  bad_feature_time = datetime(year=2023, month=5, day=11, hour=0, minute=0, second=1)
  START_TIME = Timestamp()
  START_TIME.FromDatetime(bad_feature_time)
  print(f"ignoring features before {START_TIME}")

  features_df = fs.batch_serve_to_df(
    serving_feature_ids = FEATURES_IDS,
    read_instances_df = INSTANCES_DF,
    start_time = START_TIME,
  )
  print(f"{'*' * 5}features_df{'*' * 5}")
  print(features_df.head())
  print(df_info_to_string(features_df))
  return features_df

def transform_features(features_df:pd.DataFrame) -> pd.DataFrame:
  features_df['followship_rank'] = features_df['followship_rank'].astype('Int64')
  max_features = features_df \
                .groupby(["followship_rank"]) \
                .agg({'age': ['max'], 
                      'mirrors': ['max'], 
                      'collects': ['max'], 
                      'comments': ['max']})
  max_features.columns = ['max_age', 'max_mirrors', 'max_collects', 'max_comments']
  post_score_df = features_df.join(max_features, on='followship_rank', how='left')

  def calc_post_score(row):
    return 1 * row['mirrors']/(row['max_mirrors'] if row['max_mirrors'] > 0 else 1e10) \
            + 1 * row['collects']/(row['max_collects'] if row['max_collects'] > 0 else 1e10) \
            + 3 * row['comments']/(row['max_comments'] if row['max_comments'] > 0 else 1e10) \
            - 5 * row['age']/(row['max_age'] if row['max_age'] > 0 else 1e10)

  post_score_df['post_score'] = post_score_df.apply(calc_post_score, axis=1)
  # just make sure datatypes are correct
  post_score_df['collects'] = post_score_df['collects'].astype('Int64')
  post_score_df['custom_filters_gardener_flagged'] = post_score_df['custom_filters_gardener_flagged'].astype('bool')
  post_score_df['upvotes'] = post_score_df['upvotes'].astype('Int64')
  post_score_df['mirrors'] = post_score_df['mirrors'].astype('Int64')
  post_score_df['is_original'] = post_score_df['is_original'].astype('bool')
  post_score_df['is_content_warning'] = post_score_df['is_content_warning'].astype('bool')
  post_score_df['age'] = post_score_df['age'].astype('Int64')
  post_score_df['downvotes'] = post_score_df['downvotes'].astype('Int64')
  post_score_df['comments'] = post_score_df['comments'].astype('Int64')
  post_score_df['downvotes'] = post_score_df['downvotes'].astype('Int64')
  post_score_df['max_age'] = post_score_df['max_age'].astype('Int64')
  post_score_df['max_mirrors'] = post_score_df['max_mirrors'].astype('Int64')
  post_score_df['max_collects'] = post_score_df['max_collects'].astype('Int64')
  post_score_df['max_comments'] = post_score_df['max_comments'].astype('Int64')

  print(f"{'*' * 5}post_score_df{'*' * 5}")
  print(post_score_df.head())
  print(df_info_to_string(post_score_df))
  return post_score_df

def predict_labels(input_df: pd.DataFrame, 
                   model_p: xgb.Booster, 
                   enc:OrdinalEncoder
                   ) -> pd.DataFrame:
  # model doesn't use post_id, let's pull it out separately. 
  # we can merge it back in later
  input_df, input_id_df = input_df.drop('entity_type_posts', axis=1), input_df['entity_type_posts'] 

  # recommend column values will be predicted. drop it from input.
  input_df.drop('recommend', axis=1, inplace=True)

  # the columns and the order in which they appear should match 
  # exactly with the trained model
  input_df = input_df[model_p.feature_names]

  print(f"{'*' * 5}input_df{'*' * 5}")
  print(input_df.head())
  print(df_info_to_string(input_df))

  print(f"{'*' * 5}input_id_df{'*' * 5}")
  print(input_id_df.head())
  print(df_info_to_string(input_id_df))

  # non-numeric columns must be converted to category for model
  cats = input_df.select_dtypes(exclude=np.number).columns.tolist()
  for col in cats:
      input_df[col] = input_df[col].astype('category')

  # convert to xgb DMatrix format
  dinput_df = xgb.DMatrix(input_df, enable_categorical=True)

  #model predictions
  predicted_y = model_p.predict(dinput_df)

  # predicted values should be rounded ints 
  predicted_y_int = np.rint(predicted_y)

  # convert predicted ints into classes - YES, MAYBE, NO
  # sklearn encoder inverse_transform requires a 2D array
  predicted_y_int = enc.inverse_transform(predicted_y_int.reshape(-1, 1))
  # convert back from 2D array to 1D array so we can add column to Dataframe
  predicted_y_int = predicted_y_int.reshape(-1)

  output_df = pd.merge(input_df, input_id_df, left_index=True, right_index=True)
  # add prediction to the dataframe
  output_df['recommend'] = predicted_y_int
  print(f"recommendations: {output_df['recommend'].value_counts()}")
  output_df = output_df.rename(columns={'entity_type_posts': 'post_id'})

  print(f"{'*' * 5}output_df{'*' * 5}")
  print(output_df.head())
  print(df_info_to_string(output_df))

  return output_df

def save_recommendations(bucket_name:str, model_version:str, output_df: pd.DataFrame):
  # if there are NaN, converting pandas to pyspark fails with error
  # can not merge type <class 'pyspark.sql.types.longtype'> and <class 'pyspark.sql.types.structtype'>
  # followship_rank 0 does not make sense so let's set it to a very large value
  # all other columns, set nan to 0 or ''

  # TODO FIXME should we do this step before prediction ?
  # - if we fillna before prediction, then posts by unknown profiles may never get recommended
  output_df['followship_rank'] = output_df['followship_rank'].fillna(1e10)
  not_cat_cols = output_df.select_dtypes(exclude='category').columns.tolist()
  for col in not_cat_cols:
    output_df[col].fillna(0, inplace=True)
  cat_cols = output_df.select_dtypes(include='category').columns.tolist()
  for col in cat_cols:
    output_df[col] = output_df[col].astype('string')
    output_df[col].fillna('', inplace=True)

  print(f"{'*' * 5}output_df{'*' * 5}")
  print(output_df.head())
  print(df_info_to_string(output_df))

  recommendations_df = spark.createDataFrame(output_df)

  now = datetime.now(timezone.utc)
  datetime_path = f"dtime={now.strftime('%Y%m%d%H%M%S')}"
  recommendations_df.write \
                    .partitionBy("recommend") \
                    .mode('overwrite') \
                    .parquet(f"gs://{bucket_name}/predictions/{model_version}_xgbcl/{datetime_path}")

if __name__ == '__main__':

  posts_df = load_from_parquet(args.source, 'public_profile_post')
  
  if posts_df.count() > 0:
    features_df = load_from_featurestore(posts_df)

    input_df = transform_features(features_df)
    model_p = load_model(args.mlbucket, args.modelversion)
    enc = load_recommend_encoder(args.mlbucket, args.modelversion)

    output_df = predict_labels(input_df, model_p, enc)

    save_recommendations(args.mlbucket, args.modelversion, output_df)

    save_next_checkpoint(posts_df, args.source, 'public_profile_post')
  else:
    print(f"No new records in public_profile_post")

