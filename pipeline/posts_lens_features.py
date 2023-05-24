import argparse
from google.cloud import storage
import google.cloud.aiplatform as aiplatform
from google.cloud.aiplatform import Featurestore
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, datediff, date_format
from pyspark.sql.types import BooleanType,TimestampType
from datetime import datetime, timezone

parser = argparse.ArgumentParser()

parser.add_argument("-b", "--bucket",
                    help="GCS bucket to read from." \
                      " For example, vijay-lens-bigquery-export",
                    required=True)
parser.add_argument("-g", "--staging",
                    help="GCS bucket for staging use by Vertex AI." \
                      " For example, vijay-lens-feature-store-temp",
                    required=True)
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

# TODO use block_timestamp as feature time instead of a single Feature timestamp for all entities
# FEATURE_TIME = datetime.datetime(year=2023, month=5, day=18, hour=0, minute=0, second=0)
FEATURE_TIME = args.time

spark = SparkSession.builder \
            .appName("LensFeatures") \
            .getOrCreate()

aiplatform.init(staging_bucket=f"gs://{args.staging}")
fs = Featurestore(featurestore_name=args.featurestore)
print(fs.gca_resource)

def get_checkpoint_blob(bucket_name:str, table_name: str) -> storage.Blob:
  gcs_client = storage.Client()
  bucket = gcs_client.get_bucket(bucket_name)
  checkpoint_filepath = f"checkpoints/fs_{table_name}.txt"
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
  from pyspark.sql.functions import max
  next_checkpoint = df.select(max(df.dtime).alias("dtime_max")).collect()[0]
  next_checkpoint = next_checkpoint.dtime_max
  print(f"next_checkpoint: {next_checkpoint}")
  blob = get_checkpoint_blob(bucket_name, table_name)
  blob.upload_from_string(f"{next_checkpoint}")

def load_from_bigquery(table_name: str) -> DataFrame:
  bq_df = spark.read \
          .format('com.google.cloud.spark.bigquery') \
          .option('table',f'lens-public-data.polygon.{table_name}') \
          .load()
  return bq_df

def transform_posts(posts_df:DataFrame, pub_stats_df:DataFrame) -> DataFrame:
  posts_df = posts_df.select(
    "post_id", 
    "profile_id",
    "is_related_to_post", 
    "is_related_to_comment", 
    "is_hidden", 
    "is_gated",
    "block_timestamp",
    "language",
    "region",
    "content_warning",
    "main_content_focus",
    "tags_vector",
    "custom_filters_gardener_flagged"
  )

  posts_df = posts_df \
              .withColumn("is_original", 
                  when(posts_df.is_related_to_post.isNull() & 
                        posts_df.is_related_to_comment.isNull(), lit(True)) \
                  .otherwise(lit(False))) \
              .withColumn("is_content_warning",
                  when(posts_df.content_warning.isNull(), lit(False)) \
                  .otherwise(lit(True))) \
              .withColumn("is_hidden", col("is_hidden").cast(BooleanType())) \
              .withColumn("is_gated", col("is_gated").cast(BooleanType())) \
              .withColumn("custom_filters_gardener_flagged", 
                  col("custom_filters_gardener_flagged").cast(BooleanType())) \
              .withColumn("block_timestamp", col("block_timestamp").cast(TimestampType())) \
              .withColumn("age", datediff(lit(FEATURE_TIME), col("block_timestamp")))

  posts_df = posts_df.drop(col("is_related_to_post")) \
                      .drop(col("is_related_to_comment"))

  pub_stats_df = pub_stats_df.drop(col("datastream_metadata"))

  posts_features_df = posts_df.join(pub_stats_df, posts_df.post_id == pub_stats_df.publication_id, "leftouter")
  return posts_features_df

def save_posts_to_featurestore(posts_features_df:DataFrame):
  # get reference to posts featurestore entity 
  posts_entity_type = fs.get_entity_type(entity_type_id="posts")

  # recommend is the target lable not a feature
  # we don't have it while ingesting source data 
  POSTS_FEATURES_IDS = [feature.name for feature in posts_entity_type.list_features()]
  POSTS_FEATURES_IDS.remove('recommend')

  # convert from pyspark to pandas because Featurestore API works only with pandas 
  # But, Pandas doesn't support timestamps with nanoseconds
  POSTS_DF = posts_features_df \
            .withColumn("block_timestamp", date_format("block_timestamp", "yyyy-MM-dd HH:mm:ss")) \
            .toPandas()
  
  # ensure that int types are Int64 and not object types 
  POSTS_DF['age'] = POSTS_DF['age'].astype('Int64')
  POSTS_DF['total_amount_of_collects'] = POSTS_DF['total_amount_of_collects'].astype('Int64')
  POSTS_DF['total_amount_of_mirrors'] = POSTS_DF['total_amount_of_mirrors'].astype('Int64')
  POSTS_DF['total_amount_of_comments'] = POSTS_DF['total_amount_of_comments'].astype('Int64')
  POSTS_DF['total_upvotes'] = POSTS_DF['total_upvotes'].astype('Int64')
  POSTS_DF['total_downvotes'] = POSTS_DF['total_downvotes'].astype('Int64')

  # since some field names in dataframe and featurestore are different
  # we need to create a mapping for just those fields
  POSTS_SRC_FIELDS = {
    'collects': 'total_amount_of_collects',
    'upvotes': 'total_upvotes',
    'mirrors': 'total_amount_of_mirrors',
    'downvotes': 'total_downvotes',
    'comments': 'total_amount_of_comments',
  }

  POSTS_ENTITY_ID_FIELD = "post_id"

  posts_entity_type.ingest_from_df(
    feature_ids = POSTS_FEATURES_IDS,
    feature_time = FEATURE_TIME,
    df_source = POSTS_DF,
    feature_source_fields = POSTS_SRC_FIELDS,
    entity_id_field = POSTS_ENTITY_ID_FIELD,
  )
  return

if __name__ == '__main__':

  posts_df = load_from_parquet(args.bucket, 'public_profile_post')
  posts_df.show(5, truncate=False)

  pub_stats_df = load_from_bigquery('public_publication_stats')
  pub_stats_df.show(5, truncate=False)

  posts_features_df = transform_posts(posts_df, pub_stats_df)
  posts_features_df.show(5, truncate=False)

  save_posts_to_featurestore(posts_features_df)

  save_next_checkpoint(posts_df, args.bucket, 'public_profile_post')




