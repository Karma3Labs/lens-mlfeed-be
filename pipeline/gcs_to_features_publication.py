import argparse
from google.cloud import storage
import google.cloud.aiplatform as aiplatform
from google.cloud.aiplatform import Featurestore, EntityType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, datediff, date_format
from pyspark.sql.types import BooleanType,TimestampType, IntegerType
from datetime import datetime, timezone
import pandas as pd

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
# Enable pyarrow to reduce memory pressure on driver when converting pyspark to pandas
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true") # deprecated in Spark 3.0

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
  df = df.where(f"dtime > {prev_checkpoint.strip()}")
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
          .option('table',f'lens-public-data.v2_polygon.{table_name}') \
          .load()
  return bq_df

def save_to_featurestore(spark_df:DataFrame):
  # convert from pyspark to pandas because Featurestore API works only with pandas 
  # WARNING: could result in OutOfMemory errors. 
  # Possible solutions: 
  # 1. increase spark.driver.memory 
  # 2. wrap the toPandas and ingest_from_df code in a function and call spark_df.rdd.mapPartition(map_fun)
  DF = spark_df.toPandas() 
  FEATURE_IDS = [col for col in DF.columns.values]
  ENTITY_ID_FIELD = "post_id"
  # get reference to posts featurestore entity 
  posts_entity_type = fs.get_entity_type(entity_type_id="posts")
  posts_entity_type.ingest_from_df(
    feature_ids = FEATURE_IDS,
    feature_time = FEATURE_TIME,
    df_source = DF,
    feature_source_fields = None,
    entity_id_field = ENTITY_ID_FIELD,
  )

def save_record_features(rec_df:DataFrame) -> DataFrame:
  rec_df = rec_df.select(
    col("publication_id").alias("post_id"),
    "profile_id",
    "parent_publication_id", 
    col("is_hidden").cast(BooleanType()), 
    lit(False).alias("is_gated"),
    "block_timestamp",
    col("gardener_flagged").alias("custom_filters_gardener_flagged").cast(BooleanType())
  )

  rec_df = rec_df \
              .withColumn("is_original", 
                  when(rec_df.parent_publication_id.isNull(), lit(True)) \
                  .otherwise(lit(False))) \
              .withColumn("block_timestamp", col("block_timestamp").cast(TimestampType())) \
              .withColumn("age", datediff(lit(FEATURE_TIME), col("block_timestamp")).cast(IntegerType()))

  rec_df = rec_df.drop(col("parent_publication_id"))

  # Pandas doesn't support timestamps with nanoseconds. 
  # Remove nanoseconds before converting to Pandas.
  rec_df = rec_df \
            .withColumn("block_timestamp", date_format("block_timestamp", "yyyy-MM-dd HH:mm:ss")) \
  
  save_to_featurestore(rec_df)            
  return 

def save_metadata_features(meta_df:DataFrame) -> DataFrame:
  meta_df = meta_df.select(
    col("publication_id").alias("post_id"),
    "language",
    "region",
    "content_warning",
    "main_content_focus",
    "tags_vector",
  )

  meta_df = meta_df \
              .withColumn("is_content_warning",
                  when(meta_df.content_warning.isNull(), lit(False)) \
                  .otherwise(lit(True))) \

  save_to_featurestore(meta_df)
  return 

def save_stats_features(stats_df:DataFrame) -> DataFrame:
  stats_df = stats_df.select(
    col("publication_id").alias("post_id"),
    col("total_amount_of_collects").alias("collects").cast(IntegerType()),
    col("total_amount_of_mirrors").alias("mirrors").cast(IntegerType()),
    col("total_amount_of_comments").alias("comments").cast(IntegerType())
  )

  save_to_featurestore(stats_df)
  return 

def save_reaction_features(reactions_df:DataFrame) -> DataFrame:
  reactions_df = reactions_df.select(
    col("publication_id").alias("post_id"),
    "reaction_type", 
    "total"
  )
  reactions_df = reactions_df \
                    .withColumn("upvotes",
                        when(reactions_df.reaction_type == "UPVOTE", col("total")) \
                        .otherwise(lit(0))) \
                    .withColumn("downvotes",
                        when(reactions_df.reaction_type == "DOWNVOTE", col("total")) \
                        .otherwise(lit(0)))
  reactions_df = reactions_df.drop(col("reaction_type")) \
                              .drop(col("total"))

  # ensure that int types are Int64 and not object types
  reactions_df = reactions_df \
                    .withColumn("upvotes", col("upvotes").cast(IntegerType())) \
                    .withColumn("downvotes", col("downvotes").cast(IntegerType())) 
  # DF['upvotes'] = DF['upvotes'].astype('Int64')
  # DF['downvotes'] = DF['downvotes'].astype('Int64')

  save_to_featurestore(reactions_df)

  return 

if __name__ == '__main__':

  # get reference to posts featurestore entity 
  posts_entity_type = fs.get_entity_type(entity_type_id="posts")

  table_names = ['publication_record', 
                 'publication_metadata',
                 'global_stats_publication',
                 'global_stats_publication_reaction']
  
  for table in table_names:
    df = load_from_parquet(args.bucket, table)
    if df.count() > 0:
      df.show(5, truncate=False)
      # Refactoring note: we are using py3.9 so no support for switch case 
      if table == 'publication_record':
        save_record_features(df)
      elif table == 'publication_metadata':
        save_metadata_features(df)
      elif table == 'global_stats_publication':
        save_stats_features(df)
      elif table == 'global_stats_publication_reaction':
        save_reaction_features
      save_next_checkpoint(df, args.bucket, table)
    else:
      print(f"No new records in {table}")
