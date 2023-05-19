import argparse 
from google.cloud import storage
from pyspark.sql import SparkSession, DataFrame

parser = argparse.ArgumentParser()

parser.add_argument("-b", "--bucket",
                  help="GCS bucket name to read from",
                  required=True)
args = parser.parse_args()

spark = SparkSession.builder \
            .appName("LensFeatures") \
            .getOrCreate()

def get_checkpoint_blob(bucket_name:str, table_name: str) -> storage.Blob:
  gcs_client = storage.Client()
  bucket = gcs_client.get_bucket(bucket_name)
  checkpoint_filepath = f"checkpoints/fs_{table_name}.txt"
  blob = bucket.get_blob(checkpoint_filepath)
  if not blob:
    blob = storage.Blob(checkpoint_filepath, bucket)
  return blob

def read_parquet(bucket_name:str, table_name: str) -> DataFrame:
  blob = get_checkpoint_blob(bucket_name, table_name)
  if blob.size and blob.size> 0:
    prev_checkpoint = blob.download_as_text()
  else:
    # no prev checkpoint- first run of job, harcode datetime
    # We started computing features on or after May 15, 2023
    prev_checkpoint = "20230515000000" 

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

profiles_df = read_parquet(args.bucket, 'public_profile')
save_next_checkpoint(profiles_df, args.bucket, 'public_profile')

posts_df = read_parquet(args.bucket, 'public_profile_post')
save_next_checkpoint(posts_df, args.bucket, 'public_profile_post')





