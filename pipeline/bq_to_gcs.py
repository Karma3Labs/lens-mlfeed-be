import argparse 

parser = argparse.ArgumentParser()

parser.add_argument("-b", "--bucket",
                  help="GCS bucket name to write output",
                  required=True)
args = parser.parse_args()

from pyspark.sql import SparkSession
spark = SparkSession.builder \
            .appName("LensFeatures") \
            .getOrCreate()

spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationDataset","spark_materialization")

def bq_diff_to_parquet(bucket_name:str, table_name: str):
  from google.cloud import storage
  gcs_client = storage.Client()
  bucket = gcs_client.get_bucket(bucket_name)
  checkpoint_filepath = f"checkpoints/bq_{table_name}.txt"
  blob = bucket.get_blob(checkpoint_filepath)
  if blob:
    prev_checkpoint = int(blob.download_as_text())
  else:
    # no prev checkpoint- first run of job
    # Lens launched on Polygon Mainnet in May 2022
    # "2022-05-01 00:00:00 UTC" 
    prev_checkpoint = 1651388400000
    blob = storage.Blob(checkpoint_filepath, bucket)

  from datetime import datetime, timezone
  now = datetime.now(timezone.utc)
  datetime_path = f"dtime={now.strftime('%Y%m%d%H%M%S')}"

  sql_query = f"""
      SELECT * 
      FROM temp_data
      WHERE datastream_metadata.source_timestamp > {prev_checkpoint}
      LIMIT 100000
    """
  print(f"querying ${table_name}:{sql_query}")

  temp_data = spark.read \
            .format('com.google.cloud.spark.bigquery') \
            .option("project", "lens-public-data") \
            .option("table", f"v2_polygon.{table_name}") \
            .load()
  # querying directly from BQ without this temp view throws AccessDenied errors 
  temp_data.createOrReplaceTempView("temp_data")
  bq_df = spark.sql(sql_query)

  print(bq_df.head())

  if not bq_df.count() > 0:
    print(f"No new records in {table_name}")
    return
  
  # go with the default compression type
  bq_df.write \
    .mode('overwrite') \
    .parquet(f"gs://{bucket_name}/{table_name}/{datetime_path}")

  from pyspark.sql.functions import max
  next_checkpoint = bq_df \
                      .select(max(bq_df.datastream_metadata.source_timestamp).alias("source_timestamp_max")) \
                      .collect()[0]
  next_checkpoint = next_checkpoint.source_timestamp_max
  print(f"next_checkpoint:{next_checkpoint} UTC")
  blob.upload_from_string(f"{next_checkpoint} UTC")

if __name__ == '__main__':

  bq_diff_to_parquet(args.bucket, 'publication_record')
  bq_diff_to_parquet(args.bucket, 'publication_metadata')
  bq_diff_to_parquet(args.bucket, 'global_stats_publication')
  bq_diff_to_parquet(args.bucket, 'global_stats_publication_reaction')




