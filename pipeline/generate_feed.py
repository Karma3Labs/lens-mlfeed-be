import os
import argparse
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, monotonically_increasing_id

parser = argparse.ArgumentParser()

parser.add_argument("-s", "--gcspath",
                    help="GCS bucket to read from." \
                      " For example, gs://vijay-lens-ml/predictions/20230522053757_xgbcl/",
                    required=True)
parser.add_argument("-p", "--pgsql-url",
                    help="URL to postgres db" \
                      " For example, postgresql://username:password@dbhost:dbport/dbname",
                    required=True)
parser.add_argument("-j", "--jdbc-url",
                    help="URL to postgres db in jdbc format" \
                      " For example, jdbc:postgresql://dbhost:dbport/dbname?user=username&password=secret",
                    required=True)
args = parser.parse_args()

spark = SparkSession.builder \
            .appName("LensFeatures") \
            .getOrCreate()

df = spark.read.parquet(args.gcspath)

df = df.where(f"recommend != 'NO'")
df = df.select("post_id", "recommend")
df.printSchema()

total_yes = df.select('post_id').where(df.recommend == 'YES').count()
print(f"total_yes:{total_yes}")
      
total_maybe = df.select('post_id').where(df.recommend == 'YES').count()
print(f"total_maybe:{total_maybe}")

# we need 100 rows but sampling sometimes returns less than 100; start with 120
num_yes = 0.8*120
num_maybe = 0.2*120

yes_fraction = round(num_yes / total_yes, 10)
print(f"yes_fraction:{yes_fraction}")

maybe_fraction = round(num_maybe / total_maybe, 10)
print(f"maybe_fraction:{maybe_fraction}")

sample_df = df.sampleBy("recommend", fractions={'YES': yes_fraction, 'MAYBE': maybe_fraction}, seed=0)

sample_df = sample_df.select(
                        lit("ml-xgb-followship").alias("strategy_name"), # "EigenTrust + ML"
                        "post_id", 
                        monotonically_increasing_id().alias('v'))

SQLALCHEMY_SILENCE_UBER_WARNING=1
db = create_engine(args.pgsql_url);
db.execute("DELETE FROM feed WHERE strategy_name = 'ml-xgb-followship'")
print("Deleted older feed entries for strategy 'ml-xgb-followship'")

print("Inserting new feed entries for strategy 'ml-xgb-followship'")
sample_df.limit(100).write.format("jdbc")\
    .option("url", args.jdbc_url) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "feed") \
    .mode("append") \
    .save()
