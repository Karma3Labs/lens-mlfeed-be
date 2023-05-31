import time
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
args = parser.parse_args()

spark = SparkSession.builder \
            .appName("LensFeatures") \
            .getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

df = spark.read.parquet(args.gcspath)

df = df.where(f"recommend != 'NO'")
df = df.select("post_id", "recommend", "dtime")
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

seed = round(time.time()*1000)
sample_df = df.sampleBy("recommend", fractions={'YES': yes_fraction, 'MAYBE': maybe_fraction}, 
                        seed=seed)

sample_pd_df =  sample_df.toPandas()
sample_pd_df['strategy_name'] = "ml-xgb-followship"
sample_pd_df = sample_pd_df.sort_values(['dtime'], ascending=[False], ignore_index=True)
sample_pd_df['v'] = sample_pd_df.index
print(sample_pd_df.head())
sample_pd_df.drop(['recommend', 'dtime'], axis=1, inplace=True)

SQLALCHEMY_SILENCE_UBER_WARNING=1
db = create_engine(args.pgsql_url);
print("Deleting older feed entries for strategy 'ml-xgb-followship'")
db.execute("DELETE FROM feed WHERE strategy_name = 'ml-xgb-followship'")

print("Inserting new feed entries for strategy 'ml-xgb-followship'")
sample_pd_df.iloc[:100].to_sql('feed', con=db, if_exists='append', index=False)
