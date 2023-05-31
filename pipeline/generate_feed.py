import time
from datetime import datetime, timezone
import argparse
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

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

dtime_now = int(datetime.utcnow().replace(microsecond=0).strftime("%Y%m%d%H%M%S"))
print(f"dtime_now:{dtime_now}")

condn_1day = dtime_now - col("dtime") < 86400
condn_7day = dtime_now - col("dtime") < 604800
condn_30day = dtime_now - col("dtime") < 2592000

df = df.withColumn("time_ago",
                   when(condn_1day , "1d") \
                   .when(condn_7day, "7d") \
                   .when(condn_30day, "30d") \
                   .otherwise("99d"))
print("converting Pyspark dataframe to Pandas")
pd_df = df.toPandas()
print('YES', pd_df[pd_df['recommend'] == 'YES']['time_ago'].value_counts())
print('MAYBE', pd_df[pd_df['recommend'] == 'MAYBE']['time_ago'].value_counts())

rng = np.random.default_rng()
samples = []
counts = pd_df['time_ago'].value_counts()
samples.append(pd_df.loc[pd_df['time_ago'] == '1d'].sample(n=min(counts['1d'],100), random_state=rng))
samples.append(pd_df.loc[pd_df['time_ago'] == '7d'].sample(n=min(counts['7d'],50), random_state=rng))
samples.append(pd_df.loc[pd_df['time_ago'] == '30d'].sample(n=min(counts['30d'],50), random_state=rng))
samples.append(pd_df.loc[pd_df['time_ago'] == '99d'].sample(n=min(counts['99d'],50), random_state=rng))
sample_df = pd.concat(samples)
print('Sampled', sample_df['time_ago'].value_counts())

sample_df['weights'] = np.where(sample_df['recommend'] == 'YES', .8, .2)
sample_df = sample_df.sample(n=100, weights='weights', random_state=rng)
print('Recommend', sample_df['recommend'].value_counts())

sample_df = sample_df.sort_values(['dtime'], ascending=[False], ignore_index=True)
sample_df = sample_df[['post_id']]
sample_df['strategy_name'] = "ml-xgb-followship"
sample_df['v'] = sample_df.index
print(sample_df.head())

SQLALCHEMY_SILENCE_UBER_WARNING=1
db = create_engine(args.pgsql_url);
print("Deleting older feed entries for strategy 'ml-xgb-followship'")
db.execute("DELETE FROM feed WHERE strategy_name = 'ml-xgb-followship'")

print("Inserting new feed entries for strategy 'ml-xgb-followship'")
sample_df.to_sql('feed', con=db, if_exists='append', index=False)
