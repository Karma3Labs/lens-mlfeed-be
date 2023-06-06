import time
from datetime import datetime, timezone
import argparse
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr

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

df = df.where(
            (df.is_original == 'True') 
            & (df.is_content_warning != 'True')
            & (df.recommend != 'NO')
            )
df = df.withColumn("engagement_score", expr("(1 * upvotes) + (3 * mirrors) + (5 * comments)"))
df = df.select("post_id", "recommend", "dtime", "engagement_score")
df.printSchema()

dtime_now = int(datetime.utcnow().replace(microsecond=0).strftime("%Y%m%d%H%M%S"))
print(f"dtime_now:{dtime_now}")

condn_1day = dtime_now - col("dtime") < 86400
condn_7day = dtime_now - col("dtime") < 604800
condn_30day = dtime_now - col("dtime") < 2419200

df = df.withColumn("time_ago",
                   when(condn_1day , "1d") \
                   .when(condn_7day, "1w") \
                   .when(condn_30day, "4w") \
                   .otherwise("99w"))
print("converting Pyspark dataframe to Pandas")
pd_df = df.toPandas()
print('YES', pd_df[pd_df['recommend'] == 'YES']['time_ago'].value_counts())
print('MAYBE', pd_df[pd_df['recommend'] == 'MAYBE']['time_ago'].value_counts())

rng = np.random.default_rng()
samples = []
counts = pd_df['time_ago'].value_counts()
# sample 100 or less from 1 day ago
samples.append(pd_df.loc[pd_df['time_ago'] == '1d'].sample(n=min(counts.get('1d', 0),100), random_state=rng))
# sample 50 or less from 7 days ago
samples.append(pd_df.loc[pd_df['time_ago'] == '1w'].sample(n=min(counts.get('1w', 0),50), random_state=rng))
# sample 50 or less from 30 days ago
samples.append(pd_df.loc[pd_df['time_ago'] == '4w'].sample(n=min(counts.get('4w', 0),50), random_state=rng))
# sample 50 or less older than 30 days
samples.append(pd_df.loc[pd_df['time_ago'] == '99w'].sample(n=min(counts.get('99w', 0),50), random_state=rng))

sample_df = pd.concat(samples)
print('Sampled', sample_df['time_ago'].value_counts())

# bin engagement_score with auto-selection of bin boundaries
sample_df['popularity'], bin_cuts = \
                pd.qcut(
                    sample_df['engagement_score'], 
                    q = 3, 
                    labels = ['C', 'B', 'A'], 
                    retbins = True)
print('Popularity bins', bin_cuts)
print('Popularity counts', sample_df['popularity'].value_counts())

sample_df['weights'] = np.where(
                              sample_df['recommend'] == 'YES' ,
                              np.where(
                                  sample_df['popularity'] == 'A',
                                  .4, # YES-A
                                  np.where(sample_df['popularity'] == 'B',
                                           .3, # YES-B
                                           .1 # YES-C
                                           )
                                        ),
                              .2 # treat all MAYBEs equally
                              )

sample_df['weights'] = np.where(
                            sample_df['popularity'] == 'A',
                            np.where(
                              sample_df['recommend'] == 'YES' ,
                                .75, # A-YES
                                .1), # A-MAYBE
                            .15) # B 


print('Weights', sample_df['weights'].value_counts())

sample_df = sample_df.sample(n=100, weights='weights', random_state=rng)
print('Recommend counts', sample_df['recommend'].value_counts())
print('Popularity counts', sample_df['popularity'].value_counts())
print('Time ago counts', sample_df['time_ago'].value_counts())

sample_df = sample_df.sort_values(['weights', 'engagement_score'], 
                                  ascending=[False, False], ignore_index=True)
sample_df = sample_df[['post_id']]
sample_df['strategy_name'] = "ml-xgb-followship"
sample_df['v'] = sample_df.index
print(sample_df.head())
print(sample_df.tail())

SQLALCHEMY_SILENCE_UBER_WARNING=1
db = create_engine(args.pgsql_url);
print("Deleting older feed entries for strategy 'ml-xgb-followship'")
db.execute("DELETE FROM feed WHERE strategy_name = 'ml-xgb-followship'")

print("Inserting new feed entries for strategy 'ml-xgb-followship'")
sample_df.to_sql('feed', con=db, if_exists='append', index=False)
