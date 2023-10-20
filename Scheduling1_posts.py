# Databricks notebook source
#zooapple: apify_api_IUDxKMl1wfXqdWvfSSY4iZXu87LtTO16X1tv
#twoplustep: apify_api_Qmp8pvmvg5E1QKLbLWg5IUwz90HQvO2xWrg4

# COMMAND ----------

pip install apify_client

# COMMAND ----------

# Determine which 30 ig accounts to scrape
target_accounts_df = spark.sql("select ownerUsername, max(timestamp) as Latest_date from step_proj.ig_posts group by ownerUsername order by Latest_date desc limit 30").toPandas()
target_accounts = list(target_accounts_df["ownerUsername"])

# COMMAND ----------

# Store the target accounts in pickle
import pickle
file = open('sche_accounts.pkl', 'wb')
pickle.dump(target_accounts, file)
file.close()
target_accounts

# COMMAND ----------

#Imports
from apify_client import ApifyClient
import pandas as pd
import numpy as np


# COMMAND ----------

#Scrape data with API

# Initialize the ApifyClient with your API token
client = ApifyClient("apify_api_Qmp8pvmvg5E1QKLbLWg5IUwz90HQvO2xWrg4")

# Prepare the Actor input
run_input = {
    "username": target_accounts,
    "resultsLimit": 3,
}

# Run the Actor and wait for it to finish
run = client.actor("apify/instagram-post-scraper").call(run_input=run_input)

# Fetch and print Actor results from the run's dataset (if there are any)
sche_post_list = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    sche_post_list.append(item)

#Store scraped data nto Pandas Dataframe
sche_post_df = pd.DataFrame(sche_post_list)

sche_post_df

# COMMAND ----------

# Define schedule time
from pyspark.sql.functions import current_timestamp
runtime = current_timestamp()


# COMMAND ----------

# Extract key data and convert Pandas Dataframe into Spark dataframe
sche_post_skdf = spark.createDataFrame(sche_post_df[["id", "type", "caption", "hashtags", "url", "commentsCount", "firstComment", "latestComments", "displayUrl", "likesCount", "timestamp", "ownerFullName", "ownerUsername", "ownerId"]])

# Change Datatype
from pyspark.sql.types import *
sche_post_skdf = sche_post_skdf.withColumn("timestamp",sche_post_skdf.timestamp.cast(TimestampType()))
sche_post_skdf = sche_post_skdf.withColumn("scheduledDatetime", runtime)

# Store scraped data to Hive
sche_post_skdf.write.mode('append') \
         .saveAsTable("step_proj.scheduled_ig_posts")

# COMMAND ----------

#Filter new posts from the scraped data 
post_skdf = spark.sql("select * from step_proj.ig_posts")
max_ts = post_skdf.groupBy().agg({"timestamp": "max"}).collect()[0][0]
new_post_df = sche_post_skdf.filter(sche_post_skdf.timestamp > max_ts)

# COMMAND ----------

# Store the new post urls in pickle
import pickle
file = open('sche_posts.pkl', 'wb')
new_url_list = [str(row.url) for row in new_post_df.select("url").collect()]
pickle.dump(new_url_list, file)
file.close()

#Store into Hive
from pyspark.sql.types import *
new_post_df.drop('scheduledDatetime').write.mode('append') \
         .saveAsTable("step_proj.ig_posts")


# COMMAND ----------

# Store schedule log
final_post_df = spark.sql("select * from step_proj.ig_posts")

log = [("post", post_skdf.count(), new_post_df.count(), final_post_df.count())]

schema = StructType([ \
    StructField("type",StringType(),True), \
    StructField("beforeCount",IntegerType(),True), \
    StructField("newRows",IntegerType(),True), \
    StructField("afterCount", IntegerType(), True)\
 ])

log_df = spark.createDataFrame(data=log, schema=schema)
log_df = log_df.withColumn("scheduleTime", runtime)
log_df.write.mode('append') \
         .saveAsTable("step_proj.schedule_logs")
