# Databricks notebook source
#zooapple: apify_api_IUDxKMl1wfXqdWvfSSY4iZXu87LtTO16X1tv
#twoplustep: apify_api_Qmp8pvmvg5E1QKLbLWg5IUwz90HQvO2xWrg4
#1. get ig_accounts
#2. get new post url of all ig accounts that posted after the datetime of the latest post (in ig_posts)
#3. retrieve the post url (in ig_posts) 
#4. 

# COMMAND ----------

pip install apify_client

# COMMAND ----------

target_accounts_df = spark.sql("select ownerUsername, max(timestamp) as Latest_date from step_proj.ig_posts group by ownerUsername order by Latest_date desc limit 30").toPandas()
target_accounts = list(target_accounts_df["ownerUsername"])

# COMMAND ----------

import pickle
file = open('sche_accounts', 'wb')
pickle.dump(target_accounts, file)
file.close()

# COMMAND ----------

target_accounts

# COMMAND ----------


from apify_client import ApifyClient
import pandas as pd
import numpy as np


# COMMAND ----------

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

#Into Dataframe
sche_post_df = pd.DataFrame(sche_post_list)

sche_post_df

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
runtime = current_timestamp()


# COMMAND ----------

#Into spark dataframe


sche_post_skdf = spark.createDataFrame(sche_post_df[["id", "type", "caption", "hashtags", "url", "commentsCount", "firstComment", "latestComments", "displayUrl", "likesCount", "timestamp", "ownerFullName", "ownerUsername", "ownerId"]])

from pyspark.sql.types import *

sche_post_skdf = sche_post_skdf.withColumn("timestamp",sche_post_skdf.timestamp.cast(TimestampType()))
sche_post_skdf = sche_post_skdf.withColumn("scheduledDatetime", runtime)


sche_post_skdf.write.mode('append') \
         .saveAsTable("step_proj.scheduled_ig_posts")

# COMMAND ----------

#Filter new post 
exist_post_df = spark.sql("select distinct(url) from step_proj.ig_post_init").toPandas()
new_post_df = sche_post_df[~sche_post_df["url"].isin(exist_post_df["url"])]
new_post_skdf = spark.createDataFrame(new_post_df[["id", "type", "caption", "hashtags", "url", "commentsCount", "firstComment", "latestComments", "displayUrl", "likesCount", "timestamp", "ownerFullName", "ownerUsername", "ownerId"]])
from pyspark.sql.types import *
new_post_skdf = new_post_skdf.withColumn("timestamp",new_post_skdf.timestamp.cast(TimestampType()))
new_post_skdf.write.mode('append') \
         .saveAsTable("step_proj.ig_posts")


# COMMAND ----------

final_post_df = spark.sql("select * from step_proj.ig_posts")


log = [("post", len(exist_post_df), new_post_skdf.count(), final_post_df.count())]

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
