# Databricks notebook source
#zooapple: apify_api_IUDxKMl1wfXqdWvfSSY4iZXu87LtTO16X1tv
#twoplustep: apify_api_Qmp8pvmvg5E1QKLbLWg5IUwz90HQvO2xWrg4

# COMMAND ----------

pip install apify_client

# COMMAND ----------

#Imports
from apify_client import ApifyClient
import pandas as pd
import numpy as np

# COMMAND ----------

#Get new posts
import pickle
file = open('sche_posts.pkl', 'rb')
new_posts = pickle.load(file)
file.close()

# COMMAND ----------

exist_cm_count = spark.sql("select * from step_proj.ig_comment").count()
exist_cm_count

# COMMAND ----------

#Scrape data with API

# Initialize the ApifyClient with your API token
client = ApifyClient("apify_api_Qmp8pvmvg5E1QKLbLWg5IUwz90HQvO2xWrg4")

# Prepare the Actor input
run_input = {
    "directUrls": new_posts,
    "resultsLimit": 30,
}

# Run the Actor and wait for it to finish
run = client.actor("apify/instagram-comment-scraper").call(run_input=run_input)

# Fetch and print Actor results from the run's dataset (if there are any)
comment_list = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    comment_list.append(item)

comment_df = pd.DataFrame(comment_list)

# COMMAND ----------

# Define schedule time
from pyspark.sql.functions import current_timestamp
runtime = current_timestamp()

# COMMAND ----------

comment_skdf = spark.createDataFrame(comment_df[["postUrl", "id", "text", "ownerUsername", "timestamp", "likesCount"]])
from pyspark.sql.types import *
comment_skdf = comment_skdf.withColumn("timestamp",comment_skdf.timestamp.cast(TimestampType()))
comment_skdf.write.mode('append') \
         .saveAsTable("step_proj.scheduled_ig_comment")

# COMMAND ----------

comment_skdf.write.mode('append') \
         .saveAsTable("step_proj.ig_comment")

# COMMAND ----------

# Store schedule log
final_cm_count = spark.sql("select * from step_proj.ig_comment").count()

log = [("comment", exist_cm_count, comment_skdf.count(), final_cm_count)]

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
