# Databricks notebook source
#emoproj01 (post): apify_api_A2eOR8RxdmfQsNgH1RLmFiArVwdHSj35w39y
#emoproj02: apify_api_VVOkgO0Raw7q3vFGr8b9KI8ktvXP6m1Q29yS
#emoproj03: apify_api_4hkcu22dSV3frvMvuF53uk6o3OGgU83LQe26
#emoproj04: apify_api_gVXHgQ6zxHw5Lgfy1ZCIVefTSEiyX92dqhFJ
#pickle: apify_api_OF8EEvKPUPcHJvCQ3q4ZOyOwpS7Q1L41A6hs

# COMMAND ----------

pip install apify_client

# COMMAND ----------

import pandas as pd
import numpy as np
from apify_client import ApifyClient

# COMMAND ----------

#Get ig posts
post_df = spark.sql("select * from step_proj.ig_post_init").toPandas()
temp_list=[]
for i in post_df["ownerId"].unique():
    temp_df = post_df[(post_df["ownerId"] == i)].sort_values(by=["commentsCount"], ascending=False).head(15)
    temp_list = np.append(temp_list, temp_df["url"].values)
ig_posts = list(set(temp_list))

len(ig_posts)

# COMMAND ----------

# Initialize the ApifyClient with your API token
client = ApifyClient("apify_api_VVOkgO0Raw7q3vFGr8b9KI8ktvXP6m1Q29yS")

# Prepare the Actor input
run_input = {
    "directUrls": ig_posts,
    "resultsLimit": 100,
}

# Run the Actor and wait for it to finish
run = client.actor("apify/instagram-comment-scraper").call(run_input=run_input)

# Fetch and print Actor results from the run's dataset (if there are any)
comment_list = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    comment_list.append(item)

comment_df = pd.DataFrame(comment_list)

# COMMAND ----------

comment_df

# COMMAND ----------

comment_skdf = spark.createDataFrame(comment_df[["postUrl", "id", "text", "ownerUsername", "timestamp", "likesCount"]])

# COMMAND ----------

from pyspark.sql.types import *
comment_skdf = comment_skdf.withColumn("timestamp",comment_skdf.timestamp.cast(TimestampType()))
comment_skdf.write.mode('overwrite') \
         .saveAsTable("step_proj.ig_comment_init")

# COMMAND ----------

#Additional data

temp_list_2=[]
for i in post_df["ownerId"].unique():
    temp_df_2 = post_df[(post_df["ownerId"] == i)].sort_values(by=["commentsCount"], ascending=False)
    temp_df_rest = temp_df_2[~post_df["url"].isin(ig_posts)].head(20)
    temp_list_2 = np.append(temp_list_2, temp_df_rest["url"].values)
ig_posts_2 = list(set(temp_list_2))

ig_posts_2 in ig_posts
len(ig_posts_2)

# COMMAND ----------

# Initialize the ApifyClient with your API token
client = ApifyClient("apify_api_UdCbxhJU0kNoEVDJvvHPUuiZfyZo764vaPUG")

# Prepare the Actor input
run_input = {
    "directUrls": ig_posts_2,
    "resultsLimit": 300,
}

# Run the Actor and wait for it to finish
run = client.actor("apify/instagram-comment-scraper").call(run_input=run_input)

# Fetch and print Actor results from the run's dataset (if there are any)
comment_list_2 = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    comment_list_2.append(item)

comment_df_2 = pd.DataFrame(comment_list_2)

# COMMAND ----------

comment_skdf_2 = spark.createDataFrame(comment_df_2[["postUrl", "id", "text", "ownerUsername", "timestamp", "likesCount"]])

# COMMAND ----------

from pyspark.sql.types import *
comment_skdf_2 = comment_skdf_2.withColumn("timestamp",comment_skdf_2.timestamp.cast(TimestampType()))

# COMMAND ----------

comment_init = spark.sql("select * from step_proj.ig_comment_init")

# COMMAND ----------

comment_init.write.mode('overwrite') \
         .saveAsTable("step_proj.ig_comment")

# COMMAND ----------

comment_skdf_2.write.mode('append') \
         .saveAsTable("step_proj.ig_comment")

# COMMAND ----------

spark.sql("select * from step_proj.ig_comment").count()
