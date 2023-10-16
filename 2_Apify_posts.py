# Databricks notebook source
#emoproj01 (post): apify_api_A2eOR8RxdmfQsNgH1RLmFiArVwdHSj35w39y
#emoproj02: apify_api_VVOkgO0Raw7q3vFGr8b9KI8ktvXP6m1Q29yS
#emoproj03: apify_api_4hkcu22dSV3frvMvuF53uk6o3OGgU83LQe26
#emoproj04: apify_api_gVXHgQ6zxHw5Lgfy1ZCIVefTSEiyX92dqhFJ

# COMMAND ----------

pip install apify_client

# COMMAND ----------

import pandas as pd
import numpy as np
from apify_client import ApifyClient

# COMMAND ----------

#Get ig accounts
account_df = spark.sql("select * from step_proj.ig_account_rank").toPandas()
all_df = account_df[(account_df["Category"] == "All Categories") & (account_df["Country"] == "Hong Kong")].sort_values(by=["Followers_num"], ascending=False).head(10)
ig_accounts = all_df["Account_ID"].values
for i in account_df["Category"].unique():
    temp_df = account_df[(account_df["Category"] == i) & (account_df["Country"] == "Hong Kong")].sort_values(by=["Followers_num"], ascending=False).head(3)
    ig_accounts = np.append(ig_accounts, temp_df["Account_ID"].values)
ig_accounts = list(set(ig_accounts))

# COMMAND ----------

# Initialize the ApifyClient with your API token
client = ApifyClient("apify_api_A2eOR8RxdmfQsNgH1RLmFiArVwdHSj35w39y")

# COMMAND ----------

# Prepare the Actor input
run_input = {
    "username": ig_accounts,
    "resultsLimit": 50,
}

# Run the Actor and wait for it to finish
run = client.actor("apify/instagram-post-scraper").call(run_input=run_input)

# Fetch and print Actor results from the run's dataset (if there are any)
post_list = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    post_list.append(item)

#Into Dataframe
post_df = pd.DataFrame(post_list)

# COMMAND ----------

post_df.head(5)

# COMMAND ----------

post_skdf = spark.createDataFrame(post_df[["id", "type", "caption", "hashtags", "url", "commentsCount", "firstComment", "latestComments", "displayUrl", "likesCount", "timestamp", "ownerFullName", "ownerUsername", "ownerId"]])

# COMMAND ----------

post_skdf = spark.createDataFrame(post_df[["id", "type", "caption", "hashtags", "url", "commentsCount", "firstComment", "latestComments", "displayUrl", "likesCount", "timestamp", "ownerFullName", "ownerUsername", "ownerId"]])

from pyspark.sql.types import *
post_skdf = post_skdf.withColumn("timestamp",post_skdf.timestamp.cast(TimestampType()))


post_skdf.write.mode('overwrite') \
         .saveAsTable("step_proj.ig_post_init")

# COMMAND ----------

post_skdf.printSchema()
