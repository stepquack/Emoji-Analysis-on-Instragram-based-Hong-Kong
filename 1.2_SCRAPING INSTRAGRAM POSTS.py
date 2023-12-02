# PART 2: Obtain the Instagram Posts for analysis

# STEP 1: SET UP FOR APIFY
# Eg. TOKEN: apify_api_A2eOR8RxdmfQsNgH1RLmFiArVwdHSj35w39y

# INSTALL PACKAGE---------

pip install apify_client

# IMPORTS ----------

import pandas as pd
import numpy as np
from apify_client import ApifyClient

# EXTRACT DATA: Extract Instragram accounts from storage ----------

account_df = spark.sql("select * from step_proj.ig_account_rank").toPandas()
all_df = account_df[(account_df["Category"] == "All Categories") & (account_df["Country"] == "Hong Kong")].sort_values(by=["Followers_num"], ascending=False).head(10)
ig_accounts = all_df["Account_ID"].values
for i in account_df["Category"].unique():
    temp_df = account_df[(account_df["Category"] == i) & (account_df["Country"] == "Hong Kong")].sort_values(by=["Followers_num"], ascending=False).head(3)
    ig_accounts = np.append(ig_accounts, temp_df["Account_ID"].values)
ig_accounts = list(set(ig_accounts))

# OPTIONAL: PICKLE for temporary storage of targeted Instragram Accounts ----------

import pickle

# STORE
file = open('step_proj.ig_account.pkl', 'wb')
pickle.dump(ig_accounts, file)
file.close()

# LOAD
file = open('step_proj.ig_account.pkl', 'rb')
my_object = pickle.load(file)
file.close()

# RUNNING APIFY ----------

# Initialize the ApifyClient with your API token
client = ApifyClient("apify_api_A2eOR8RxdmfQsNgH1RLmFiArVwdHSj35w39y")

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

# STORE DATA: Store into Spark datafrome----------
post_df = pd.DataFrame(post_list)

post_skdf = spark.createDataFrame(post_df[["id", "type", "caption", "hashtags", "url", "commentsCount", "firstComment", "latestComments", "displayUrl", "likesCount", "timestamp", "ownerFullName", "ownerUsername", "ownerId"]])

# DATA TYPE CONVERTION ----------

from pyspark.sql.types import *
post_skdf = post_skdf.withColumn("timestamp",post_skdf.timestamp.cast(TimestampType()))


# STORE DATA ----------

post_skdf.write.mode('overwrite') \
         .saveAsTable("step_proj.ig_post_init")

# END ----------
