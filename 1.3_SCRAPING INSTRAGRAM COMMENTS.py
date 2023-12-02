# PART 3: Obtain the Instagram Comments for analysis

# STEP 1: SET UP FOR APIFY
# Eg. TOKEN: apify_api_A2eOR8RxdmfQsNgH1RLmFiArVwdHSj35w39y

# INSTALL PACKAGE---------

pip install apify_client

# IMPORTS ----------

import pandas as pd
import numpy as np
from apify_client import ApifyClient

# STEP 1: EXTRACT DATA: Extract Instragram posts from storage ----------

post_df = spark.sql("select * from step_proj.ig_post_init").toPandas()
temp_list=[]
for i in post_df["ownerId"].unique():
    temp_df = post_df[(post_df["ownerId"] == i)].sort_values(by=["commentsCount"], ascending=False).head(15)
    temp_list = np.append(temp_list, temp_df["url"].values)
ig_posts = list(set(temp_list))

# RUNNING APIFY ----------

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

# STORE DATA: Store into Spark datafrome----------
comment_df = pd.DataFrame(comment_list)

comment_skdf = spark.createDataFrame(comment_df[["postUrl", "id", "text", "ownerUsername", "timestamp", "likesCount"]])

# DATA TYPE CONVERTION ----------

from pyspark.sql.types import *
comment_skdf = comment_skdf.withColumn("timestamp",comment_skdf.timestamp.cast(TimestampType()))

# STORE DATA ----------

comment_skdf.write.mode('overwrite') \
         .saveAsTable("step_proj.ig_comment_init")

# END ----------
