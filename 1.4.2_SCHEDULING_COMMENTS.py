# STEP 1: SET UP FOR APIFY
# Eg. TOKEN: apify_api_A2eOR8RxdmfQsNgH1RLmFiArVwdHSj35w39y

# INSTALL PACKAGE---------

pip install apify_client


# IMPORTS ----------
from apify_client import ApifyClient
import pandas as pd
import numpy as np

# EXTRACT DATA: Extract Instragram posts from previous scheduled task  ----------
import pickle
file = open('sche_posts.pkl', 'rb')
new_posts = pickle.load(file)
file.close()
new_posts

# If error occurred in the previous scheduled task, stop the current task ----------

if len(new_posts) == 0:
    dbutils.notebook.exit("Aborting as ondition not met. Further tasks will be skipped")
else:
    pass

# COMMAND ----------

exist_cm_count = spark.sql("select * from step_proj.ig_comment").count()
exist_cm_count

# RUNNING APIFY ----------

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


# If error occurred in the API scraping task, stop the current task ----------

if len(comment_df) == 0:
    dbutils.notebook.exit("Aborting as ondition not met. Further tasks will be skipped")
else:
    pass


# Define schedule time
from pyspark.sql.functions import current_timestamp
runtime = current_timestamp()


# Extract key data and convert Pandas Dataframe into Spark dataframe
comment_skdf = spark.createDataFrame(comment_df[["postUrl", "id", "text", "ownerUsername", "timestamp", "likesCount"]])
from pyspark.sql.types import *
comment_skdf = comment_skdf.withColumn("timestamp",comment_skdf.timestamp.cast(TimestampType()))
comment_skdf.write.mode('append') \
         .saveAsTable("step_proj.scheduled_ig_comment")

# Store scraped data to Hive
comment_skdf.write.mode('append') \
         .saveAsTable("step_proj.ig_comment")


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


# END ----------
