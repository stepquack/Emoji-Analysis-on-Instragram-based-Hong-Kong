# Databricks notebook source
#Post

# COMMAND ----------

exist_post_df = spark.sql("select distinct(url) from step_proj.ig_posts").toPandas()

# COMMAND ----------

import pickle
file = open('sche_posts.pkl', 'rb')
my_object = pickle.load(file)
file.close()
my_object

# COMMAND ----------

df= spark.sql("select * from step_proj.scheduled_ig_posts").toPandas()
new_post_df = df[df["url"].isin(my_object)]

# COMMAND ----------

#Convert to spark df, change datatype and store into Hive
from pyspark.sql.types import *
schema = spark.sql("select * from step_proj.ig_posts").schema
new_post_skdf = spark.createDataFrame(data=new_post_df[["id", "type", "caption", "hashtags", "url", "commentsCount", "firstComment", "latestComments", "displayUrl", "likesCount", "timestamp", "ownerFullName", "ownerUsername", "ownerId"]], schema=schema)

#new_post_skdf = new_post_skdf.withColumn("timestamp",new_post_skdf.timestamp.cast(TimestampType()))
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
