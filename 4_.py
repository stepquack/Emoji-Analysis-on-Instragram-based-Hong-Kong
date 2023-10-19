# Databricks notebook source
ig_posts_df = spark.sql("select * from step_proj.ig_post_init")

# COMMAND ----------

sche_post_df = spark.sql("select * from step_proj.scheduled_ig_posts").toPandas()

# COMMAND ----------

ig_posts_df.count()

# COMMAND ----------

ig_posts_df.write.mode('overwrite') \
         .saveAsTable("step_proj.ig_posts")

# COMMAND ----------

exist_post_df = spark.sql("select distinct(url) from step_proj.ig_posts").toPandas()
new_post_df = sche_post_df[~sche_post_df["url"].isin(exist_post_df["url"])]

# COMMAND ----------

new_post_df.printSchema()

# COMMAND ----------

#Error, try filter by spark
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

# COMMAND ----------



new_post_skdf.write.mode('append') \
         .saveAsTable("step_proj.ig_posts")

# COMMAND ----------

target_accounts_df = spark.sql("select ownerUsername, max(timestamp) as Latest_date from step_proj.ig_posts group by ownerUsername order by Latest_date desc limit 30").toPandas()
target_accounts = list(target_accounts["ownerUsername"])

# COMMAND ----------

import pickle
file = open('sche_accounts', 'wb')
pickle.dump(target_accounts, file)
file.close()

# COMMAND ----------

spark.sql("select url from step_proj.ig_posts").count()

# COMMAND ----------

clear = spark.sql("select url from step_proj.ig_posts").dropDuplicates()
clear.count()
clear.write.mode('overwrite') \
         .saveAsTable("step_proj.ig_posts")

# COMMAND ----------

spark.sql("select id from step_proj.ig_comment").count()

# COMMAND ----------

clear = spark.sql("select id from step_proj.ig_comment").dropDuplicates()
clear.count()

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

spark.sql("select * from step_proj.ig_posts").schema
schema = StructType([
           StructField("id", StringType(), True),
           StructField("type", StringType(), True),
           StructField("caption", StringType(), True),
           StructField("hashtags", ArrayType(StringType()), True),
           StructField("url", StringType(), True),
           StructField("commentsCount", LongType(), True),
           StructField("firstComment", StringType(), True),
           StructField("latestComments", ArrayType(StringType()), True),
           StructField("displayUrl", StringType(), True),
           StructField("likesCount", LongType(), True),
           StructField("timestamp", TimestampType(), True),
           StructField("ownerFullName", StringType(), True),
           StructField("ownerUsername", StringType(), True),
           StructField("ownerId", StringType(), True)
         ])
