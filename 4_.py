# Databricks notebook source
spark.sql("select * from step_proj.ig_posts").count()

# COMMAND ----------

sche_post_df = spark.sql("select * from step_proj.scheduled_ig_posts").toPandas()

# COMMAND ----------

sche_post_df.count()

# COMMAND ----------

exist_post_df = spark.sql("select distinct(url) from step_proj.ig_posts").toPandas()
new_post_df = sche_post_df[~sche_post_df["url"].isin(exist_post_df["url"])]

# COMMAND ----------

new_post_df

# COMMAND ----------

new_post_skdf = spark.createDataFrame(new_post_df[["id", "type", "caption", "hashtags", "url", "commentsCount", "firstComment", "latestComments", "displayUrl", "likesCount", "timestamp", "ownerFullName", "ownerUsername", "ownerId"]])
from pyspark.sql.types import *
new_post_skdf = new_post_skdf.withColumn("timestamp",new_post_skdf.timestamp.cast(TimestampType()))
new_post_skdf.write.mode('append') \
         .saveAsTable("step_proj.ig_posts")

# COMMAND ----------



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
