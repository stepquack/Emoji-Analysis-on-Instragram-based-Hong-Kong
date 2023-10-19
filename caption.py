# Databricks notebook source
post_df = spark.sql("select caption from step_proj.ig_posts")
comment_df = spark.sql("select `text` from step_proj.ig_comment")

# COMMAND ----------

caption_df  = post_df

# COMMAND ----------

#https://pypi.org/project/emoji/
from emoji import UNICODE_EMOJI

def is_emoji(s):
    return s in UNICODE_EMOJI
