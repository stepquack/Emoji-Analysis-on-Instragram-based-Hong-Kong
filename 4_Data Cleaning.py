# Databricks notebook source
post_df = spark.sql("select caption as text, url as postUrl from step_proj.ig_posts").toPandas()
post_df["is_post"] = True
comment_df = spark.sql("select `text`, postUrl from step_proj.ig_comment").toPandas()
comment_df["is_post"] = False

# COMMAND ----------

#
text_df = post_df.append(comment_df, ignore_index=True)
text_skdf = spark.createDataFrame(text_df)
text_skdf.write.mode('overwrite') \
         .saveAsTable("step_proj.raw_text")

# COMMAND ----------

pip install emoji

# COMMAND ----------

import emoji #https://pypi.org/project/emoji/
text_df['emojis'] = text_df['text'].apply(lambda row: ''.join(c for c in row if c in emoji.EMOJI_DATA))

# COMMAND ----------

emoji_df = text_df[text_df["emojis"]!=""]
emoji_df

# COMMAND ----------

pip install translate

# COMMAND ----------



# COMMAND ----------

#https://pypi.org/project/translate/
from translate import Translator
translator= Translator(to_lang="zh-TW")
def translate(c):
    print(c)
    try: 
        return translator.translate(c)
    except:
        return c
emoji_df["translated"] = emoji_df["text"].apply(translate)



# COMMAND ----------

import re

def filter_text(row):
    return ''.join(c for c in row if (c in emoji.EMOJI_DATA or re.match(r'[\u4E00-\u9FA5]+', c)))

emoji_df['text_cleaned'] = emoji_df['text'].apply(filter_text)

emoji_df


# COMMAND ----------

emoji = spark.createDataFrame()
log_df.write.mode('append') \
         .saveAsTable("step_proj.schedule_logs")
