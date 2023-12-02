# DATA CLEANING & FEATURE ENGINEERING

# EXTRACT DATA: Posts and Comments

post_df = spark.sql("select caption as text, url as postUrl from step_proj.ig_posts").toPandas()
# Distinguish posts
post_df["is_post"] = True

comment_df = spark.sql("select `text`, postUrl from step_proj.ig_comment").toPandas()
# Distinguish comments
comment_df["is_post"] = False

# Combine Post caption and comments (Texts to analyze) into a single Dataframe ----------

text_df = post_df.append(comment_df, ignore_index=True)
text_df

# STORE DATA: Store text ----------

text_skdf = spark.createDataFrame(text_df)
text_skdf.write.mode('overwrite') \
         .saveAsTable("step_proj.raw_text")

# Extract Emojis from texts ----------
# Reference: https://pypi.org/project/emoji/

pip install emoji

# COMMAND ----------

import emoji

text_df['emojis'] = text_df['text'].apply(lambda row: ''.join(c for c in row if c in emoji.EMOJI_DATA))
emoji_df = text_df[text_df["emojis"]!=""]
emoji_df

# COMMAND ----------

emoji_df = text_df[text_df["emojis"]!=""]
emoji_df

# Extract Chinese characters and emoji only ----------
# Reference: https://pypi.org/project/translate/

pip install translate

# COMMAND ----------

from translate import Translator
translator= Translator(to_lang="zh-TW")
def translate(c):
    print(c)
    try:
        return translator.translate(c)
    except:
        return c
emoji_df["translated"] = emoji_df["text"].apply(translate)

# Extract emoji only ----------

import re

def filter_text(row):
    return ''.join(c for c in row if (c in emoji.EMOJI_DATA or re.match(r'[\u4E00-\u9FA5]+', c)))

emoji_df['text_cleaned'] = emoji_df['text'].apply(filter_text)

emoji_df


# STORE DATA ----------

emoji = spark.createDataFrame()
log_df.write.mode('append') \
         .saveAsTable("step_proj.schedule_logs")
