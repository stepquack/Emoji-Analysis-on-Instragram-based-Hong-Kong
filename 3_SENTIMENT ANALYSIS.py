# SENTIMENT ANALYSIS

# EXTRACT DATA: Extract Instragram accounts from storage ----------

post_df = spark.sql("select caption as text, url as postUrl from step_proj.ig_posts").toPandas()
post_df["is_post"] = True
comment_df = spark.sql("select `text`, postUrl from step_proj.ig_comment").toPandas()
comment_df["is_post"] = False

# STORE DATA: Store text ----------

text_df = post_df.append(comment_df, ignore_index=True)
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

# Extract emoji ----------

import re

def filter_text(row):
    return ''.join(c for c in row if (c in emoji.EMOJI_DATA or re.match(r'[\u4E00-\u9FA5]+', c)))

emoji_df['text_cleaned'] = emoji_df['text'].apply(filter_text)

emoji_df

# SENTIMENT ANALYSIS with SnowNLP ---------
# Reference: https://github.com/isnowfy/snownlp

pip install snownlp

# COMMAND ----------

from snownlp import SnowNLP

# Filter out emojis, leaving text only ----------

def filter_textOnly(row):
    try:
        return ''.join(c for c in row if re.match(r'[\u4E00-\u9FA5]+', c))
    except:
        return None
emoji_df['text_only'] = text_df['emojis'] = text_df['text'].apply(filter_textOnly)
with_text_df = emoji_df[emoji_df["text_only"]!=""]
with_text_df

# Apply sentiment analysis ----------

def sentiment(t):
    s = SnowNLP(t)
    sen = SnowNLP(s.sentences[0])
    return sen.sentiments

with_text_df["sentiment_score"] = with_text_df["text_only"].apply(sentiment)
with_text_df

# Obtain Sentiment score
with_text_df[["text_cleaned","emojis", "sentiment_score"]].sort_values(by=['sentiment_score'], ascending=False)


#Remove duplicated emojis
with_text_df["emojis_unique"] = with_text_df["emojis"].apply(lambda x: list(set(x)))

#Explode dataframe
exploded_df = with_text_df.explode("emojis_unique")


# STORE DATA ----------

exploded_skdf = spark.createDataFrame(exploded_df)
exploded_skdf.write.mode('overwrite') \
         .saveAsTable("step_proj.sentiment_analysis")

# COMMAND ----------

# DISPLAY: Sentiment Analysis
spark.sql("select emojis_unique, avg(sentiment_score), count(emojis_unique) from step_proj.sentiment_analysis group by emojis_unique order by count(emojis_unique) desc, avg(sentiment_score) desc").toPandas()
