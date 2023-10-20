# Databricks notebook source
pip install emoji

# COMMAND ----------

pip install imojify

# COMMAND ----------

import emoji
text_df = spark.sql("select * from step_proj.raw_text").toPandas()
text_df['emojis'] = text_df['text'].apply(lambda row: ''.join(c for c in row if c in emoji.EMOJI_DATA))
text_df

# COMMAND ----------

sentiment_df = spark.sql("select emojis_unique as Emoji, avg(sentiment_score) as `Sentiment Score`, count(emojis_unique) as `Count` from step_proj.sentiment_analysis group by emojis_unique order by count(emojis_unique) desc, avg(sentiment_score) desc").toPandas()

# COMMAND ----------


sentiment_df["Demojize"]= sentiment_df["Emoji"].apply(lambda x: emoji.demojize(x))
emo_df = sentiment_df[:30].set_index(["Emoji"])[["Demojize","Count"]]
emo_df["%"] = emo_df["Count"].apply(lambda x: (x/emo_df["Count"].sum())*100)

emo_df.style.set_caption("Top 30 Emojis used on Instagram")

# COMMAND ----------

from imojify import imojify
from matplotlib import pyplot as plt 
from matplotlib.offsetbox import OffsetImage,AnnotationBbox
def offset_image(cords, emoji, ax):
    img = plt.imread(imojify.get_img_path(emoji))
    im = OffsetImage(img, zoom=0.05)
    im.image.axes = ax
    ab = AnnotationBbox(im, (cords[0], cords[1]),  frameon=False, pad=0)
    ax.add_artist(ab)

emjis = list(sentiment_df["Emoji"][:30])
values =list(sentiment_df["Count"][:30])

fig, ax = plt.subplots(figsize=(12,8))
ax.bar(range(len(emjis)), values, width=0.5,align="center")
ax.set_xticks(range(len(emjis)))
ax.set_xticklabels([])

ax.tick_params(axis='x', which='major', pad=26)
ax.set_ylim((0, ax.get_ylim()[1]+10))

for i, e in enumerate(emjis):
    offset_image([i,values[i]+5], e, ax)
    


# COMMAND ----------

from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
vectorizer = CountVectorizer(analyzer='char')
X = vectorizer.fit_transform(list(text_df[text_df['emojis']!=""]['emojis']))
countvectorizer_df = pd.DataFrame(X.toarray(), columns=vectorizer.get_feature_names_out())
countvectorizer_df


# COMMAND ----------

df1 = countvectorizer_df.max(axis=0).sort_values(ascending=False)[:30].to_frame().reset_index().rename(columns={"index": "Emoji", 0:"Max"})
df1["Demojize"]= df1["Emoji"].apply(lambda x: emoji.demojize(x))
df2 = df1[["Emoji", "Demojize", "Max"]]
df2
#countvectorizer_df["😡"].idxmax()
#text_df.loc[32038]["emojis"]
#text_df[text_df['emojis']!=""].iloc[32038]["text"]

# COMMAND ----------

from imojify import imojify
from matplotlib import pyplot as plt 
from matplotlib.offsetbox import OffsetImage,AnnotationBbox
def offset_image(cords, emoji, ax):
    img = plt.imread(imojify.get_img_path(emoji))
    im = OffsetImage(img, zoom=0.05)
    im.image.axes = ax
    ab = AnnotationBbox(im, (cords[0], cords[1]),  frameon=False, pad=0)
    ax.add_artist(ab)

emjis = list(df2["Emoji"][:30])
values =list(df2["Max"][:30])

fig, ax = plt.subplots(figsize=(12,8))
ax.bar(range(len(emjis)), values, width=0.5,align="center")
ax.set_xticks(range(len(emjis)))
ax.set_xticklabels([])

ax.tick_params(axis='x', which='major', pad=26)
ax.set_ylim((0, ax.get_ylim()[1]+10))

for i, e in enumerate(emjis):
    offset_image([i,values[i]+5], e, ax)

# COMMAND ----------

countvectorizer_df_2 = countvectorizer_df[["Emoji", "Demojize","Top 20 Emojis spammed on Instagram" ]]

# COMMAND ----------

from imojify import imojify
from matplotlib import pyplot as plt 
from matplotlib.offsetbox import OffsetImage,AnnotationBbox
def offset_image(cords, emoji, ax):
    img = plt.imread(imojify.get_img_path(emoji))
    im = OffsetImage(img, zoom=0.05)
    im.image.axes = ax
    ab = AnnotationBbox(im, (cords[0], cords[1]),  frameon=False, pad=0)
    ax.add_artist(ab)

emjis = list(sentiment_df["Emoji"][:30])
values =list(sentiment_df["Count"][:30])

fig, ax = plt.subplots(figsize=(12,8))
ax.bar(range(len(emjis)), values, width=0.5,align="center")
ax.set_xticks(range(len(emjis)))
ax.set_xticklabels([])

ax.tick_params(axis='x', which='major', pad=26)
ax.set_ylim((0, ax.get_ylim()[1]+10))

for i, e in enumerate(emjis):
    offset_image([i,values[i]+5], e, ax)
