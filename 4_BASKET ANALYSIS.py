# BASKET ANALYSIS

# EXTRACT DATA: Extract emoji and texts ----------
emoji_uni_df = spark.sql("select * from step_proj.sentiment_analysis").toPandas()
text_df = spark.sql("select * from step_proj.raw_text").toPandas()

# Extract Emojis from texts ----------
# Reference: https://pypi.org/project/emoji/

pip install emoji

# COMMAND ----------

import emoji

text_df['emojis'] = text_df['text'].apply(lambda row: ''.join(c for c in row if c in emoji.EMOJI_DATA))
emoji_df = text_df[text_df["emojis"]!=""]
emoji_df

# COMMAND ----------

#Extract significant emojis
emo_count = emoji_uni_df.groupby(["emojis_unique"],as_index=True)["emojis_unique"].count().sort_values(ascending =False).to_frame().rename(columns={"emojis_unique": "Count"})
sign_emos = emo_count[emo_count["Count"]>=100].drop(index=('🏻'))

# BASKET ANALYSIS with itertools ---------

from itertools import permutations
rules = list(permutations(sign_emos.index, 2))
print('# of rules:',len(rules))
print(rules[:5])

# COMMAND ----------

emojis = emoji_uni_df[emoji_uni_df["emojis_unique"].isin(sign_emos.index)]
emojis


# One Hot Encode with TransactionEncoder ---------

pip install mlxtend

# Import the transaction encoder function from mlxtend
from mlxtend.preprocessing import TransactionEncoder
import pandas as pd
import emoji

# Instantiate transaction encoder and identify unique items
encoder = TransactionEncoder().fit(emoji_df["emojis"])

# One-hot encode transactions
onehot = encoder.transform(emoji_df["emojis"])

# Convert one-hot encoded data to DataFrame
onehot = pd.DataFrame(onehot, columns = encoder.columns_)

# Print the one-hot encoded transaction dataset
onehot

# STORE DATA ----------

onehot_skdf = spark.createDataFrame(onehot)
onehot_skdf.write.mode('overwrite') \
         .saveAsTable("step_proj.onehot")



# Display Support score ---------

support = onehot.mean()
support = pd.DataFrame(support, columns=['support']).sort_values('support',ascending=False)
support


# Basket Analysis with apriori ---------

# Import the association rules function
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

# Compute frequent itemsets using the Apriori algorithm
frequent_itemsets = apriori(onehot, min_support = 0.001,
                            max_len = 2, use_colnames = True)

# Compute all association rules for frequent_itemsets
rules = association_rules(frequent_itemsets,
                            metric = "lift")

# Print association rules
rules.head()

# COMMAND ----------

rules["antecedents"] = rules["antecedents"].apply(lambda x: ', '.join(list(x))).astype("unicode")
rules["consequents"] = rules["consequents"].apply(lambda x: ', '.join(list(x))).astype("unicode")

# DATA TYPE CONVERSIONS ----------

from pyspark.sql.types import *
Schema = StructType([ StructField("antecedents", StringType(), True),
                      StructField("consequents", StringType(), True),
                      StructField("antecedent_support", DoubleType(), True),
                      StructField("consequent_support", DoubleType(), True),
                      StructField("support", DoubleType(), True),
                      StructField("confidence", DoubleType(), True),
                      StructField("lift", DoubleType(), True),
                      StructField("leverage", DoubleType(), True),
                      StructField("conviction", DoubleType(), True),
                      StructField("zhangs_metric", DoubleType(), True)
                    ])

# STORE DATA ----------

basket_skdf = spark.createDataFrame(data = rules, schema=Schema)
basket_skdf.write.mode('overwrite') \
         .saveAsTable("step_proj.basket_analysis")
