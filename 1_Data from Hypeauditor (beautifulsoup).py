# Databricks notebook source
#spark.sql("DROP TABLE IF EXISTS step_proj.ig_account_ranks")

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import pandas as pd

# COMMAND ----------

header = {"user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
url = "https://hypeauditor.com/top-instagram-all-hong-kong/"
response = requests.get(url, headers=header)
http_string = response.text
html = BeautifulSoup(http_string, 'html.parser')

cat_ul = html.select_one('div.menu[data-v-4b597f91]')
cat_lis = cat_ul.select('.menu__item[data-v-4b597f91]')

categories = []
categories_path = []
categories_link = []
for li in cat_lis:
    name = li.select_one('span').text
    categories.append(name)
    if name == "All Categories":
        path_name = "all"
    else:
        path_name = name.lower().replace(" ", "-").replace("&", "-").replace("/", "-").replace("---", "-")
    categories_path.append(path_name)
    categories_link.append("https://hypeauditor.com/top-instagram-" + path_name + "-hong-kong/")

#Into Dataframe
cat_data = {"Category": categories, "Path": categories_path, "Link":categories_link }
category_df = pd.DataFrame(cat_data)
cat_df = spark.createDataFrame(category_df)
cat_df.write.mode('overwrite') \
         .saveAsTable("step_proj.hypeauditor_ig_category")

# COMMAND ----------

category_df = spark.sql("select * from step_proj.hypeauditor_ig_category").toPandas()
#category_df.createOrReplaceTempView("category_df_t1")

# COMMAND ----------

category_ = spark.sql("select Category from category_df_t1").toPandas()
category_link = spark.sql("select Link from category_df_t1").toPandas()

# COMMAND ----------

rank_category = []
rank = []
account_id = []
account_name = []
account_link = []
followers = []
country = []

header = {"user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}

for i, ilink in enumerate(category_df["Link"]):
    url = ilink
    
    response = requests.get(url, headers=header)
    http_string = response.text
    html = BeautifulSoup(http_string, 'html.parser')
    rank_ul = html .select_one('.ranking-card')
    rank_lis = rank_ul.select('.row')

    for li in rank_lis:
        rank_category.append(category_df["Category"].iloc[i])
        print(rank_category)
        try:
            d1 = li.select_one('.rank > span').text
            rank.append(d1)
        except:
            rank.append(None)
        print(rank)
        try:
            d2 = li.select_one('.contributor__name-content').text
            account_id.append(d2)
        except:
            account_id.append(None)
        print(account_id)
        try:
            d3 = li.select_one('.contributor__title').text
            account_name.append(d3)
        except:
            account_name.append(None)
        print(account_name)
        try:
            d5 = li.select_one('.subscribers').text
            followers.append(d5)
        except:
            followers.append(None)
        print(followers)
        try:
            d6 = li.select_one('.audience').text
            country.append(d6)
        except:
            country.append(None)
        print(country)

#Into Dataframe
rank_data = {"Category": rank_category, "Rank": rank, "Influencer":account_name, "Account_ID": account_id, "Followers": followers, "Country": country}
rank_pddf = pd.DataFrame(rank_data)

    

#rank_data = list(zip(rank_category, rank, account_name, account_id, followers, country, account_link))
#rank_df = spark.createDataFrame(rank_data, ["Category", "Rank", "Influencer", "Account_ID", "Followers", "Country", "Link"])

# COMMAND ----------

rank_pddf

# COMMAND ----------

#drop null: row 0
#rank_pddf.drop([0], inplace=True)

#convert K and M
def convert(follower):
    if follower is None:
        pass
    elif follower[-1] == "K":
        return float(follower[:-1])*1000
    elif follower[-1] == "M":
        return float(follower[:-1])*10000000
    else:
        return float(follower)
rank_pddf["Followers_num"] = rank_pddf["Followers"].apply(convert)


# COMMAND ----------

rank_skdf = spark.createDataFrame(rank_pddf)

from pyspark.sql.types import IntegerType
rank_skdf = rank_skdf.withColumn("Rank",rank_skdf.Rank.cast(IntegerType()))

rank_skdf.write.mode('overwrite') \
         .saveAsTable("step_proj.ig_account_rank")

# COMMAND ----------


