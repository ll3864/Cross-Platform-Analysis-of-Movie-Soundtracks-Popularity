#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, year, explode, lower, size, array_contains
from pymongo import MongoClient
import findspark
import pandas as pd


# In[2]:


import sys
print(sys.executable)


# In[3]:


# Connect to MongoDB
client = MongoClient("localhost", 27017)
db = client["project"]

genre_col = db["Genre"]
movies_col = db["Movies"]
movies_albums_col = db["Movie_Albums"]
album_info_col = db["Albums_Info"]


# In[4]:


import os

# Set up full environment
os.environ["SPARK_HOME"] = "C:\\spark"
os.environ["HADOOP_HOME"] = "C:\\spark"
os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-1.8"  # <-- update with your real JDK folder
os.environ["PATH"] = f"{os.environ['SPARK_HOME']}\\bin;{os.environ['JAVA_HOME']}\\bin;" + os.environ["PATH"]

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\linl8\\AppData\\Local\\Programs\\Python\\Python313\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\linl8\\AppData\\Local\\Programs\\Python\\Python313\\python.exe"



# In[5]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MongoSparkConnector") \
    .master("local[*]") \
    .config("spark.jars", ",".join([
        "file:///C:/spark/jars/mongo-spark-connector_2.12-3.0.1.jar",
        "file:///C:/spark/jars/mongodb-driver-sync-4.2.3.jar",
        "file:///C:/spark/jars/mongodb-driver-core-4.2.3.jar",
        "file:///C:/spark/jars/bson-4.2.3.jar"
    ])) \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/project") \
    .getOrCreate()


# In[6]:


spark


# In[7]:


spark._jvm.org.mongodb.spark.MongoSpark


# In[8]:


Genre_df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017") \
    .option("database", "project") \
    .option("collection", "Genre") \
    .load()

Movies_df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017") \
    .option("database", "project") \
    .option("collection", "Movies") \
    .load()

Movie_Albums_df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017") \
    .option("database", "project") \
    .option("collection", "Movie_Albums") \
    .load()

Albums_Info_df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017") \
    .option("database", "project") \
    .option("collection", "Albums_Info") \
    .load()



# In[9]:


# Get Genre ID by name
def get_genre_id(genre_name: str):
    genres_df = Genre_df.select(explode("genres").alias("genre"))
    result = genres_df.filter(lower(col("genre.name")) == genre_name.lower()).select("genre.id").collect()
    return result[0]["id"] if result else None

def get_all_genre_names():
    genres_df = Genre_df.select(explode("genres").alias("genre"))
    names = genres_df.select("genre.name").distinct().orderBy("genre.name").collect()
    return [row["name"] for row in names]


# In[10]:


# Average Movie Statistics in Genre
def average_stats(genre_id):
    filtered = Movies_df.filter(array_contains(col("genre_ids"), genre_id))

    stats = filtered.agg(
        avg("vote_average").alias("avg_rating"),
        avg("vote_count").alias("avg_vote_count"),
        avg("popularity").alias("avg_popularity"),
        count("*").alias("total_movies")
    ).collect()

    return stats[0].asDict() if stats else None


# In[16]:


from pyspark.sql.functions import col, array_contains, year, to_date, avg, count

# Timeline Data for each genre
def genre_timeline(genre_id):
    # Step 1: Filter movies that contain the genre ID and have a valid release_date
    filtered = Movies_df.filter(
        (array_contains(col("genre_ids"), genre_id)) &
        (col("release_date").isNotNull())
    )

    # Step 2: Convert release_date to date and extract year
    with_year = filtered.withColumn("release_year", year(to_date(col("release_date"), "yyyy-MM-dd")))

    # Step 3: Group by year and compute averages and count
    result = with_year.groupBy("release_year").agg(
        avg("vote_average").alias("avg_rating"),
        avg("popularity").alias("avg_popularity"),
        count("*").alias("movie_count")
    ).orderBy("release_year")

    return result

timeline_df = genre_timeline(28)
timeline_df.show()


# In[12]:


# Get top 10 voted movies for the genre
def get_top_10_movies(genre_id):
    top_10_df = Movies_df.filter(
        (array_contains(col("genre_ids"), genre_id)) &
        (col("vote_count") > 10000)
    ).select("title", "vote_average", "release_date") \
     .orderBy(col("vote_average").desc()) \
     .limit(10)

    return top_10_df.toPandas().to_dict("records")
get_top_10_movies(28)


# In[13]:


# Calculate the average sound track length for genre
def average_soundtrack_amount(genre_id):
    # Get movie titles in the genre
    titles = db.Movies.find(
        {"genre_ids": genre_id},
        {"_id": 0, "title": 1}
    )
    title_set = {doc["title"] for doc in titles}

    # Get related album IDs from Movie_Albums
    album_ids = db.Movie_Albums.find(
        {"search_term": {"$in": list(title_set)}},
        {"_id": 0, "raw_data.id": 1}
    )
    album_id_set = {doc["raw_data"]["id"] for doc in album_ids if "raw_data" in doc}

    # Get total_tracks from Albums_Info
    albums = db.Albums_Info.find(
        {"album_id": {"$in": list(album_id_set)}},
        {"_id": 0, "raw_data.total_tracks": 1}
    )

    total_tracks = [album["raw_data"]["total_tracks"] for album in albums if "raw_data" in album]

    if total_tracks:
        avg_length = sum(total_tracks) / len(total_tracks)
    else:
        avg_length = 0

    return avg_length
average_soundtrack_amount(18)


# In[14]:


from collections import Counter

def top_composers(genre_id):
    # Get titles from movies in genre
    titles = db.Movies.find({"genre_ids": genre_id}, {"_id": 0, "title": 1})
    title_set = {doc["title"] for doc in titles}

    # Get album IDs via Movie_Albums
    album_ids = db.Movie_Albums.find(
        {"search_term": {"$in": list(title_set)}},
        {"_id": 0, "raw_data.id": 1}
    )
    album_id_set = {doc["raw_data"]["id"] for doc in album_ids if "raw_data" in doc}

    # Extract composers (artists) from Albums_Info
    albums = db.Albums_Info.find(
        {"album_id": {"$in": list(album_id_set)}},
        {"_id": 0, "raw_data.artists": 1}
    )

    composer_counter = Counter()
    for album in albums:
        if "raw_data" in album:
            for artist in album["raw_data"].get("artists", []):
                name = artist.get("name")
                if name:
                    composer_counter[name] += 1

    return composer_counter.most_common(10)


# In[15]:


top_composers(18)


# In[ ]:




