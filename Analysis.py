#!/usr/bin/env python
# coding: utf-8

# In[78]:


from pymongo import MongoClient
from pprint import pprint
import json


# In[91]:


# Connect to MongoDB
client = MongoClient("localhost", 27017)
db = client["project"]

genre_col = db["Genre"]
movies_col = db["Movies"]
movies_albums_col = db["Movie_Albums"]
album_info_col = db["Albums_Info"]


# In[92]:


# Get Genre ID by name
def get_genre_id(genre_name):
    genre_doc = db.Genre.find_one()
    for genre in genre_doc['genres']:
        if genre['name'].lower() == genre_name.lower():
            return genre['id']
    return None

def get_all_genre_names():
    genre_doc = db.Genre.find_one()
    return sorted([genre["name"] for genre in genre_doc["genres"]])


# In[93]:


# Average Movie Rating in Genre
def average_stats(genre_id):
    pipeline = [
        {"$match": {"genre_ids": genre_id}},  
        {"$group": {
            "_id": None,
            "avg_rating": {"$avg": "$vote_average"},
            "avg_vote_count": {"$avg": "$vote_count"},
            "avg_popularity": {"$avg": "$popularity"},
            "total_movies": {"$sum": 1}
        }}
    ]
    result = list(db.Movies.aggregate(pipeline))
    return result[0] if result else None


# In[94]:


# Timeline Data for each genre
def genre_timeline(genre_id):
    pipeline = [
        {
            "$match": {
                "genre_ids": genre_id,
                "release_date": {"$exists": True, "$ne": None}
            }
        },
        {
            "$addFields": {
                "release_year": {"$year": {"$toDate": "$release_date"}}
            }
        },
        {
            "$group": {
                "_id": "$release_year",
                "avg_rating": {"$avg": "$vote_average"},
                "avg_popularity": {"$avg": "$popularity"},
                "movie_count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    return list(db.Movies.aggregate(pipeline))


# In[95]:


# Get top 10 voted movies for the genre
def get_top_10_movies(genre_id):
    top_10 = db.Movies.find(
        {
            "genre_ids": genre_id,
            "vote_count": {"$gt": 10000}  
        },
        {
            "_id": 0,
            "title": 1,
            "vote_average": 1,
            "release_date": 1
        }
    ).sort("vote_average", -1).limit(10)

    return list(top_10)
get_top_10_movies(28)


# In[ ]:





# In[96]:


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


# In[97]:


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


# In[98]:


top_composers(18)


# In[ ]:




