from pymongo import MongoClient
from bson import ObjectId
import json

try:
    connection = MongoClient('localhost', 27017)
except:
    print("Error occured during connect")

db = connection['MyProject']
movies = db['movies']

print("The top n movies -\n")


def movies_highest_imdb_rating(n):
    output = movies.aggregate([{"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
                               {"$sort": {"imdb.rating": -1}}, {"$limit": n}])
    for doc in output:
        print(doc)


print("With highest Imdb ratings are -\n")
movies_highest_imdb_rating(3)
print()


def movie_with_highest_IMDB_rating_in_year(n, year):
    output = movies.aggregate(
        [{"$addFields": {"yr": {"$getField": {"field": {"$literal": "$numberInt"}, "input": "$year"}},
                         "rating": {"$getField": {"field": {"$literal": "$numberDouble"}, "input": "$imdb.rating"}}}},
         {"$match": {"yr": {"$eq": year}}},
         {"$project": {"_id": 0, "title": 1, "yr": 1, "rating": 1}},
         {"$sort": {"rating": -1}},
         {"$limit": n}])
    for i in output:
        print(i)


print("With highest Imdb rating in 2000 year are -\n")
movie_with_highest_IMDB_rating_in_year(3, "2000")
print()


def movies_having_votes_gt_thousand(n):
    output = db.movies.aggregate(
        [{"$addFields": {"vote": {"$getField": {"field": {"$literal": "$numberInt"}, "input": "$imdb.votes"}}}},
         {"$match": {"$expr": {"$gt": [{"$toInt": "$vote"}, 1000]}}},
         {"$sort": {"imdb.rating": -1}},
         {"$project": {"_id": 0, "title": 1, "imdb.rating": 1, "vote": 1}},
         {"$limit": n}])
    for i in output:
        print(i)


print("With highest Imdb rating with number of votes greater than 1000 are -\n")
movies_having_votes_gt_thousand(3)
print()


def movies_with_matching_string(n, string_match):
    pipeline = [{"$addFields": {"tomatoes_Rating": "$tomatoes.viewer.rating", "result": {
        "$cond": {"if": {"$regexMatch": {"input": "$title", "regex": string_match}}, "then": "yes", "else": "no"}}}},
                {"$project": {"_id": 0, "title": 1, "tomatoes_Rating": 1, "result": 1}},
                {"$match": {"result": {"$eq": "yes"}}},
                {"$sort": {"tomatoes_Rating": -1}},
                {"$limit": n}]
    output = list(db.movies.aggregate(pipeline))
    for doc in output:
        print(doc)


print("With title matching a given pattern with highest tomatoes rating are -\n")
movies_with_matching_string(3, 'my')
print()

print("Top n directors -\n")


def top_director_create_max_Movies(n):
    output = movies.aggregate([{"$unwind": "$directors"},
                               {"$group": {"_id": {"dir_name": "$directors"}, "Movie_count": {"$sum": 1}}},
                               {"$project": {"dir_name": 1, "Movie_count": 1}},
                               {"$sort": {"Movie_count": -1}},
                               {"$limit": n}, ])
    for doc in output:
        print(doc)


print("Who created the maximum number of movies are -\n")
top_director_create_max_Movies(3)
print()


def top_director_max_movie_in_year(n, year):
    output = movies.aggregate(
        [{"$addFields": {"yr": {"$getField": {"field": {"$literal": "$numberInt"}, "input": "$year"}}}},
         {"$unwind": "$directors"},
         {"$match": {"yr": {"$eq": year}}},
         {"$group": {"_id": {"director_name": "$directors"}, "count": {"$sum": 1}}},
         {"$project": {"director_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": n}])
    for i in output:
        print(i)


print("Who created the maximum number of movies in 2000 are -\n")
top_director_max_movie_in_year(3, '2000')
print()


def top_directors_with_highest_movie_given_genre(n, genres):
    output = movies.aggregate(
        [{"$unwind": "$directors"},
         {"$match": {"genres": {"$eq": genres}}},
         {"$group": {"_id": {"director_name": "$directors"}, "count": {"$sum": 1}}},
         {"$project": {"director_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": n}])
    for i in output:
        print(i)


print("Who created the max number of movies for 'Comedy' and 'Drama' genres are -\n")
top_directors_with_highest_movie_given_genre(3, ["Comedy", "Drama"])
print()


print("Top n actors -\n")


def top_actor_starred_in_max_Movies(n):
    output = movies.aggregate([{"$unwind": "$cast"},
                               {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
                               {"$project": {"cast": 1, "count": 1}},
                               {"$sort": {"count": -1}},
                               {"$limit": n}, ])
    for doc in output:
        print(doc)


print("Who starred in the maximum number of movies are -\n")
top_actor_starred_in_max_Movies(3)
print()


def top_actor_max_movie_in_year(n, year):
    output = movies.aggregate(
        [{"$addFields": {"yr": {"$getField": {"field": {"$literal": "$numberInt"}, "input": "$year"}}}},
         {"$unwind": "$cast"},
         {"$match": {"yr": {"$eq": year}}},
         {"$group": {"_id": {"actor_name": "$cast"}, "count": {"$sum": 1}}},
         {"$project": {"actor_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": n}])

    for i in output:
        print(i)


print("Who starred in the maximum number of movies in 2000 year are -\n")
top_actor_max_movie_in_year(3, "2000")
print()


def top_actor_with_highest_movie_given_genre(n, genres):
    output = movies.aggregate(
        [{"$unwind": "$cast"},
         {"$match": {"genres": {"$eq": genres}}},
         {"$group": {"_id": {"actor_name": "$directors"}, "count": {"$sum": 1}}},
         {"$project": {"actor_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": n}])
    for i in output:
        print(i)


print("Who starred the maximum number of movies in Comedy and Drama are -\n")
top_actor_with_highest_movie_given_genre(3, ["Comedy", "Drama"])
print()


def top_movie_for_each_genre(n):
    output = movies.aggregate([{"$unwind": "$genres"}, {"$sort": {"imdb.rating": -1}},
                               {"$group": {"_id": "$genres", "title": {"$push": "$title"},
                                           "rating": {"$push": {"$getField": {"field": {"$literal": "$numberDouble"},
                                                                              "input": "$imdb.rating"}}}}},
                               {"$project": {"_id": 1, "Movies": {"$slice": ['$title', 0, n]},
                                             "ratings": {"$slice": ["$rating", 0, n]}}}])
    for i in output:
        print(i)


print("Top n movies for each genre are -\n")
top_movie_for_each_genre(3)
print()