from pymongo import MongoClient
from bson import ObjectId
import json

try:
    connection = MongoClient('localhost', 27017)
except:
    print("Error occured during connect")

db = connection['MyProject']
comments = db['comments']


def top_ten_user_max_comment():
    output = comments.aggregate([{"$group": {"_id": {"name": "$name"}, "total_comments": {"$sum": 1}}},
                                 {"$sort": {"total_comments": -1}},
                                 {"$limit": 10}])
    for doc in output:
        print(doc)


print("Top 10 users who made the maximum number of comments are- \n")
top_ten_user_max_comment()
print()

def top_ten_movies_having_most_comment():
    output = comments.aggregate([{"$group": {"_id": {"name": "$movie_id"}, "total_comments": {"$sum": 1}}},
                                 {"$sort": {"total_comments": -1}},
                                 {"$limit": 10}])
    for doc in output:
        print(doc)


print("Top 10 movies with most comments are-\n")
top_ten_movies_having_most_comment()
print()


def comment_for_each_month_for_year(year):
    output = comments.aggregate(
        [{"$project": {"_id": 0, "date": {"$toDate": {"$convert": {"input": "$date", "to": "long"}}}}},
         {"$group": {"_id": {"year": {"$year": "$date"}, "month": {"$month": "$date"}}, "total_comment": {"$sum": 1}}},
         {"$match": {"_id.year": {"$eq": year}}},
         {"$sort": {"_id.month": 1}}
         ])
    for i in output:
        print(i)


print("In 2000 year the number of comments created in each months are-\n")
comment_for_each_month_for_year(2000)
print()