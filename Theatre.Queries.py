from pymongo import MongoClient
from bson import ObjectId
import json

try:
    connection = MongoClient('localhost', 27017)
except:
    print("Error occured during connect")

db = connection['MyProject']
comments = db['comments']
users = db['users']
movies = db['movies']
theaters = db['theaters']
sessions = db['sessions']


def city_with_max_theaters():
    output = theaters.aggregate([{"$group": {"_id": "$location.address.city", "count": {"$sum": 1}}},
                                 {"$project": {"location.address.city": 1, "count": 1}},
                                 {"$sort": {"count": -1}},
                                 {"$limit": 10}])

    for i in output:
        print(i)


print("Top 10 cities with maximum number of theatres are -\n")
city_with_max_theaters()
print()


# db.theaters.createIndex({"location.geo": "2dsphere"})
def theaters_near_given_coordinates():
    output = db.theaters.aggregate([{"$geoNear": {"near": {"type": "Point", "coordinates": [-122.51875, 37.966579]},
                                                  "maxDistance": 10000000, "distanceField": "distance"}},
                                    {"$project": {"location.address.city": 1, "_id": 0, "location.geo.coordinates": 1}},
                                    {"$limit": 10}])
    for i in output:
        print(i)


print("Top 10 theatres nearby [-122.51875, 37.966579] coordinates are -\n")
theaters_near_given_coordinates()
print()
