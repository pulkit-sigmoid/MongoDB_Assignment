from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
import json

try:
    connection = MongoClient('localhost', 27017)
except:
    print("Error in Connect")

db = connection['MyProject']  # to use database

# uploading comments data
file = open('/Users/pulkitgupta/Downloads/sample_mflix/comments.json', 'r')
data = []
lines = file.readlines()
for line in lines:
    dic = json.loads(line)  # Converting JSON to Python to
    dic["_id"] = ObjectId(dic["_id"]["$oid"])
    dic["date"] = dic["date"]["$date"]["$numberLong"]
    data.append(dic)
collection = db["comments"]
collection.insert_many(data)

# uploading movies data
file = open('/Users/pulkitgupta/Downloads/sample_mflix/movies.json', 'r')
data = []
lines = file.readlines()
for line in lines:
    dic = json.loads(line)
    dic["_id"] = ObjectId(dic["_id"]["$oid"])
    data.append(dic)

collection = db["movies"]
collection.insert_many(data)

# uploading theaters data
file = open('/Users/pulkitgupta/Downloads/sample_mflix/theaters.json', 'r')
data = []
lines = file.readlines()
for line in lines:
    dic = json.loads(line)
    dic["_id"] = ObjectId(dic["_id"]["$oid"])
    dic["location"]["geo"]["coordinates"][0] = float(dic["location"]["geo"]["coordinates"][0]["$numberDouble"])
    dic["location"]["geo"]["coordinates"][1] = float(dic["location"]["geo"]["coordinates"][1]["$numberDouble"])
    data.append(dic)

collection = db["theaters"]
collection.insert_many(data)

# uploading sessions data
file = open('/Users/pulkitgupta/Downloads/sample_mflix/sessions.json', 'r')
data = []
lines = file.readlines()
for line in lines:
    dic = json.loads(line)
    dic["_id"] = ObjectId(dic["_id"]["$oid"])
    data.append(dic)

collection = db["sessions"]
collection.insert_many(data)

# uploading users data
file = open('/Users/pulkitgupta/Downloads/sample_mflix/users.json', 'r')
data = []
lines = file.readlines()
for line in lines:
    dic = json.loads(line)
    dic["_id"] = ObjectId(dic["_id"]["$oid"])
    data.append(dic)

collection = db["users"]
collection.insert_many(data)
