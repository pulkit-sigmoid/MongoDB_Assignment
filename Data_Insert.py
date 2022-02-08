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


def insertComment(value):
    comments.insert_one(value)


def insertMovie(value):
    users.insert_one(value)


def insertTheater(value):
    theaters.insert_one(value)


def insertUser(value):
    users.insert_one(value)


def insertSession(value):
    sessions.insert_one(value)


# any value enter by user to insert data in User Comments Movie Theaters and Users

# value = {
#     "name": "Kapil",
#     "age": 25
# }

# insertComment(value)
# insertMovie(value)
# insertTheater(value)
# insertUser(value)

