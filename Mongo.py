import json
from pymongo import MongoClient


# Making Connection
myclient = MongoClient("mongodb://localhost:27017/")

# database
db = myclient["Database"]

# Created or Switched to collection
# names: GeeksForGeeks
Collection = db["Pedestian"]

# Loading or Opening the json file
with open('Pedestian.json') as Pedestian_file:
    Pedestian_file_data = json.load(Pedestian_file)

# Inserting the loaded data in the Collection
# if JSON contains data more than one entry
# insert_many is used else inser_one is used
if isinstance(Pedestian_file_data, list):
    Collection.insert_many(Pedestian_file_data)
else:
    Collection.insert_one(Pedestian_file_data)
Collection = db["SensorData"]
with open('SensorData.json') as SensorData_file:
    SensorData_file_data = json.load(SensorData_file)
if isinstance(SensorData_file_data, list):
    Collection.insert_many(SensorData_file_data)
else:
    Collection.insert_one(SensorData_file_data)
