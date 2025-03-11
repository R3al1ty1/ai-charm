import os

from pymongo import MongoClient
from dotenv import load_dotenv


load_dotenv()


docker_uri = "mongodb://localhost:27017"

client = MongoClient(os.getenv("MONGO_URL"))
db = client["telegram_client"]
collection = db["parsedSites"]

cnt = 0
records = []
for record in collection.find({}):
    if "category" in record and record["category"] == "articles":
        records.append(record)
        cnt += 1

try:
    new_client = MongoClient(docker_uri)
    new_db = new_client["telegram_client"]
    new_collection = new_db["parsedSites"]

    result = new_collection.insert_many(records)
    # print("Вставленные id:", result.inserted_ids)
    print(cnt)

except Exception as e:
    print(e)
    client.close()

client.close()
