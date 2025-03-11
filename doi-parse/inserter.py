from pymongo import MongoClient
from main import MONGO_URL


docker_uri = "mongodb://localhost:27017"

client = MongoClient(MONGO_URL)
db = client["telegram_client"]
collection = db["parsedSites"]

cnt = 0
records = []
for record in collection.find({}):
    if cnt > 20:
        break
    if "ncbi.nlm.nih.gov" in record["articleUrl"] and "pubmed" in record["articleUrl"]:
        records.append(record)
        cnt += 1
# records = [record for record in collection.find({})]
# print(records)

# result = collection.delete_many({"name": {"$regex": "^record"}})
# print("Удалено документов:", result.deleted_count)

try:
    new_client = MongoClient(docker_uri)
    new_db = new_client["telegram_client"]
    new_collection = new_db["parsedSites"]

    result = new_collection.insert_many(records)
    print("Вставленные id:", result.inserted_ids)

except Exception as e:
    print(e)
    client.close()

client.close()
