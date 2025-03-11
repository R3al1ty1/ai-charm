import json
from collections import defaultdict
from pymongo import MongoClient
from main import MONGO_URL


client = MongoClient(MONGO_URL)
db = client["telegram_client"]
collection = db["parsedSites"]

cursor = collection.find({}).sort('_id', -1).limit(2000)

record_counts = defaultdict(int)
record_docs = defaultdict(list)

for record in cursor:
    record_without_id = record.copy()
    record_without_id.pop('_id', None)
    
    record_str = json.dumps(record_without_id, sort_keys=True, default=str)
    
    record_counts[record_str] += 1
    record_docs[record_str].append(record)

for record_str, count in record_counts.items():
    if count > 1:
        print("Найден дубликат:")
        print("Запись:", record_str)
        print("Количество повторений:", count)
        print("Документы:", record_docs[record_str])
        print("-" * 40)

client.close()
