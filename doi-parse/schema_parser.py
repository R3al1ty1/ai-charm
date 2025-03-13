import os
from pymongo import MongoClient
from collections import defaultdict


def get_keys_statistics():

    MONGO_URL = os.getenv("MONGO_URL")
    client = MongoClient(MONGO_URL)
    db = client["telegram_client"]
    collection = db["parsedSites"]

    key_counts = defaultdict(int)

    query = {"category": "articles"}
    documents = collection.find(query)

    for doc in documents:
        def extract_keys(dct, parent_key=''):
            for key, value in dct.items():
                full_key = f"{parent_key}.{key}" if parent_key else key
                key_counts[full_key] += 1
                if isinstance(value, dict):
                    extract_keys(value, full_key)
        extract_keys(doc)

    sorted_stats = sorted(key_counts.items(), key=lambda x: x[1], reverse=True)

    with open("stats.txt", "w", encoding="utf-8") as f:
        f.write("Статистика ключей:\n")
        for key, count in sorted_stats:
            f.write(f"{key}: {count}\n")


if __name__ == "__main__":
    get_keys_statistics()
