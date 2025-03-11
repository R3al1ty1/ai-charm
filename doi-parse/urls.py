import os
from pymongo import MongoClient
from dotenv import load_dotenv


load_dotenv()

urls = []

MONGO_URL = os.getenv("MONGO_URL")
client = MongoClient(MONGO_URL)


def urls_getter():
    db = client["telegram_client"]
    collection = db["parsedSites"]

    for document in collection.find({}):
        if "articleUrl" in document:
            doc_url = document["articleUrl"]
            base_url = "/".join(doc_url.split("/")[:3]) + "/"
            if base_url not in urls:
                print(base_url, doc_url)
                urls.append(base_url)
    print(len(urls))
    print(urls)

if __name__=="__main__":
    urls_getter()
