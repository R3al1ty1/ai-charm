import os

from dotenv import load_dotenv
from pymongo import MongoClient
from urllib.parse import urlparse


load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["telegram_client"]
collection = db["drsarha_articles"]

# Множество для хранения уникальных доменов
unique_domains = set()

# Итерация по всем документам в коллекции
for document in collection.find({}, {"articleUrl": 1}):
    url = document.get("articleUrl")
    if url:
        # Извлечение домена из URL
        domain = urlparse(url).netloc
        unique_domains.add(domain)

# Вывод всех уникальных доменов
for domain in unique_domains:
    print(domain)

# Закрытие соединения с MongoDB
client.close()
