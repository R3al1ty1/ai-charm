import os

from dotenv import load_dotenv
from pymongo import MongoClient


def migrate_collection(old_coll, new_coll):

    # Фильтр для выбора только статей
    query = {"category": "articles"}

    for doc in old_coll.find(query):
        new_doc = {
            "_id": doc["_id"],  # Сохраняем оригинальный ID
            "languages": ["en", "ru"],
            "title": {
                "ru": {
                    "ai": doc.get("title_translation_ai", ""),
                    "human": doc.get("title_translation_human", "")
                },
                "en": {
                    "ai": doc.get("title", ""),
                    "human": ""
                },
                "raw": doc.get("title", "")
            },
            "content": {
                "ru": {
                    "ai": doc.get("pdf_text_translation_ai") or doc.get("translation_ai", ""),
                    "human": doc.get("pdf_text_translation_human") or doc.get("translation_human", "")
                },
                "en": {
                    "ai": doc.get("content", ""),
                    "human": ""
                },
                "raw": doc.get("content", "")
            },
            "summary": {
                "ru": {
                    "ai": doc.get("summary_ai", ""),
                    "human": doc.get("summary_human", "")
                }
            },
            "meta": {
                "isIndexed": doc.get("isIndexed", False),
                "isPublished": doc.get("isPublished", False),
                "isDeleted": doc.get("isDeleted", False),
                "hasTranslation": any([
                    doc.get("pdf_text_translation_ai"),
                    doc.get("translation_ai"),
                    doc.get("title_translation_ai")
                ]),
                "hasDevComment": False,
                "isClinicalCase": doc.get("is_clinic_case", False)
            },
            "articleUrl": doc.get("articleUrl", ""),
            "dates": {
                "published": doc.get("publishedDate", ""),
                "created": doc.get("createdAt", ""),
                "updated": doc.get("updatedAt", "")
            },
            "category": doc.get("category", ""),
            "subcategory": doc.get("subcategory", ""),
            "references": doc.get("references", []),
            "doi": doc.get("doi", ""),
            "publisherName": doc.get("publisherName", ""),
            "authors": doc.get("authors", []),
            "addons": {},
            "parserIteration": doc.get("parserIteration", 1)
        }

        # Поля, которые явно обрабатываются и не должны попасть в addons
        exclude_fields = {
            '_id', 'title', 'content', 'createdAt', 'publishedDate',
            'title_translation_ai', 'title_translation_human', 'pdf_text_translation_ai',
            'translation_ai', 'pdf_text_translation_human', 'translation_human',
            'summary_ai', 'summary_human', 'isIndexed', 'isPublished', 'isDeleted',
            'is_clinic_case', 'articleUrl', 'category', 'subcategory', 'references',
            'doi', 'publisherName', 'authors', 'parserIteration', 'updatedAt'
        }

        # Добавляем все неиспользованные поля в addons
        for field in doc:
            if field not in exclude_fields:
                new_doc['addons'][field] = doc[field]

        # Вставка/обновление документа
        new_coll.replace_one({"_id": doc["_id"]}, new_doc, upsert=True)


load_dotenv()


MONGO_URL = os.getenv("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["telegram_client"]
old_collection = db["parsedSites"]
new_collection = db["drsarha_articles"]

migrate_collection(old_collection, new_collection)
