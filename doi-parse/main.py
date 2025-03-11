import os
import re
import time

from pymongo import MongoClient
from dotenv import load_dotenv
from metapub import CrossRefFetcher
from typing import List


load_dotenv()


MONGO_URL = os.getenv("MONGO_URL_TEST")
client = MongoClient(MONGO_URL)


def clean_title(title: str) -> str:
    """Функция очистки названия статьи."""
    title = re.sub(r'<[^>]+>', '', title)
    title = title.replace('‐', '-').replace('–', '-').replace('—', '-')
    title = title.rstrip(".")
    return title.strip()


def is_url_valid(
        url: str,
        trash: List
) -> bool:
    """Функция проверки, входит ли URL в правильные."""
    for item in trash:
        if item in url:
            return False
    return True


def insert_value(
        collection,
        doc_id,
        field_name,
        value
) -> None:
    """Функция вставки DOI или publisherName."""
    collection.update_one(
        {"_id": doc_id},
        {"$set": {field_name: value}}
    )


def search_article_with_retry(
        search_func,
        search_param: str,
        max_retries: int = 3
):
    """Функция для поиска статьи с повторными попытками при ошибках SSL"""
    retry_count = 0
    last_error = None
    while retry_count < max_retries:
        try:
            result = search_func(search_param)
            return result

        except Exception as e:
            last_error = e
            error_str = str(e)

            if (
                "SSL" in error_str
                or "timeout" in error_str
                or "timed out" in error_str
            ):
                retry_count += 1
                wait_time = 2 ** retry_count
                print(f"Повторная попытка {retry_count}/{max_retries} через {wait_time} секунд...")
                time.sleep(wait_time)
            else:
                break

    print(f"Все попытки исчерпаны. Последняя ошибка: {last_error}")
    return


def process_documents() -> None:
    """Функция для обработки каждой записи."""
    db = client["telegram_client"]
    collection = db["parsedSites"]

    trash = [
        "https://jsstd.org/",
        "https://www.dermatoljournal.com/",
        "https://www.mdedge.com/"
    ]

    cnt_all = 0
    cnt = 0
    crossref_fetcher = CrossRefFetcher()

    for document in collection.find({}):
        doc_url = document["articleUrl"]

        if is_url_valid(doc_url, trash):
            cnt_all += 1
            try:
                if "doi" in document and document["doi"]:
                    doi = document["doi"]
                    try:
                        article = search_article_with_retry(
                            crossref_fetcher.article_by_doi,
                            doi
                        )

                        if article:
                            print(article.publisher)

                            insert_value(
                                collection=collection,
                                doc_id=document["_id"],
                                field_name="publisherName",
                                value=article.publisher
                            )

                            cnt += 1

                        else:
                            print('fail')

                    except Exception as e:
                        print(f"Ошибка при поиске статьи по DOI: {e}")

                elif "title" in document and document["title"]:
                    title = document["title"]

                    if "pubmed" in doc_url and "PubMed" in title:
                        title = title[:-9]

                    clean_db_title = clean_title(title)

                    article = search_article_with_retry(
                        crossref_fetcher.article_by_title,
                        title
                    )

                    if article:
                        clean_article_title = clean_title(article.title[0])
                        if clean_article_title == clean_db_title:
                            print(article.title[0])
                            print(title)
                            print(article.publisher)
                            print(article.doi)

                            insert_value(
                                collection=collection,
                                doc_id=document["_id"],
                                field_name="publisherName",
                                value=article.publisher
                            )

                            insert_value(
                                collection=collection,
                                doc_id=document["_id"],
                                field_name="doi",
                                value=article.doi
                            )

                            cnt += 1
            except Exception as e:
                print(f"Общая ошибка при обработке документа {doc_url}: {e}")
            print("--------------------")

    print(cnt, " | ", cnt_all)


if __name__ == "__main__":
    process_documents()
