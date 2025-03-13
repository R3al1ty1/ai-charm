import os
import aiohttp
import asyncio
from typing import List
from dotenv import load_dotenv
from pymongo import MongoClient
from bs4 import BeautifulSoup

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL_TEST")
API_KEY1 = os.getenv("PUBMED_API_KEY")
API_KEY2 = os.getenv("PUBMED_SECOND_KEY")
client = MongoClient(MONGO_URL)

right = [
    "https://doi.org/",
    "https://ncbi.nlm.nih.gov/",
    "https://onlinelibrary.wiley.com/",
    "https://pubmed.ncbi.nlm.nih.gov/",
    "https://www.pubmed.ncbi.nlm.nih.gov/",
]


async def publisher_doi_finder(response_text: str):
    try:
        doi = None
        publisher = None

        soup = BeautifulSoup(response_text, 'xml')

        publisher_meta = soup.find('Item', {'Name': 'FullJournalName'})
        if publisher_meta:
            publisher = publisher_meta.text.strip()

        doi_meta = soup.find('Item', {'Name': 'ELocationID'})
        if doi_meta:
            doi = doi_meta.text.replace("doi: ", "").strip()

        return doi, publisher

    except Exception as e:
        print(f"Ошибка при парсинге XML: {e}")
        return None, None


async def is_url_valid(
        url: str,
        right: List
) -> bool:
    """Функция проверки, входит ли URL в правильные."""
    for item in right:
        if item in url:
            return True
    return False


async def fetch_data(session, url, api_key):
    params = {}

    if "pubmed.ncbi.nlm.nih.gov" in url:
        pubmed_id = url.rstrip('/').split('/')[-1]

        api_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
        params = {
            "db": "pubmed",
            "id": pubmed_id,
            "api_key": api_key,
            "retmode": "xml"
        }
    else:
        api_url = url

    async with session.get(api_url, params=params) as response:
        res = await response.text()
        return await publisher_doi_finder(res)


async def process_batch(batch, api_key):
    """Обработка пакета URL с определенным API ключом"""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, url, api_key) for url in batch]
        results = await asyncio.gather(*tasks)
        return results


async def send_request():
    db = client["telegram_client"]
    collection = db["parsedSites"]
    urls = []
    cnt = 0

    for document in collection.find({}):
        if (
            "articleUrl" in document and
            ("category" in document and document["category"] == "articles")
        ):
            if cnt < 100:
                doc_url = document["articleUrl"]
                check = await is_url_valid(doc_url, right)
                if check:
                    cnt += 1
                    print(doc_url)
                    urls.append(doc_url)

    middle = len(urls) // 2
    urls1 = urls[:middle]
    urls2 = urls[middle:]

    batch_size = 10
    url_batches1 = [
        urls1[i:i + batch_size] for i in range(0, len(urls1), batch_size)
    ]
    url_batches2 = [
        urls2[i:i + batch_size] for i in range(0, len(urls2), batch_size)
    ]

    all_results = []

    for i in range(max(len(url_batches1), len(url_batches2))):
        tasks = []

        if i < len(url_batches1):
            tasks.append(process_batch(url_batches1[i], API_KEY1))
            print(f"Запуск группы {i+1} с ключом 1 ({len(url_batches1[i])} URL)")

        if i < len(url_batches2):
            tasks.append(process_batch(url_batches2[i], API_KEY2))
            print(f"Запуск группы {i+1} с ключом 2 ({len(url_batches2[i])} URL)")

        batch_results = await asyncio.gather(*tasks)

        for batch_result in batch_results:
            all_results.extend(batch_result)

        print(f"Завершена обработка группы {i+1}")
        await asyncio.sleep(1)

    count = all_results.count((None, None))
    print(f"Количество неудачных запросов: {count}")
    print(f"Всего запросов обработано: {len(all_results)}")
    print(all_results)


if __name__ == "__main__":
    asyncio.run(send_request())
