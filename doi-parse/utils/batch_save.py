import os
import aiohttp
import asyncio
import bibtexparser
from typing import List
from dotenv import load_dotenv
from pymongo import MongoClient
from bs4 import BeautifulSoup

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL_TEST")
API_KEY1 = os.getenv("PUBMED_API_KEY")
API_KEY2 = os.getenv("PUBMED_SECOND_KEY")
API_KEY3 = os.getenv("PUBMED_THIRD_KEY")
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

        soup = BeautifulSoup(response_text, 'xml')  # Парсим XML

        # Извлекаем FullJournalName (издателя)
        publisher_meta = soup.find('Item', {'Name': 'FullJournalName'})
        if publisher_meta:
            publisher = publisher_meta.text.strip()

        # Извлекаем DOI (убираем префикс "doi:")
        doi_meta = soup.find('Item', {'Name': 'ELocationID'})
        if doi_meta:
            doi = doi_meta.text.replace("doi: ", "").strip()

        return doi, publisher

    except Exception as e:
        print(f"Ошибка при парсинге XML: {e}")
        return None, None


async def extract_bibtex(bibtex_str):
    try:
        entry = bibtexparser.loads(bibtex_str).entries[0]
        doi = entry.get('doi') or entry.get('DOI')
        journal = entry.get('journal')

        return doi, journal

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

    if "pubmed" in url:
        pubmed_id = url.rstrip('/').split('/')[-1]

        api_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
        params = {
            "db": "pubmed",
            "id": pubmed_id,
            "api_key": api_key,
            "retmode": "xml"
        }
        async with session.get(api_url, params=params) as response:
            res = await response.text()
            return await publisher_doi_finder(res)
    else:
        doi = url.rstrip('/').split('/')[-2:]
        doi_s = doi[0] + "/" + doi[1]
        url = f"https://api.crossref.org/works/{doi_s}/transform/application/x-bibtex"

        async with session.get(url) as response:
            res = await response.text()

            return await extract_bibtex(res)


async def process_batch(batch, api_key):
    """Обработка пакета URL с определенным API ключом"""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, url, api_key) for url in batch]
        results = await asyncio.gather(*tasks)
        return results


async def send_request():
    db = client["telegram_client"]
    collection = db["parsedSites"]
    urls_pub = []
    urls_others = []
    cnt = 0

    for document in collection.find({}):
        if (
            "articleUrl" in document and
            ("category" in document and document["category"] == "articles")
        ):
            if cnt < 1000:
                doc_url = document["articleUrl"]
                check = await is_url_valid(doc_url, right)
                if check and "pubmed" in doc_url:
                    cnt += 1
                    urls_pub.append(doc_url)
                elif check:
                    cnt += 1
                    urls_others.append(doc_url)

    all_results = []
    batch_size_pub = 10
    part_size = len(urls_pub) // 3
    urls1 = urls_pub[:part_size]
    urls2 = urls_pub[part_size:2*part_size]
    urls3 = urls_pub[2*part_size:]

    url_batches1 = [urls1[i:i+batch_size_pub] for i in range(0, len(urls1), batch_size_pub)]
    url_batches2 = [urls2[i:i+batch_size_pub] for i in range(0, len(urls2), batch_size_pub)]
    url_batches3 = [urls3[i:i+batch_size_pub] for i in range(0, len(urls3), batch_size_pub)]

    max_batches = max(len(url_batches1), len(url_batches2), len(url_batches3))

    for i in range(max_batches):
        tasks = []

        if i < len(url_batches1):
            tasks.append(process_batch(url_batches1[i], API_KEY1))
            print(f"Запуск группы {i+1} с ключом 1 ({len(url_batches1[i])} URL)")

        if i < len(url_batches2):
            tasks.append(process_batch(url_batches2[i], API_KEY2))
            print(f"Запуск группы {i+1} с ключом 2 ({len(url_batches2[i])} URL)")

        if i < len(url_batches3):
            tasks.append(process_batch(url_batches3[i], API_KEY3))
            print(f"Запуск группы {i+1} с ключом 3 ({len(url_batches3[i])} URL)")

        batch_results = await asyncio.gather(*tasks)

        for batch_result in batch_results:
            all_results.extend(batch_result)

        print(f"Завершена обработка группы {i+1} для всех ключей")
        await asyncio.sleep(1)

    batch_size_others = 5

    url_batches_others = [
        urls_others[i:i + batch_size_others] for i in range(
            0, len(urls_others), batch_size_others
        )
    ]

    for i, batch in enumerate(url_batches_others):

        current_api_key = ""

        async with aiohttp.ClientSession() as session:
            tasks = [
                fetch_data(session, url, current_api_key) for url in batch
            ]
            results = await asyncio.gather(*tasks)
            all_results.extend(results)

        print(f"Обработана группа {i+1} из {len(batch)} запросов...")
        await asyncio.sleep(1)

    count = all_results.count((None, None))
    print(f"Количество неудачных запросов: {count}")
    print(count)
    print(all_results)


if __name__ == "__main__":
    asyncio.run(send_request())
