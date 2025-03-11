import os
import re
import asyncio
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
from metapub import CrossRefFetcher, PubMedFetcher
from bs4 import BeautifulSoup


load_dotenv()

MONGO_URL = os.getenv("MONGO_URL_TEST")
client = MongoClient(MONGO_URL)


def clean_title(title):
    title = re.sub(r'<[^>]+>', '', title)
    title = title.replace('‐', '-').replace('–', '-').replace('—', '-')

    return title.strip()


def get_pmid_from_page(url):
    try:
        params = {
            "report": "xml",
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            meta_tag = soup.find("meta", attrs={"name": "citation_pmid"})
            
            if meta_tag and "content" in meta_tag.attrs:
                return meta_tag["content"]
    
    except Exception as e:
        print(e)


def process_documents():
    db = client["telegram_client"]
    collection = db["parsedSites"]
    cnt = 0

    crossref_fetcher = CrossRefFetcher()

    for document in collection.find({}):
        doc_url = document["articleUrl"]
        if "doi" in document and document["doi"]:
            doi = document["doi"]
            try:
                article = crossref_fetcher.article_by_doi(doi)
                
                if article:
                    print(article.publisher)
                    
                    cnt += 1

            except Exception as e:
                print(f"Ошибка при поиске статьи по DOI '{title}': {e}")

        elif "title" in document and document["title"]:
            title = document["title"]
            try:
                if "ncbi" in doc_url:
                    if "pubmed" in title:
                        pmid = doc_url.split("/")[-1]
                    else:
                        pmid = get_pmid_from_page(doc_url)

                    pubmed_fetcher = PubMedFetcher()
                        
                    article = pubmed_fetcher.article_by_pmid(pmid)

                    if article:
                        print(article.journal)
                        cnt += 1
                else:
                    article = crossref_fetcher.article_by_title(title)
                    clean_article_title = clean_title(article.title[0])
                    clean_db_title = clean_title(title)

                    if article and clean_article_title == clean_db_title:
                        print(article.title[0])
                        print(title)
                        print(article.publisher)
                        print(clean_article_title == clean_db_title)
                        cnt += 1
                    break

            except Exception as e:
                print(f"Ошибка при поиске статьи по названию '{title}': {e}")
        print("--------------------")
    
    print(cnt)


if __name__ == "__main__":
    # asyncio.run(main())
    process_documents()
    # get_info_from_ncbi_books_url("https://www.ncbi.nlm.nih.gov/books/n/statpearls/article-33983/")
    # get_pmid_from_page("https://www.ncbi.nlm.nih.gov/books/n/statpearls/article-41376/")