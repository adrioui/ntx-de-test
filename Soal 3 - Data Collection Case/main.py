import asyncio
import json
from typing import List, Tuple
from bs4 import BeautifulSoup
import httpx
import polars as pl
from tqdm import tqdm

base_url = "https://www.fortiguard.com/encyclopedia?type=ips&risk={}&page={}"

max_pages = [10, 10, 10, 10, 10]  # Define the maximum pages for each level


async def fetch_page_content(session: httpx.AsyncClient, level: int, page: int) -> str:
    url = base_url.format(level, page)
    try:
        response = await session.get(url, timeout=30)
        response.raise_for_status()
        return response.text
    except (httpx.HTTPStatusError, httpx.TimeoutException) as exc:
        print(f"Failed to fetch {url}: {exc}")
        return None


def extract_data(html_content: str) -> List[Tuple[str, str]]:
    data = []
    soup = BeautifulSoup(html_content, "html.parser")
    for article in soup.select(".table-row"):
        title = article.select_one(".table-link").text.strip()
        link = article.select_one(".table-link")["href"]
        data.append((title, link))
    return data


async def fetch_and_extract(session: httpx.AsyncClient, level: int, page: int) -> List[Tuple[str, str]]:
    html_content = await fetch_page_content(session, level, page)
    if html_content is not None:
        return extract_data(html_content)
    else:
        return []


async def scrape_level(session: httpx.AsyncClient, level: int, max_page: int) -> Tuple[
    List[Tuple[str, str]], List[int]]:
    tasks = [fetch_and_extract(session, level, page) for page in range(1, max_page + 1)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    data = []
    skipped_pages = []
    for page, result in enumerate(results, start=1):
        if isinstance(result, Exception):
            print(f"Skipping page {page} for level {level} due to an exception")
            skipped_pages.append(page)
        else:
            data.extend(result)

    return data, skipped_pages


async def main():
    async with httpx.AsyncClient() as session:
        for level, max_page in enumerate(max_pages, start=1):
            print(f"Scraping Level {level}...")
            data, skipped_pages = await scrape_level(session, level, max_page)

            df = pl.DataFrame(data, columns=["title", "link"])
            df.write_csv(f"datasets/forti_lists_{level}.csv")

            with open(f"datasets/skipped.json", "w") as f:
                json.dump({"level": level, "skipped_pages": skipped_pages}, f)
            print(f"Level {level} scraping completed.")


if __name__ == "__main__":
    asyncio.run(main())
