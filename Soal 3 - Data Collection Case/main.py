import asyncio
import httpx
from bs4 import BeautifulSoup as bs
from re import search
from tqdm import tqdm
import traceback
import polars as pl
import os
import json

base_url = "https://www.fortiguard.com"
pages = [1, 2, 3, 4, 5]
skipped_pages = {}  # Dictionary to store skipped pages for each level
data = []


async def fetch_page(session, level, page_num) -> any:
    url = f"{base_url}/encyclopedia?type=ips&risk={level}&page={page_num}"
    try:
        response = await session.get(url, timeout=30)  # Set a timeout to prevent hanging requests
        response.raise_for_status()  # Raise an exception for non-200 status codes
        return response
    except httpx.HTTPStatusError as e:
        print(f"Error fetching page {page_num} for level {level}: {e}")
    except httpx.TimeoutException as e:
        print(f"Timeout fetching page {page_num} for level {level}: {e}")
    except httpx.RequestError as e:
        print(f"Request error fetching page {page_num} for level {level}: {e}")
    except httpx.TransportError as e:
        print(f"Transport error fetching page {page_num} for level {level}: {e}")
    except Exception:
        print(f"Unexpected error fetching page {page_num} for level {level}")
        traceback.print_exc()  # Print the traceback
    return None  # Handle other unexpected errors


async def fetch_level(session, level) -> any:
    for page_num in tqdm(range(1, max(pages) + 1), desc=f"Level {level} Pages"):  # tqdm loop
        response = await fetch_page(session, level, page_num)
        if response is None:
            skipped_pages.setdefault(level, []).append(page_num)
            continue

        soup = bs(response.content, "html.parser")
        page_content = soup.find("div", {"class": "page-content"})
        table_body = page_content.find("section", {"class": "table-body"})
        rows = table_body.find_all("div", {"class": "row"})
        for row in rows:
            if row.has_attr("onclick"):
                link = search(r"'/([^']+)'", row["onclick"])
                link = f"{base_url}/{link.group(1)}"
            title = row.find("b").text
            data.append({"level": level, "title": title, "link": link})

        await asyncio.sleep(5)


async def main():
    async with httpx.AsyncClient() as client:
        tasks = [fetch_level(client, level) for level in range(1, 6)]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())

    # Create the directory if it doesn't exist
    output_directory = "datasets"
    os.makedirs(output_directory, exist_ok=True)

    # Write each level's data to a CSV file
    for level in range(1, 6):
        df = pl.DataFrame([d for d in data if d["level"] == level])
        df.write_csv(f"datasets/forti_lists_{level}.csv")

    # Write skipped pages to a JSON file
    if len(skipped_pages) != 0:
        with open("datasets/skipped.json", "w") as f:
            json.dump(skipped_pages, f)

    print("CSV files and skipped pages JSON file (if any) have been created successfully.")
