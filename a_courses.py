import os
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import csv
import datetime
import re
import logging
import asyncio
import aiohttp

from urllib3 import Retry

ua = UserAgent()
headers = {
    "User-Agent": ua.random,
    "Accept": "*/*",
}
# session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
# session.mount("http://", adapter)
# session.mount("https://", adapter)
requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)
logging.basicConfig(level=logging.INFO)


async def get_pages_count(session, home_url):
    response = await session.get(url=home_url, headers=headers)
    soup = BeautifulSoup(await response.text(), "lxml")

    navigation_panel = soup.find("div", class_="navigation")
    navigation_titles = navigation_panel.find_all("a")
    return int(navigation_titles[-2].text)


async def get_page_data(session, url_page):
    async with session.get(url=url_page, headers=headers) as response_page:
        soup = BeautifulSoup(await response_page.text(), "lxml")

        items = soup.find_all("div", class_="catalog__item")
        if len(items) == 0:
            items = soup.find_all("div", class_="courses-cards__list__item")

        page_items = []
        i = 0
        tasks = []
        for item in items:
            logging.info(f"Item {i+1}/{len(items)}, {url_page}")
            item_url = item.find("a", class_="catalog__item__link")
            task = asyncio.create_task(get_item_data(session, item, item_url))
            tasks.append(task)
            # item_data = await task
            # if item_data != None:
            #     print("Added", item_data)
            #     page_items.append(item_data)
            # else:
            #     logging.info(f"Item {i+1} skipped")
            i += 1
        await asyncio.gather(*tasks)
        return page_items


async def get_item_data(session, item, item_url):
    if item_url == None:
        item_url = item.find("a", class_="course-card__wrap")
    item_url = item_url.get("href")
    if "away.php" in item_url:
        return None

    async with session.get(
        url=f"https://info-hit.ru{item_url}", headers=headers
    ) as response_item:
        soup = BeautifulSoup(await response_item.text(), "lxml")

        try:
            weekly_views = re.search(
                r"\d+",
                soup.find("span", class_="cp-hero__rating-text").text,
            ).group()
        except Exception as e:
            return None

        return [item_url, weekly_views]


async def gather_data():
    async with aiohttp.ClientSession() as session:
        tasks = []
        url = "https://info-hit.ru/catalog"
        pages = await get_pages_count(session, url)
        start_page = 1
        first_page = True if start_page == 1 else False

        tasks = []
        for page in range(start_page, pages + 1):
            logging.info(f"Page {page}/{pages}")
            url_page = f"{url}/?PAGEN_1={page}"
            task = asyncio.create_task(get_page_data(session, url_page))
            tasks.append(task)
            # page_data = await task
            # data_to_csv("csv/courses.csv", page_data, first_page)
            first_page = False

        await asyncio.gather(*tasks)


def main():
    asyncio.run(gather_data())


if __name__ == "__main__":
    main()
