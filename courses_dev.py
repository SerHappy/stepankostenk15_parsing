import os
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import csv
import datetime
import re
import logging
from tqdm import tqdm

from urllib3 import Retry

ua = UserAgent()
headers = {
    "User-Agent": ua.random,
    "Accept": "*/*",
}
session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)
requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)
logging.basicConfig(level=logging.INFO)


def get_pages_count(home_url) -> int:
    response = session.get(url=home_url, headers=headers, verify=False)

    soup = BeautifulSoup(response.text, "lxml")

    navigation_panel = soup.find("div", class_="navigation")
    navigation_titles = navigation_panel.find_all("a")
    return int(navigation_titles[-2].text)


def get_headers(filename: str) -> list[str]:
    """Get csv file header row"""

    with open(filename, "r", encoding="cp1251") as file:
        reader = csv.reader(file, delimiter=",")
        return next(reader)


def add_headers(filename: str, headers: list[str]) -> None:
    """Add headers to csv file"""

    with open(filename, "w", encoding="cp1251") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(headers)


def get_csv(filename: str) -> list[list[str]]:
    """Get all data from csv"""

    with open(filename, "r", encoding="cp1251") as file:
        reader = csv.reader(file, delimiter=",")
        csv_data = [item for item in reader]
        return csv_data


def create_csv(filename: str, data: list[str]) -> None:
    """Create csv file with data"""

    header = ["", datetime.datetime.now()]
    with open(filename, "w", encoding="cp1251") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(header)
        for item in data:
            writer.writerow(item)


def add_to_csv(filename: str, data: list[str]) -> None:
    """Add data to csv file"""

    with open(filename, "w", encoding="cp1251") as file:
        writer = csv.writer(file, delimiter=",")
        for item in data:
            writer.writerow(item)


def new_iteration_csv(filename: str, data: list[str]) -> None:
    """Add new headers to csv at new parser iteration"""

    header = get_headers(filename)
    header.append(str(datetime.datetime.now()))
    with open(filename, "w", encoding="cp1251") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(header)
        for item in data[1:]:
            writer.writerow(item)


def data_to_csv(
    filename: str, data: list[str], first_dataframe: bool = False
) -> None:
    """Add data to csv file or create it"""

    if not os.path.exists(filename):
        create_csv(filename, data)
    else:
        if first_dataframe == True:
            new_iteration_csv(filename, get_csv(filename))

        csv_data = get_csv(filename)

        headers_len = len(get_headers(filename))
        for data_item in data:
            for i, csv_item in enumerate(csv_data):
                if data_item[0] == csv_item[0]:
                    if len(csv_item) == headers_len - 1:
                        csv_data[i].append(data_item[1])
                    break
            else:
                if headers_len == len(data_item):
                    csv_data.append(data_item)
                elif headers_len > len(data_item):
                    csv_data.append(
                        [
                            data_item[0],
                            *[""] * (headers_len - len(data_item)),
                            data_item[1],
                        ]
                    )
                csv_data.append(data_item)
        add_to_csv(filename, csv_data)


def get_item_data(item, item_url: str) -> list[str] | None:
    """Get item data or None"""

    if item_url == None:
        item_url = item.find("a", class_="course-card__wrap")
    item_url = item_url.get("href")
    if "away.php" in item_url:
        return None

    response_item = session.get(
        url=f"https://info-hit.ru{item_url}", headers=headers, verify=False
    )

    soup = BeautifulSoup(response_item.text, "lxml")

    try:
        weekly_views = re.search(
            r"\d+",
            soup.find("span", class_="cp-hero__rating-text").text,
        ).group()
    except Exception as e:
        return None

    return [item_url, weekly_views]


def get_page_data(url_page: str) -> list[list[str]]:
    """Get data of all items on page"""

    response_page = session.get(url=url_page, headers=headers, verify=False)
    soup = BeautifulSoup(response_page.text, "lxml")

    items = soup.find_all("div", class_="catalog__item")
    if len(items) == 0:
        items = soup.find_all("div", class_="courses-cards__list__item")

    page_items = []
    i = 0
    for item in items:
        logging.info(f"Item {i+1}/{len(items)}")
        item_url = item.find("a", class_="catalog__item__link")
        item_data = get_item_data(item, item_url)
        if item_data != None:
            page_items.append(item_data)
        else:
            logging.info(f"Item {i+1} skipped")
        i += 1

    return page_items


def gather_data(url: str) -> None:
    """Gather all data"""

    pages = get_pages_count(url)

    is_first_page = True
    for page in tqdm(range(1, pages + 1)):
        logging.info(f"Page {page}/{pages}")
        url_page = f"{url}/?PAGEN_1={page}"
        page_data = get_page_data(url_page)
        if len(page_data) > 0:
            data_to_csv("csv/courses.csv", page_data, is_first_page)
        is_first_page = False


def main() -> None:
    """Main function"""

    start_time = datetime.datetime.now()
    gather_data(url="https://info-hit.ru/catalog")
    end_time = datetime.datetime.now()
    print("Duration: {}".format(end_time - start_time))


if __name__ == "__main__":
    main()
