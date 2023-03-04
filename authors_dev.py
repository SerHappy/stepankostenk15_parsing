import os
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import csv
import datetime
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


def get_header(filename) -> list[str]:
    with open(filename, "r", encoding="cp1251") as file:
        reader = csv.reader(file, delimiter=",")
        return next(reader)


def read_from_csv(filename) -> list[list[str]]:
    with open(filename, "r", encoding="cp1251") as file:
        reader = csv.reader(file, delimiter=",")
        csv_data = [item for item in reader]
        return csv_data


def create_csv(filename, data):
    header = ["", datetime.datetime.now(), ""]
    with open(filename, "w", encoding="cp1251") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(header)
        for item in data:
            writer.writerow(item)


def add_headers(filename, headers):
    with open(filename, "w", encoding="cp1251") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(headers)


def add_to_csv(filename, data) -> None:
    with open(filename, "w", encoding="cp1251") as file:
        writer = csv.writer(file, delimiter=",")
        for item in data:
            writer.writerow(item)


def new_iteration_csv(filename, data) -> None:
    header = get_header(filename)
    header.append(str(datetime.datetime.now()))
    header.append("")
    with open(filename, "w", encoding="cp1251") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(header)
        for item in data[1:]:
            writer.writerow(item)


def data_to_csv(filename, data, first_dataframe=False) -> None:
    if not os.path.exists(filename):
        create_csv(filename, data)
    else:
        if first_dataframe == True:
            new_iteration_csv(filename, read_from_csv(filename))

        csv_data = read_from_csv(filename)

        headers_len = len(get_header(filename))
        for data_item in data:
            for i, csv_item in enumerate(csv_data):
                if data_item[0] == csv_item[0]:
                    if len(csv_item) == headers_len - 2:
                        csv_data[i].append(data_item[1])
                        csv_data[i].append(data_item[2])
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
                            data_item[2],
                        ]
                    )
        add_to_csv(filename, csv_data)


def get_item_data(item) -> list[str]:
    item_name = item.find("div", class_="author-item-name").text
    item_li = (
        item.find("div", class_="author-item-stat")
        .find("ul")
        .find_all("li")[:2]
    )
    try:
        item_views = item_li[0].find("span").text
    except:
        item_views = 0
    try:
        item_revs = item_li[1].find("span").text
    except:
        item_revs = 0
    return [item_name, item_views, item_revs]


def get_page_data(url_page) -> list[list[str]]:
    response_page = session.get(url=url_page, headers=headers, verify=False)

    soup = BeautifulSoup(response_page.text, "lxml")

    items = soup.find_all("div", class_="author-item_wrap")

    page_items = []
    i = 0
    for item in items:
        logging.info(f"Item {i+1}/{len(items)}")
        item_data = get_item_data(item)
        if item_data != None:
            page_items.append(item_data)
        else:
            logging.info(f"Item {i+1} skipped")
        i += 1

    return page_items


def get_data(url, start_page=1):
    pages = get_pages_count(url)

    first_page = True if start_page == 1 else False
    for page in tqdm(range(start_page, pages + 1)):
        logging.info(f"Page {page}/{pages}")
        url_page = f"{url}/?PAGEN_1={page}"
        page_data = get_page_data(url_page)
        data_to_csv("csv/authors.csv", page_data, first_page)
        first_page = False


def main():
    start_time = datetime.datetime.now()
    get_data(
        url="https://info-hit.ru/authors/",
    )
    end_time = datetime.datetime.now()
    print("Duration: {}".format(end_time - start_time))


if __name__ == "__main__":
    main()
