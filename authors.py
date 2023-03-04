import os
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from anti_useragent import UserAgent
import csv
import datetime
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


def get_pages_count(home_url: str) -> int:
    """Get pages count"""

    response = session.get(url=home_url, headers=headers, verify=False)

    soup = BeautifulSoup(response.text, "lxml")

    navigation_panel = soup.find("div", class_="navigation")
    navigation_titles = navigation_panel.find_all("a")
    return int(navigation_titles[-2].text)


def get_headers(filename: str) -> list[str]:
    """Get csv file header row"""

    with open(filename, "r", encoding="Windows-1251", newline="") as file:
        reader = csv.reader(file, delimiter=",")
        return next(reader)


def add_headers(filename: str, headers: list[str]) -> None:
    """Add headers to csv file"""

    with open(filename, "w", encoding="Windows-1251", newline="") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(headers)


def get_csv(filename: str) -> list[list[str]]:
    """Get all data from csv"""

    with open(filename, "r", encoding="Windows-1251", newline="") as file:
        reader = csv.reader(file, delimiter=",")
        csv_data = [item for item in reader]
        return csv_data


def create_csv(filename: str, data: list[str]) -> None:
    """Create csv file with data"""

    header = ["", datetime.datetime.now(), ""]
    with open(filename, "w", encoding="Windows-1251", newline="") as file:
        writer = csv.writer(file, delimiter=",")
        writer.writerow(header)
        for item in data:
            writer.writerow(item)


def add_to_csv(filename: str, data: list[str]) -> None:
    """Add data to csv file"""

    with open(filename, "w", encoding="Windows-1251", newline="") as file:
        writer = csv.writer(file, delimiter=",")
        for item in data:
            writer.writerow(item)


def new_iteration_csv(filename: str, data: list[str]) -> None:
    """Add new headers to csv at new parser iteration"""

    header = get_headers(filename)
    header.append(str(datetime.datetime.now()))
    header.append("")
    with open(filename, "w", encoding="Windows-1251", newline="") as file:
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
    """Get item data"""

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


def get_page_data(url_page: str) -> list[list[str]]:
    """Get data of all items on page"""

    response_page = session.get(url=url_page, headers=headers, verify=False)

    soup = BeautifulSoup(response_page.text, "lxml")

    items = soup.find_all("div", class_="author-item_wrap")

    page_items = []
    i = 0
    for item in items:
        item_data = get_item_data(item)
        if item_data != None:
            page_items.append(item_data)
        i += 1

    return page_items


def gather_data(url: str, csv_filename: str) -> None:
    """Gather all data"""

    pages = get_pages_count(url)

    first_page = True
    for page in tqdm(range(1, pages + 1)):
        url_page = f"{url}/?PAGEN_1={page}"
        page_data = get_page_data(url_page)
        data_to_csv(csv_filename, page_data, first_page)
        first_page = False


def main() -> None:
    """Main function"""

    gather_data(
        url="https://info-hit.ru/authors/",
        csv_filename="E:/authors.csv",
    )


if __name__ == "__main__":
    main()
