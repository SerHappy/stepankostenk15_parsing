import os
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import csv
import datetime
import logging

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


def get_scladchins_urls(url: str) -> list[str]:
    """Get all scladnins urls"""

    response = session.get(url=url, headers=headers, verify=False)

    soup = BeautifulSoup(response.text, "lxml")

    scladchins_all = soup.find("li", class_="node category level_1 node_46")
    scladchins_ol = scladchins_all.find("ol", class_="nodeList")
    scladchins_li = scladchins_ol.find_all("li")

    scladchins_urls = []
    for li in scladchins_li:
        li_title = li.find("h3", class_="nodeTitle")
        scladchina_href = li_title.find("a").get("href")
        scladchins_urls.append(scladchina_href)

    return scladchins_urls


def get_threads_pages_count(url: str) -> int:
    """Get thread data"""

    response = session.get(url=url, headers=headers, verify=False)

    soup = BeautifulSoup(response.text, "lxml")
    try:
        navigation_panel = soup.find("div", class_="PageNav").find("nav")
        navigation_titles = navigation_panel.find_all("a")
    except:
        return 1
    return int(navigation_titles[-2].text)


def get_headers(filename: str) -> list[str]:
    """Get csv file header row"""

    with open(filename, "r") as file:
        reader = csv.reader(file)
        return next(reader)


def add_headers(filename: str, headers: list[str]) -> None:
    """Add headers to csv file"""

    with open(filename, "w") as file:
        writer = csv.writer(file)
        writer.writerow(headers)


def get_csv(filename: str) -> list[list[str]]:
    """Get all data from csv"""

    with open(filename, "r") as file:
        reader = csv.reader(file)
        csv_data = [item for item in reader]
        return csv_data


def create_csv(filename: str, data: list[str]) -> None:
    """Create csv file with data"""

    header = ["", datetime.datetime.now(), ""]
    with open(filename, "w") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        for item in data:
            writer.writerow(item)


def add_to_csv(filename: str, data: list[str]) -> None:
    """Add data to csv file"""

    with open(filename, "w") as file:
        writer = csv.writer(file)
        for item in data:
            writer.writerow(item)


def new_iteration_csv(filename: str, data: list[str]) -> None:
    """Add new headers to csv at new parser iteration"""

    header = get_headers(filename)
    header.append(str(datetime.datetime.now()))
    header.append("")
    with open(filename, "w") as file:
        writer = csv.writer(file)
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


def get_thread_data(item) -> list[str]:
    """Get thread data"""

    thread_data_name = item.find("a").get("href")

    thread_data_row = item.find("div", class_="listBlock stats pairsJustified")

    try:
        thread_data_scladniks = (
            thread_data_row.find("dl", class_="major").find("dd").text
        )
    except:
        thread_data_scladniks = 0
    try:
        thread_data_views = (
            thread_data_row.find("dl", class_="minor").find("dd").text
        )
    except:
        thread_data_views = 0

    return [thread_data_name, thread_data_scladniks, thread_data_views]


def gather_scladchina_data(url_scladchina: str, is_first_page: bool):
    """Gather all scladchins data"""

    sclanchina_pages_count = get_threads_pages_count(url_scladchina)

    for page_number in range(1, sclanchina_pages_count + 1):
        logging.info(f"Scladchina page {page_number}/{sclanchina_pages_count}")
        sclanchina_url = f"{url_scladchina}page-{page_number}"

        response = session.get(
            url=sclanchina_url, headers=headers, verify=False
        )

        soup = BeautifulSoup(response.text, "lxml")

        threads = []
        for thread in soup.find_all("li", class_="discussionListItem"):
            if thread.find(class_="sticky"):
                continue
            threads.append(thread)
        scladchina_threads = []
        i = 0
        for thread in threads:
            logging.info(f"Item {i+1}/{len(threads)}")
            item_data = get_thread_data(thread)
            if item_data != None:
                scladchina_threads.append(item_data)
            else:
                logging.info(f"Item {i+1} skipped")
            i += 1

        data_to_csv("csv/scladchina.csv", scladchina_threads, is_first_page)
        is_first_page = False


def gather_data(url: str) -> None:
    """Gather all data"""

    scladchins = get_scladchins_urls(url)

    scladchins_count = len(scladchins)
    is_first_page = True
    for i, scladchina in enumerate(scladchins, start=1):
        logging.info(f"Scladchina {i}/{scladchins_count}")
        url_scladchina = f"{url}/{scladchina}"
        gather_scladchina_data(url_scladchina, is_first_page)
        is_first_page = False


def main() -> None:
    """Main function"""

    start_time = datetime.datetime.now()
    gather_data(url="https://s107.skladchina.biz/")
    end_time = datetime.datetime.now()
    logging.info("Duration: {}".format(end_time - start_time))


if __name__ == "__main__":
    main()
