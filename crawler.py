import asyncio
import aiohttp
import re
import os
import requests
import logging

from time import sleep
from bs4 import BeautifulSoup

LOGGING_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
LOGGING_LEVEL = logging.INFO

TIMEOUT = 5
INIT_URL = 'https://news.ycombinator.com/'
FILENAME_LIMIT = 80
ROOT_FOLDER = 'hasker_news'
IGNORE = ['.pdf', '.jpg']

headers = {
        # 'Host': '',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        # 'Accept-encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }


def check_folder(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)


def get_first_page(url: str) -> str:
    session = requests.Session()
    s = session.get(INIT_URL, headers=headers)
    return s.text


def get_top_urls_from_first_page(html: str) -> list:
    urls_and_titles = []
    bsObj = BeautifulSoup(html, "html.parser")
    top_news = bsObj.find_all("a", attrs={"class": "storylink"})
    comment_urls = bsObj.find_all("td", attrs={"class": "subtext"})
    comment_urls = [INIT_URL + comment.find_all("a")[-1]["href"] for comment in comment_urls]
    if len(top_news) != len(comment_urls):
        raise ValueError('Len top news list and len comments list is not equal')
    for n, url in zip(top_news, comment_urls):
        urls_and_titles.append( (n['href'], n.text, url))
    return urls_and_titles


async def get_all_urls_from_comment_page(comment_url: str) -> list:
    urls=[]
    html = await aget_one_page(comment_url)
    bsObj = BeautifulSoup(html, "html.parser")
    comments_raw = bsObj.find_all("span", attrs={"class": "c00"})
    for comment in comments_raw:
        if not comment.a:
            continue
        href = comment.a["href"]
        # this if because we has broken html and no span closed tag
        # and reply links adds to comments_raw
        if 'reply' in href:
            continue
        urls.append(href)
    return urls


def get_file_name_from_title(title: str) -> str:
    title = re.sub(r"[^\w\s]", '_', title)
    filename = re.sub(r"\s+", '-', title)
    filename = filename.replace('__', '_')
    return filename[:FILENAME_LIMIT]


def save_page(html: str, filename: str, folder=''):
    filename = filename + '.html'
    if folder:
        filename = os.path.join(folder, filename)
    else:
        filename = os.path.join(ROOT_FOLDER, filename)
    with open(filename, 'w') as f:
        f.write(html)
    logging.info(f'SAVED: {filename}')


async def fetch(session, url):
    async with session.get(url) as response:
        try:
            return await response.text()
        except UnicodeDecodeError:
            logging.info(f'ERROR: UnicodeDecodeError in {url}')
            return ''


async def aget_one_page(url: str) -> str:
    if is_url_ignored(url):
        return ''
    async with aiohttp.ClientSession() as session:
        html = await fetch(session, url)
    return html


def get_title(html):
    bsObj = BeautifulSoup(html, "html.parser")
    title = bsObj.title.text if bsObj.title else None
    return title


async def parse_one(url: str, title, comment_url):
    prefix = f'Coro: {url}: PARSE_ONE '
    logging.info(f'{prefix}')
    html = await aget_one_page(url)
    if not html:
        return
    get_title(html)
    filename = get_file_name_from_title(title)

    sub_folder = os.path.join(ROOT_FOLDER, filename)
    check_folder(sub_folder)
    save_page(html, filename, folder=sub_folder)
    urls_from_comment_page = await get_all_urls_from_comment_page(comment_url)

    for sub_url in urls_from_comment_page:
        sub_html = await aget_one_page(sub_url)
        if not sub_html:
            continue
        sub_title = get_title(sub_html)
        if not sub_title:
            continue
        sub_filename = get_file_name_from_title(sub_title)
        save_page(sub_html, sub_filename, folder=sub_folder)


async def parse_top_urls(top_urls_and_titles: list):
    tasks = [asyncio.ensure_future(parse_one(url, title, comment_url)) for url, title, comment_url in top_urls_and_titles]
    logging.info('Creating tasks. Await')
    await asyncio.wait(tasks)
    logging.info('Parse Done')


def is_url_ignored(url):
    for ignore_suffix in IGNORE:
        if url.endswith(ignore_suffix):
            return True
    return False


def is_url_valid(url:str):
    if url.startswith('http://') or url.startswith('https://'):
        return True
    else:
        return False


def check_top_urls(urls: list):
    filtered_urls = []
    for item in urls:
        url = item[0]
        title = item[1]
        if is_url_ignored(url) or not is_url_valid(url):
            continue
        folder = get_file_name_from_title(title)
        folder = os.path.join(ROOT_FOLDER, folder)
        if os.path.exists(folder):
            continue
        filtered_urls.append(item)
    return filtered_urls


if __name__ == "__main__":
    logging.basicConfig(format=LOGGING_FORMAT, datefmt='%Y.%m.%d %H:%M:%S', level=LOGGING_LEVEL)
    check_folder(ROOT_FOLDER)
    ioloop = asyncio.get_event_loop()
    try:
        while True:
            first_page_html = get_first_page(INIT_URL)
            top_urls_and_titles = get_top_urls_from_first_page(first_page_html)
            urls_to_parse = check_top_urls(top_urls_and_titles)
            if not urls_to_parse:
                logging.info('No new links, timeout')
                sleep(TIMEOUT)
                continue
            logging.info('run async coroutines')
            ioloop.run_until_complete(parse_top_urls(urls_to_parse))
            logging.info('exit from async coroutines')
    except KeyboardInterrupt:
        logging.info('KeyboardInterrupt, exiting')
    except Exception as e:
        logging.exception(f"Unexpected exception: {e}")
    finally:
        ioloop.close()
