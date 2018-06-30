import asyncio
import aiohttp
import re
import os
import logging
import async_timeout

from bs4 import BeautifulSoup

LOGGING_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
LOGGING_LEVEL = logging.INFO

CHECK_NEW_TIMEOUT = 60
INIT_URL = 'https://news.ycombinator.com/'
FILENAME_LIMIT = 80
ROOT_FOLDER = 'hasker_news'
IGNORE = ['.pdf', '.jpg']

FETCH_TIMEOUT = 10
MAXIMUM_FETCHES = 5

headers = {
        # 'Host': '',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        # 'Accept-encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }


class Post:
    def __init__(self, post_url, post_title, comments_url):
        self.url = post_url
        self.title = post_title
        self.comments = comments_url
        self.filename = self.get_filename()
        self.folder = os.path.join(ROOT_FOLDER, self.filename)
        self.html = ''

    def get_filename(self):
        uid = self.comments.split('=')[1]
        title = re.sub(r"[^\w\s]", '_', self.title)
        filename = re.sub(r"\s+", '-', title)
        filename = filename.replace('__', '_')
        return uid + '_' + filename[:FILENAME_LIMIT]

    def is_downloaded(self):
        return os.path.exists(self.folder)

    async def load(self):
        self.html = await get_one_page(self.url)

    def save(self):
        os.makedirs(self.folder, exist_ok=True)
        filename = self.filename + '.html'
        full_filename = os.path.join(self.folder, filename)
        with open(full_filename, 'w') as f:
            f.write(self.html)
        logging.info(f'SAVED: {full_filename}')


def clean_filename(filename):
    filename = re.sub(r"[^\w\s]", '_', filename)
    filename = re.sub(r"\s+", '-', filename)
    filename = filename.replace('__', '_')
    return filename[:FILENAME_LIMIT]


def parse_main_page(html: str) -> list:
    """ return list of
        (post_url, post_title, comments_on_post_url)
    """
    top_posts = []
    bsObj = BeautifulSoup(html, "html.parser")
    top_news = bsObj.find_all("a", attrs={"class": "storylink"})
    comment_urls = bsObj.find_all("td", attrs={"class": "subtext"})
    comment_urls = [INIT_URL + comment.find_all("a")[-1]["href"] for comment in comment_urls]
    if len(top_news) != len(comment_urls):
        raise ValueError('Len top news list and len comments list is not equal')
    for post, comment_url in zip(top_news, comment_urls):
        post_url = post['href']
        if post_url.startswith('item'):
            post_url = INIT_URL + post_url
        top_posts.append((post_url, post.text, comment_url))
    return top_posts


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


async def fetch(session, url):
    async with session.get(url) as response:
        try:
            return await response.text()
        except UnicodeDecodeError:
            logging.info(f'ERROR: UnicodeDecodeError in {url}')
            return ''


async def get_one_page(url: str) -> str:
    html = ''
    if is_url_ignored(url):
        return html
    try:
        with async_timeout.timeout(FETCH_TIMEOUT):
            async with aiohttp.ClientSession() as session:
                    html = await fetch(session, url)
    except aiohttp.client_exceptions.ClientConnectorError:
        logging.warning(f'Connection Error with {url}')
    except asyncio.TimeoutError:
        logging.warning(f'Timeout Error with {url}')
    except aiohttp.client_exceptions.ServerDisconnectedError:
        logging.warning(f'Server Disconnected Error with {url}')
    return html


async def check_for_new_posts():
    main_page_html = await get_one_page(INIT_URL)
    top_posts_data = parse_main_page(main_page_html)
    top_posts = [Post(*post_data) for post_data in top_posts_data]
    posts_to_parse = [post for post in top_posts
                      if not post.is_downloaded() and is_url_valid(post.url)]
    return posts_to_parse


async def get_all_urls_from_comment_page(comment_url: str) -> list:
    urls=[]
    html = await get_one_page(comment_url)
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


def get_title(html):
    bsObj = BeautifulSoup(html, "html.parser")
    title = bsObj.title.text if bsObj.title else None
    return title


def save_page(html: str, filename: str, folder):
    filename = filename + '.html'
    filename = os.path.join(folder, filename)
    with open(filename, 'w') as f:
        f.write(html)
    logging.info(f'SAVED: {filename}')


async def load_url_from_comment(url, folder):
    logging.info('TASK LOAD URL FROM COMMENT {}'.format(url))
    html = await get_one_page(url)
    if not html:
        return
    title = get_title(html)
    if not title:
        return
    sub_filename = clean_filename(title)
    save_page(html, sub_filename, folder=folder)


async def parse_one(post: Post):
    logging.info(f'Coro: {post.url}: PARSE_ONE ')
    await post.load()
    logging.info(f'Coro: {post.url}: LOADED ')
    post.save()
    logging.info(f'Coro: {post.url}: SAVED ')

    urls_from_comment_page = await get_all_urls_from_comment_page(post.comments)
    logging.info(f'Coro: {post.url}: COMMENT URLS {len(urls_from_comment_page)}')
    tasks = [asyncio.ensure_future(load_url_from_comment(url, post.folder))
             for url in urls_from_comment_page]
    try:
        await asyncio.wait(tasks)
    except ValueError:
        return


async def parse_all(queue):
    logging.info('PARSE ALL: Get new urls')
    posts = await check_for_new_posts()
    if not posts:
        logging.info('PARSE ALL: no urls')
    else:
        logging.info('PARSE ALL: starting parse_one on urls: {}'.format(len(posts)))
        tasks = [asyncio.ensure_future(parse_one(post)) for post in posts]
        await asyncio.wait(tasks)
        logging.info('ending parse urls')


async def run_forever(queue):
    while True:
        tasks = [asyncio.ensure_future(parse_all(queue))]
        await asyncio.wait(tasks)
        queue.join()
        await asyncio.sleep(CHECK_NEW_TIMEOUT)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, datefmt='%Y.%m.%d %H:%M:%S', level=LOGGING_LEVEL)
    os.makedirs(ROOT_FOLDER, exist_ok=True)
    ioloop = asyncio.get_event_loop()
    queue = asyncio.Queue()
    try:
        logging.info('run parse all coro')
        # task = ioloop.create_task(parse_all(ioloop))
        # ioloop.run_until_complete(task)
        ioloop.create_task(run_forever(queue))
        ioloop.run_forever()
        logging.info('exit from async coroutines')
    except KeyboardInterrupt:
        logging.info('KeyboardInterrupt, exiting')
    except Exception as e:
        logging.exception(f"Unexpected exception: {e}")
    finally:
        ioloop.close()
