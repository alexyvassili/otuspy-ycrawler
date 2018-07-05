import re
import os
import io
import logging
import asyncio
import async_timeout
import aiohttp

from bs4 import BeautifulSoup

INIT_URL = 'https://news.ycombinator.com/'
ROOT_FOLDER = 'haker_news'
IGNORE = ['.pdf', '.jpg']
FETCH_TIMEOUT = 10
FILENAME_LIMIT = 80


class Page:
    def __init__(self, url):
        self.url = url
        self.html = ''

    def is_url_ignored(self):
        for ignore_suffix in IGNORE:
            if self.url.endswith(ignore_suffix):
                return True
        return False

    def is_url_valid(self):
        if self.url.startswith('http://') or self.url.startswith('https://'):
            return True
        else:
            return False

    async def fetch(self, session):
        async with session.get(self.url) as response:
            try:
                return await response.text()
            except UnicodeDecodeError:
                logging.info(f'ERROR: UnicodeDecodeError in {self.url}')
                return ''

    async def get(self) -> str:
        html = ''
        if self.is_url_ignored():
            return html
        try:
            with async_timeout.timeout(FETCH_TIMEOUT):
                async with aiohttp.ClientSession() as session:
                    html = await self.fetch(session)
        except aiohttp.client_exceptions.ClientConnectorError:
            logging.warning(f'Connection Error with {self.url}')
        except asyncio.TimeoutError:
            logging.warning(f'Timeout Error with {self.url}')
        except aiohttp.client_exceptions.ServerDisconnectedError:
            logging.warning(f'Server Disconnected Error with {self.url}')
        return html

    async def load(self):
        self.html = await self.get()

    async def fetch_w_chunks(self, session):
        f = io.BytesIO()
        try:
            async with session.get(self.url) as response:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
            return f.getvalue().decode()
        except UnicodeDecodeError:
            logging.info(f'ERROR: UnicodeDecodeError in {self.url}')
            return ''

    @staticmethod
    def clean_filename(filename):
        filename = re.sub(r"[^\w\s]", '_', filename)
        filename = re.sub(r"\s+", '-', filename)
        filename = filename.replace('__', '_')
        return filename[:FILENAME_LIMIT]

    def save(self, filename: str, folder: str):
        filename = filename + '.html'
        filename = os.path.join(folder, filename)
        with open(filename, 'w') as f:
            f.write(self.html)
        logging.info(f'SAVED: {filename}')


class PageFromComment(Page):
    def __init__(self, page_url, page_folder):
        self.url = page_url
        self.title = ''
        self.folder = page_folder
        self.html = ''

    @staticmethod
    def get_title(html):
        bsObj = BeautifulSoup(html, "html.parser")
        title = bsObj.title.text if bsObj.title else None
        return title

    def save(self):
        logging.info('LOAD TITLE FROM COMMENT {}'.format(self.url))
        if not self.html:
            return
        self.title = self.get_title(self.html)
        if not self.title:
            return
        sub_filename = self.clean_filename(self.title)
        logging.info('SAVE COMMENT {} / {}'.format(self.folder, sub_filename))
        super().save(sub_filename, folder=self.folder)


class Post(Page):
    def __init__(self, post_url, post_title, comments_url):
        self.url = post_url
        self.title = post_title
        self.comments = comments_url
        self.filename = self.get_filename()
        self.folder = os.path.join(ROOT_FOLDER, self.filename)
        self.html = ''

    def get_filename(self):
        uid = self.comments.split('=')[1]
        filename = self.clean_filename(uid + '_' + self.title)
        return filename

    def is_downloaded(self):
        return os.path.exists(self.folder)

    async def get_all_urls_from_comment_page(self) -> list:
        urls = []
        comments_page = Page(self.comments)
        await comments_page.load()
        html = comments_page.html
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

    def save(self):
        os.makedirs(self.folder, exist_ok=True)
        super().save(self.filename, self.folder)


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
