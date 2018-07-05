import asyncio
import os
import logging

from pages import Page, Post, PageFromComment, parse_main_page
from pages import INIT_URL, ROOT_FOLDER

LOGGING_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
LOGGING_LEVEL = logging.INFO

CHECK_NEW_TIMEOUT = 60
MAXIMUM_FETCHES = 5

headers = {
        # 'Host': '',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        # 'Accept-encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }


async def check_for_new_posts(queue: asyncio.Queue):
    while True:
        logging.info(f'CHECK NEWS: MAIN PAGE ')
        main_page = Page(INIT_URL)
        logging.info(f'CHECK NEWS: MAIN PAGE LOADING {main_page.url}')
        await main_page.load()
        logging.info(f'CHECK NEWS: GET POSTS ')
        top_posts_data = parse_main_page(main_page.html)
        # print(top_posts_data)
        top_posts = [Post(*post_data) for post_data in top_posts_data]
        posts_to_parse = [post for post in top_posts
                          if not post.is_downloaded() and post.is_url_valid()]
        logging.info(f'CHECK NEWS: PUTTING NEW POSTS TO QUEUE')
        # print([post.url for post in posts_to_parse])
        for post in posts_to_parse:
            queue.put_nowait(post)
        logging.info(f'CHECK NEWS: SLEEP')
        await asyncio.sleep(CHECK_NEW_TIMEOUT)


async def post_worker(post_queue: asyncio.Queue, comments_queue:asyncio.Queue):
    while True:
        logging.info(f'POST WORKER: EXPECTNG POST ')
        post = await post_queue.get()
        logging.info(f'POST WORKER: GET POST {post.url} ')
        await post.load()
        logging.info(f'POST WORKER: GET POST COMMENTS URLS {post.url} ')
        comments_urls = await post.get_all_urls_from_comment_page()
        logging.info(f'POST WORKER: SAVE POST {post.url} ')
        post.save()
        logging.info(f'POST WORKER: PUT COMMENTS TO QUEUE: {len(comments_urls)} {post.url} ')
        for url in comments_urls:
            comments_queue.put_nowait(PageFromComment(url, post.folder))
        logging.info(f'POST WORKER: TASK DONE {post.url} ')
        post_queue.task_done()


async def comment_worker(comments_queue: asyncio.Queue):
    while True:
        logging.info(f'COMMENT WORKER: EXPECTING COMMENT ')
        page = await comments_queue.get()
        logging.info(f'COMMENT WORKER: LOAD COMMENT {page.url} ')
        await page.load()
        logging.info(f'COMMENT WORKER: SAVE COMMENT {page.url} ')
        page.save()
        logging.info(f'COMMENT WORKER: TASK DONE {page.url} ')
        comments_queue.task_done()


async def run_forever(post_queue, comments_queue):
    tasks = [asyncio.ensure_future(check_for_new_posts(post_queue)),
             asyncio.ensure_future(post_worker(post_queue, comments_queue)),
             asyncio.ensure_future(comment_worker(comments_queue))
             ]
    await asyncio.wait(tasks)
    post_queue.join()
    comments_queue.join()


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, datefmt='%Y.%m.%d %H:%M:%S', level=LOGGING_LEVEL)
    os.makedirs(ROOT_FOLDER, exist_ok=True)
    ioloop = asyncio.get_event_loop()
    post_queue = asyncio.Queue()
    comments_queue = asyncio.Queue()
    try:
        logging.info('run parse all coro')
        ioloop.create_task(run_forever(post_queue, comments_queue))
        ioloop.run_forever()
        logging.info('exit from async coroutines')
    except KeyboardInterrupt:
        logging.info('KeyboardInterrupt, exiting')
    except Exception as e:
        logging.exception(f"Unexpected exception: {e}")
    finally:
        ioloop.close()
