import asyncio
import logging
import random
from pathlib import Path
import platform
import threading

import aiohttp
import aiofiles
import pandas as pd
from aiohttp import ClientSession

DATA_HOME = Path('data')
CSV_10_Q_PATH = DATA_HOME / 'metadata_10-Q_2023to2003_cleaned_onlyhtm_with_company_details.csv.gz'
HTML_OUTPUT = DATA_HOME / 'html'
HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "User-Agent": "company01, joe@company01.com",
    "Host": 'www.sec.gov',
}
NB_DOWNLOADER = 8

LOGGING_HOME = Path('log')
HTTP_LOG_FILE = LOGGING_HOME / 'http.log'


def setup_logging():
    http_logger = logging.getLogger('http')
    http_logger.setLevel(logging.INFO)

    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(HTTP_LOG_FILE)

    c_handler.setLevel(logging.WARNING)
    f_handler.setLevel(logging.ERROR)

    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    http_logger.addHandler(c_handler)
    http_logger.addHandler(f_handler)


def backoff_retry_approach(
    initial_delay: float = .1,
    exponential_base: float = 2.,
    jitter: bool = True,
    max_retries: int = 10,
):
    http_logger = logging.getLogger('http')

    def real_decorator(func):
        async def wrapper(*args, **kwargs):
            num_retries = 0
            delay = initial_delay
            while True:
                try:
                    return await func(*args, **kwargs)
                except aiohttp.ClientResponseError as e:
                    if e.status != 429:
                        raise
                    num_retries += 1
                    if num_retries > max_retries:
                        raise
                    delay *= exponential_base * (1 + jitter * random.random())
                    http_logger.info(f'[429] retry - {num_retries}, url - {e.request_info.url}, dur - {delay}')
                    await asyncio.sleep(delay)
        return wrapper

    return real_decorator


@backoff_retry_approach()
async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    resp = await session.get(url=url, headers=HEADERS, **kwargs)
    resp.raise_for_status()
    html = await resp.text()
    return html


async def write_one(idx: int, content: str):
    filepath = HTML_OUTPUT / f'{idx: 07d}.html'
    async with aiofiles.open(filepath, "w") as fp:
        await fp.write(content)


def extract_filings(df: pd.DataFrame, req_queue: asyncio.Queue):

    def callback(row: pd.Series):
        link = row['linkToFilingDetails']
        link = link.replace("https://www.sec.gov/ix?doc=/", "https://www.sec.gov/")
        req_queue.put_nowait((row.name, link))

    df.apply(callback, axis=1)


async def download_filings(req_queue: asyncio.Queue, session: ClientSession):
    while True:
        idx, url = await req_queue.get()
        html_content = await fetch_html(url=url, session=session)
        await write_one(idx, html_content)
        print(f'\r{req_queue.qsize(): 7d}: {idx: 7d} - {url}                     ', end='\r')
        req_queue.task_done()


async def main():
    df_10_q = pd.read_csv(CSV_10_Q_PATH, compression='gzip', low_memory=False)
    req_queue = asyncio.Queue()

    extracting_thread = threading.Thread(target=extract_filings, args=(df_10_q, req_queue))
    extracting_thread.start()

    async with ClientSession(headers=HEADERS) as session:
        downloaders = [
            asyncio.create_task(download_filings(req_queue, session)) for _ in range(NB_DOWNLOADER)
        ]
        extracting_thread.join()
        await req_queue.join()

    for downloader in downloaders:
        downloader.cancel()

    print()

    return 0


if __name__ == "__main__":
    HTML_OUTPUT.mkdir(exist_ok=True, parents=True)

    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    setup_logging()
    asyncio.run(main())
