import asyncio
from asyncio import Semaphore
from pprint import pprint

from bs4 import BeautifulSoup
from httpx import AsyncClient, Response

from .config import HEADERS, COOKIES
from .utils import get_links, get_data


async def main():
    semaphore = Semaphore(50)
    async with AsyncClient(headers=HEADERS, cookies=COOKIES, timeout=60000, http2=True) as session:
        links = await get_links(semaphore, session, 'https://www.alta.ru/rois/all/')
        tasks = []
        for page in range(2, 352):
            tasks.append(asyncio.create_task(get_links(semaphore, session, f'https://www.alta.ru/rois/page_{page}/')))

        raw_links = await asyncio.gather(*tasks)
        raw_links = sum(raw_links, [])
        links.extend(raw_links)
        tasks = [asyncio.create_task(get_data(semaphore, session, link)) for link in links]
        await asyncio.gather(*tasks)
