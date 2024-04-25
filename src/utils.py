from asyncio import Semaphore
from bs4 import BeautifulSoup
from httpx import AsyncClient, Response
from loguru import logger
from tenacity import retry, wait_fixed


@retry(wait=wait_fixed(0.2))
async def get_response(session: AsyncClient, url: str) -> Response:
    res = await session.get(url)
    logger.info(f'{res.status_code} | {url}')
    assert res.status_code == 200
    return res


async def get_links(semaphore: Semaphore, session: AsyncClient, url: str) -> list:
    async with semaphore:
        res = await get_response(session, url)
        soup = BeautifulSoup(res.text, 'lxml')
        links = soup.find_all('a', class_='d-block mt5')
        links = [f"https://www.alta.ru{i['href']}" for i in links]
        return links
