import asyncio
import os
import re
from asyncio import Semaphore
from bs4 import BeautifulSoup
from httpx import AsyncClient, Response
from loguru import logger
from tenacity import retry, wait_fixed
import pandas as pd

import phonenumbers
from phonenumbers import geocoder

from .db_client import get_customs, insert_customs


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


def extract_contacts(text: str) -> tuple[str, str]:
    all_phone_numbers = phonenumbers.PhoneNumberMatcher(text, "RU")
    extracted_numbers = []

    for match in all_phone_numbers:
        phone_number = match.number

        # Проверка на валидность номера телефона
        if phonenumbers.is_valid_number(phone_number):
            # Нормализация номера телефона в международном формате
            normalized_number = phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)

            if normalized_number not in extracted_numbers:
                extracted_numbers.append(normalized_number)

    email_pattern = r"[\w\.-]+@[\w\.-]+"
    emails = re.findall(email_pattern, text)
    emails = ', '.join(set(emails))
    numbers = ', '.join(extracted_numbers)

    return emails, numbers


async def get_data(semaphore: Semaphore, session: AsyncClient, url: str):
    async with semaphore:
        res = await get_response(session, url)
        soup = BeautifulSoup(res.text, 'lxml')
        name_and_inn = soup.find('h1').text.split()
        name = ' '.join(name_and_inn[:-1])
        inn = name_and_inn[-1]
        table = soup.find_all('tr')
        field_data = ''
        for tr in table:

            if tr.text.strip() == 'Доверенные лица правообладателя':
                field_data = tr.find_next('tr').text.strip()

        email, phone = extract_contacts(field_data)

        await asyncio.create_task(insert_customs(name, inn, field_data, phone, email))


async def write_to_excel() -> None:
    src_path = os.path.dirname(__file__)
    data_path = os.path.join(src_path, '../data')
    if not os.path.exists(data_path):
        os.mkdir(data_path)

    data = await get_customs()
    columns = ['Бренд', 'ИНН', 'Контактное лицо', 'Телефон', 'Email']

    df = pd.DataFrame(data, columns=columns)
    df.to_excel(f'{data_path}/data.xlsx', index=False)
