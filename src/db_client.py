from asyncpg import connect, Connection

from .config import DB


async def connection() -> Connection:
    conn = await connect(DB)
    return conn


async def insert_customs(name: str, inn: str, info_field: str, phone: str, email: str) -> None:
    conn = await connection()
    await conn.execute("""INSERT INTO customs(name, inn, info_field, phone, email)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (inn) DO NOTHING 
    """, name, inn, info_field, phone, email)

    await conn.close()


async def get_customs() -> list:
    conn = await connection()
    res = await conn.fetch("""SELECT name, inn, info_field, phone, email FROM customs""")
    await conn.close()

    return [list(i) for i in res]
