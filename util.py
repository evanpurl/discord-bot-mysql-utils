import os

import aiomysql
from dotenv import load_dotenv

load_dotenv()


async def create_pool():
    pool = await aiomysql.create_pool(host=os.getenv('host'), port=int(os.getenv('port')),
                                      user=os.getenv('user'), password=os.getenv('password'),
                                      db=os.getenv('db'))

    return pool


async def insert_one(pool, bot, column, data):

    mysql = f"""INSERT INTO {bot} ({column}) VALUES ({data});"""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(mysql)
                await conn.commit()
        pool.close()
        await pool.wait_closed()
    except aiomysql.Error as e:
        print(e)


async def insert_one_ignore(pool, bot, column, data):
    mysql = f"""INSERT IGNORE INTO {bot} ({column}) VALUES ({data});"""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(mysql)
                await conn.commit()
        pool.close()
        await pool.wait_closed()
    except aiomysql.Error as e:
        print(e)


async def insert_many(pool, bot, columnlist, datalist):
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for index, val in enumerate(columnlist):
                    await cur.execute(f"""INSERT INTO {bot} ({val}) VALUES ({datalist[index]});""")
                await conn.commit()
        pool.close()
        await pool.wait_closed()
    except aiomysql.Error as e:
        print(e)


async def insert_many_ignore(pool, bot, columnlist, datalist):
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for index, val in enumerate(columnlist):
                    await cur.execute(f"""INSERT IGNORE INTO {bot} ({val}) VALUES ({datalist[index]});""")
                await conn.commit()
        pool.close()
        await pool.wait_closed()
    except aiomysql.Error as e:
        print(e)


async def update_one(pool, bot, column, data, serverid):

    mysql = f"""UPDATE {bot} SET {column} = {data} WHERE serverid = {serverid};"""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(mysql)
                await conn.commit()
        pool.close()
        await pool.wait_closed()
    except aiomysql.Error as e:
        print(e)


async def update_many(pool, bot, columnlist, datalist, serverid):
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for index, val in enumerate(columnlist):
                    await cur.execute(f"""UPDATE {bot} SET {val} = {datalist[index]} WHERE serverid = {serverid};""")
                await conn.commit()
        pool.close()
        await pool.wait_closed()
    except aiomysql.Error as e:
        print(e)


async def get(pool, mysql):
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(mysql)
                result = await cur.fetchone()

        pool.close()
        await pool.wait_closed()
        if not result:
            return 0
        if len(result) == 0:
            return 0
        return result[0]
    except Exception as e:
        print(e)


async def getmultiple(pool, mysql):
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(mysql)
                result = await cur.fetchone()

        pool.close()
        await pool.wait_closed()
        if not result:
            return 0
        if len(result) == 0:
            return 0
        return result
    except Exception as e:
        print(e)


async def getall(pool, mysql):
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(mysql)
                result = await cur.fetchall()

        pool.close()
        await pool.wait_closed()
        if not result:
            return [0]
        if len(result) == 0:
            return [0]
        returnlist = []
        for a in result:
            returnlist.append(a[0])
        return returnlist
    except Exception as e:
        print(e)


async def createserver(pool, bot, guild):
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"""INSERT IGNORE INTO {bot.user.name} (serverid) VALUES ({guild.id});""")
                await conn.commit()
        pool.close()
        await pool.wait_closed()
    except Exception as e:
        print(e)
        return e


async def deleteserver(pool, bot, guild):
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"""DELETE IGNORE FROM {bot.user.name} WHERE serverid = ({guild.id});""")
                await conn.commit()
        pool.close()
        await pool.wait_closed()
    except Exception as e:
        print(e)
        return e
