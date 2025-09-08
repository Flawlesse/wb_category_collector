import aiohttp
import asyncio
import aiosqlite
from collections import deque
import logging
import time
import json
import uvloop
import os
from pathlib import Path
from user_agents import USER_AGENTS


BATCH_SIZE = 500
SQLITE_PATH = Path("/data") / os.environ.get("DB_PATH")

cat_list_url = "https://static-basket-01.wbbasket.ru/vol0/data/main-menu-by-ru-v3.json"


logging.basicConfig(filename=Path("/logs") / os.environ.get("LOG_PATH"))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def parse_filters(cat: dict, session: aiohttp.ClientSession, fullname: str):
    """Parse list category subjects."""

    # WB has some issues with these categories (literally no products displayed)
    blacklisted_fullnames = [
        "#Здоровье#Контрацептивы и лубриканты",
        "#Товары для взрослых#Презервативы и лубриканты",
        "#Товары для взрослых#Секс игрушки#Вибраторы и стимуляторы",
        "#Товары для взрослых#Фетиш и БДСМ",
    ]
    if fullname in blacklisted_fullnames:
        return []

    url_old_filters = (
        "https://catalog.wb.ru/catalog/{shard}/v8/filters?"
        + "ab_testing=false&appType=1&{query}&curr=rub"
        + "&dest=-59202&lang=ru"
    )
    # russian symbols from searchQuery are url-encoded automatically
    url_sq = (
        "https://search.wb.ru/exactmatch/sng/common/v18/search?ab_testing=false&appType=1&autoselectFilters=false"
        + "&curr=rub&dest=-59202&lang=ru&query={searchQuery}&resultset=filters&filters=ffsubject"
    )

    # all the needed payload
    shard = cat.get("shard")
    cat_id = cat.get("id")
    query = cat.get("query", f"cat={cat_id}")  # this is important
    searchQuery = cat.get("searchQuery")

    if searchQuery:
        url = url_sq.format(searchQuery=searchQuery)
    else:
        logger.warning(f"searchQuery=NONE!!!! category {fullname}")
        url = url_old_filters.format(shard=shard, query=query)

    logger.info(
        f"Parsing category '{fullname}' id={cat_id} {shard=} " + f"{query=} {searchQuery=}...\nQuery: {url}"
    )

    if shard == "blackhole":
        logger.warning(
            f"List category '{fullname}' id={cat_id} {shard=} {query=} "
            + "is a blackhole shard. Error on WB side, aborting."
        )
        return []

    if not shard and not searchQuery:
        logger.warning(
            f"List category '{fullname}' id={cat_id} {shard=} {query=} "
            + "is a broken category. Error on WB side, aborting."
        )
        return []

    retry_needed = True
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "accept-encoding": "gzip, deflate, br, zstd",
        "upgrade-insecure-requests": "1",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "priority": "u=0, i",
    }
    user_agent_iter = iter(USER_AGENTS)
    headers.update(next(user_agent_iter))
    while retry_needed:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status >= 400:
                    logger.error(f"Error processing {url=}: status={response.status}")

                resp_text = await response.text()
                data = json.loads(resp_text.encode("utf-8"))
                filters = data["data"]["filters"]
                cat_filter = [f for f in filters if f["name"] == "Категория"]

                if not cat_filter:
                    logger.warning(
                        f"Category '{fullname}' id={cat_id} {shard=} "
                        + f"{query=} {searchQuery=} does not have 'Категория' filter."
                    )
                    return []
        except Exception:
            try:
                logger.warning(f"Category '{fullname}': Fetching WB API failed. Setting new user agent...")
                headers.update(next(user_agent_iter))
                logger.warning(f"Category '{fullname}': Setup User-Agent: {headers['user-agent']}.")
            except StopIteration:
                logger.error(f"User Agents exhausted! Category '{fullname}' parsing did not succeed.")
                return []
        else:
            retry_needed = False

    cat_filter = cat_filter[0]
    return [[item["id"], item["name"], 99] for item in cat_filter["items"]]


async def parse(table_name: str, cat: dict, session: aiohttp.ClientSession):
    """
    Parse category tree DFS way.

    NOTICE: We don't have to transform data into a TreeType, since\n
    categories JSON is already representing it. Also dictionaries\n
    are mutable, so it's the same as managing pointers, we don't
    copy anything.
    """

    async with aiosqlite.connect(SQLITE_PATH) as conn:
        # create table right away
        await conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}"(Id, Name, Depth)')

        # traverse category tree DFS way
        node_stack = deque([(cat, 1, "")])
        batch = []
        while node_stack:
            node_t = node_stack.pop()
            cat = node_t[0]
            dlevel = node_t[1]
            fullname_curr = node_t[2]

            # write category info
            batch.append([cat["id"], cat["name"], dlevel])

            if not cat.get("childs"):  # is list category
                subjects = await parse_filters(cat, session, fullname_curr + "#" + cat["name"])
                batch.extend(subjects)
                continue

            # not a list category -> push children and traverse further
            for ch_cat in reversed(cat["childs"]):
                node_stack.append((ch_cat, dlevel + 1, fullname_curr + "#" + cat["name"]))

            # dump data
            if len(batch) >= BATCH_SIZE:
                logger.info(
                    f"Dumping batch (size={len(batch)}), for table '{table_name}'; PREVIEW: {batch[:5]}..."
                )
                await conn.executemany(f'INSERT INTO "{table_name}" VALUES (?, ?, ?)', batch)
                await conn.commit()
                del batch
                batch = []

        # dump remaining data
        logger.info(f"Dumping batch (size={len(batch)}), for table '{table_name}'; PREVIEW: {batch[:5]}...")
        await conn.executemany(f'INSERT INTO "{table_name}" VALUES(?, ?, ?)', batch)
        await conn.commit()


async def main():
    start_time = time.perf_counter()

    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=20,
        enable_cleanup_closed=True,
    )

    timeout = aiohttp.ClientTimeout(
        total=30,
        connect=30,
        sock_connect=30,
        sock_read=30,
    )

    tasks = []
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        trust_env=True,
    ) as session:
        async with session.get(cat_list_url) as response:
            cat_list = await response.json()
            for cat in cat_list:
                coro = parse(cat["name"], cat, session)
                tasks.append(coro)

        res = await asyncio.gather(*tasks, return_exceptions=True)

    logger.info(f"Result: {res}")
    if any(res):
        logger.error("Parsing ended with runtime errors. Check the logs and re-run the parsing.")
    end_time = time.perf_counter()
    logger.info(f"MAIN Time elapsed: {(end_time - start_time):.2f} seconds.")


if __name__ == "__main__":
    asyncio.run(main())
