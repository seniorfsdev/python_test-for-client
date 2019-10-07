import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import asyncio
import aiohttp
import sqlite3
from sqlite3 import Error
import requests
from bs4 import BeautifulSoup
from multiprocessing import Process, Queue, cpu_count, current_process
import os

NUM_THREADS = 10
url = 'https://www.stackoverflow.com/'
q = Queue()
db_file = os.path.join(os.getcwd(), "sqlite.db")


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

@asyncio.coroutine
async def crawl_page():
    conn = create_connection(db_file)
    results = []
    try:
        while not q.empty():
            link = q.get()
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(link) as resp:
                        html_content = await resp.text()
                        soup = BeautifulSoup(html_content, 'html.parser')
                        title = soup.find('title').text
                        results.append((title, link))
                except:
                    pass

        try:
            cur = conn.cursor()
            sql = ''' INSERT INTO pages(title,url)
                      VALUES(?,?) '''
            cur.executemany(sql, results)
            cur.close()
        except Error as e:
            print(e)
    finally:
        conn.commit()
        conn.close()


def run_crawler():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        crawler = crawl_page()
        return loop.run_until_complete(crawler)
    finally:
        loop.close()


async def spawn_workers():
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(max_workers=NUM_THREADS)
    futures = [
        loop.run_in_executor(executor, run_crawler)
    ]
    await asyncio.gather(*futures)


def spawn_process(q):
    loop = asyncio.get_event_loop()
    policy = asyncio.get_event_loop_policy()
    policy.set_event_loop(policy.new_event_loop())
    loop.run_until_complete(spawn_workers())


if __name__ == '__main__':
    if url[-1] == "/":
        url = url[:-1]
    r = requests.get(url)
    html_content = r.text
    soup = BeautifulSoup(html_content, 'html.parser')

    sql_create_pages_table = """ CREATE TABLE IF NOT EXISTS pages (
                                            title text NOT NULL,
                                            url text NOT NULL
                                        ); """
    conn = create_connection(db_file)
    if conn is not None:
        create_table(conn, sql_create_pages_table,)
    else:
        print("Error: Could not establish a connection with the database.")
        exit()
    conn.close()

    links = []
    for a in soup.find_all('a', href=True):
        link = a.get('href')
        if link == '#' or link.find("mailto:") >= 0: continue
        if (link[0] == '/' or link[0] == '#') and len(link.strip()) > 0:
            link = url + link[0]
        if link.find("/") > link.find("."):
            link = url + "/" + link
        links.append(link)
    unique_links = list(set(links))

    for link in unique_links:
        q.put(link)

    num_cores = cpu_count()
    processes = []
    for _ in range(num_cores):
        p = Process(target=spawn_process, args=(q,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

