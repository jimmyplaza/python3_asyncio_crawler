#-*- coding: utf-8 -*-
import json
import requests
import re
import sys
import errno
import asyncio
import aiohttp
import socket
from os import walk
from os.path import join

import guess_language
from nltk import wordpunct_tokenize
from nltk.corpus import stopwords
from pymongo import MongoClient
from socket import error as SocketError
from gensim.summarization import keywords
from concurrent.futures import ProcessPoolExecutor
import tqdm

from multiprocessing import Process, Queue

mongo_host = ""
mongo_port = 0
linelimit = 0
CONNECTION_TIMEOUT = 20

async def cleanHtml(html):
    """
    Remove HTML markup from the given string.
    :param html: the HTML string to be cleaned
    :type html: str
    :rtype: str
    """
    # remove inline JavaScript/CSS:
    cleaned = re.sub(
        r"(?is)<(script|style).*?>.*?(</\1>)", "", html.strip())
    # Then we remove html comments. This has to be done before removing regular
    # tags since comments can contain '>' characters.
    cleaned = re.sub(r"(?s)<!--(.*?)-->[\n]?", "", cleaned)
    # Next we can remove the remaining tags:
    cleaned = re.sub(r"(?s)<.*?>", " ", cleaned)
    # Finally, we deal with whitespace
    cleaned = re.sub(r"&nbsp;", " ", cleaned)
    cleaned = re.sub(r" +", " ", cleaned)
    cleaned = re.sub(r" +", " ", cleaned)
    cleaned = re.sub(r"\t+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


async def strip(url):
    global CONNECTION_TIMEOUT
    async with aiohttp.ClientSession() as session:
        with aiohttp.Timeout(CONNECTION_TIMEOUT):
            try:
                async with session.get(url) as response:
                    #print('%s: %s' % (url, response.status))
                    if response.status != 200:
                        print(response.reason)
                        return None
                    text = await response.text()
                    cleanhtml = await cleanHtml(text)
                    return cleanhtml
            except socket.error as e:
                print(e.strerror)
                return None
            except Exception as e:
                print(e)
                with open('./dmoz/unknow.list', 'a') as outFile:
                    outFile.write('{'+'\"url\"'+':'+'\"'+url+'\"'+'}\n')
                return None

async def save2mongo(db, jsondata):
    col = jsondata['category']
    db[col].create_index("url", unique=True)

    count = db[col].count()
    if count >= 200:
        return

    if not db.get_collection(col):
        db.create_collection(col)
    try:
        ret = db[col].insert_one(jsondata)
    except:
        return

def getTextrank(text):
    try:
        if text is None:
           return ""
        textrank = keywords(text)
        return textrank
    except asyncio.CancelledError:
        print('textrank be timeout cancelled')
        return ""
    except:
        with open('./badtextrank.txt', 'a') as outFile:
            if text is not None:
                outFile.write(text)
        return ""


async def workflow(db, line,loop):
    jsonData = json.loads(line)
    domain = jsonData["url"]
    category = jsonData["category"]
    text = await strip(domain)
    if text is None:
        return
    with ProcessPoolExecutor(2) as executor:
        textrank = await loop.run_in_executor(executor, getTextrank, text)
    lang = guess_language.guessLanguage(text)
    if lang == "UNKNOWN":
        return
    jsondata = {
        'url': domain,
        'textrank': textrank,
        'lang': lang,
        'category': category,
        'content': text,
    }
    await save2mongo(db, jsondata)



@asyncio.coroutine
def wait_with_progress(coros):
    for f in tqdm.tqdm(coros, total=len(coros), desc="Progress: ", smoothing=0.5):
        yield from f

def loadConfig():
    with open("config.json", "r") as con:
        config = json.load(con)
    global mongo_host
    global mongo_port
    global linelimit
    mongo_host = config['mongo_host']
    mongo_port = int(config['mongo_port'])
    linelimit = int(config['linelimit'])

def main():
    loadConfig()
    workFile = "./dmoz/cateeories/"
    categories = ["arts", "computers", "health", "news", "reference", "science", "society",
                "business", "games", "home", "recreation", "regional", "shopping", "sports"]

    loop = asyncio.get_event_loop()
    client = MongoClient(mongo_host, mongo_port)
    db = client['new_dmoz']
    for cate in categories:
        print(cate)
        try:
            with open(workFile + cate + "_url", 'rU') as f:
                    tasks = [workflow(db, line,loop) for i, line in enumerate(f) if i < linelimit]
                    loop.run_until_complete(wait_with_progress(tasks))
                    print('[%s] complete' % cate)
        except:
            continue
    loop.close()
    db.close()

if __name__ == '__main__':
    main()
