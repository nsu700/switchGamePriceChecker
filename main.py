import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import date
import logging
import sys
import time
import os
from fake_useragent import UserAgent

def slack_notification(data):
    url = os.environ['SLACK_WEBHOOK']
    requests.post(url,json=data)

def get_response(url):
    try:
        response = requests.get(url, HEADERS, timeout=CTIMEOUT)
    except requests.HTTPError:
        time.sleep(5)
        response = requests.get(url, HEADERS, timeout=CTIMEOUT)
    finally:
        if response:
            return(response)
        else:
            sys.exit

def get_price(url, name, db_conn, today, price):
    response = get_response(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    logging.debug('Pulling info from {}'.format(url))
    price = int(soup.find("meta", property="product:price:amount").get("content"))
    currency = soup.find("meta", property="product:price:currency").get("content")
    id = url.split('/')[-1]
    gameTableID = maintainGameTable(db_conn, id, name, url, price)
    logging.debug(gameTableID)
    insertDB(db_conn, today, name, price, currency, url, gameTableID)

def get_list(url, db_conn):
    response = get_response(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    gamelist = soup.findAll("div", class_=["category-product-item-price", "category-product-item-title"])
    today = date.today()
    numOfGame = len(gamelist)
    for i in range(0, numOfGame, 2):
        price = gamelist[i].select("span.price-wrapper")[0].get_text(strip=True)
        name = gamelist[i+1].select("a.category-product-item-title-link")[0].get_text(strip=True)
        gameUrl = gamelist[i+1].select("a.category-product-item-title-link")[0].get("href")
        if gameUrl:
            get_price(gameUrl, name, db_conn, today, price)

def insertDB(cursor, date, name, price, currency, url, table):
    sqlInsert = "INSERT INTO '{}' VALUES ('{}', \"{}\", {}, '{}', '{}')".format(table, date, name, price, currency, url)
    cursor.execute(sqlInsert)
    logging.debug(sqlInsert)
    logging.info("Insert detail of game {}".format(name))

def maintainGameTable(cursor, id, name, url, price):
    gameID = "g" + str(id)
    sqlSearch = "SELECT name FROM sqlite_master WHERE type='table' AND name=('{}')".format(gameID)
    cursor.execute(sqlSearch)
    if (not cursor.fetchall()):
        logging.info("{} table missing, creating table {}".format(name, gameID))
        cursor.execute("INSERT INTO switch VALUES(\"{}\", '{}', '{}', {})".format(name, gameID, url, price))
        cursor.execute("CREATE TABLE IF NOT EXISTS '{}' (date real, name text, price real, currency text, url text )".format(gameID))
        data = {'text':'发现新游戏{}，价钱是{}，链接是{}'.format(name, price, url)}
        slack_notification(data)
    else:
        updateGamePrice(cursor, gameID, price, name, url)
    return gameID

def updateGamePrice(cursor, id, price, name, link):
    priceQuery = int(cursor.execute("SELECT price FROM switch WHERE id='{}'".format(id)).fetchall()[0][0])
    sqlUpdate = "UPDATE switch SET price = {} WHERE id = '{}'".format(price, id)
    if (priceQuery > price):
        data = {'text':'{}降价啦，原价是{}， 现价是{}，点击链接不要买先{}'.format(name, priceQuery, price, link)}
        slack_notification(data)
        logging.info("Price not the same, updating price. {} > {}".format(priceQuery, price))
        logging.debug("The original price is {}, type is {}, the current price is {}, type is {}".format(priceQuery, type(priceQuery), price, type(price)))
        cursor.execute(sqlUpdate)
    elif (priceQuery < price):
        data = {'text':'他娘类， {}涨价了，原价是{}， 现价是{}，游戏链接{}'.format(name, priceQuery, price, link)}
        slack_notification(data)
        logging.info("Price not the same, updating price. {} < {}".format(priceQuery, price))
        logging.debug("The original price is {}, type is {}, the current price is {}, type is {}".format(priceQuery, type(priceQuery), price, type(price)))
        cursor.execute(sqlUpdate)

if __name__=='__main__':
    SWITCH_URL = 'https://store.nintendo.com.hk/games/all-released-games'
    CTIMEOUT = 20
    fu = UserAgent()
    global HEADERS
    HEADERS = {'User-Agent': fu.random}
    try:
        db_conn = sqlite3.connect("switch.db")
        cursor = db_conn.cursor()
        logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.DEBUG)
        logging.info("Successfully Connected to SQLite")
        # cursor.execute('''ALTER TABLE switch ADD price text''')
        # sys.exit(0)
        cursor.execute('''CREATE TABLE IF NOT EXISTS switch
                        (name real, id integer, url text, price text)''')
        get_list(SWITCH_URL, cursor)
        db_conn.commit()
        logging.info("Record inserted successfully!")

    except sqlite3.Error as error:
        logging.info("Failed to insert data into sqlite", error)
    finally:
        if db_conn:
            db_conn.close()
            logging.info("The SQLite connection is closed")