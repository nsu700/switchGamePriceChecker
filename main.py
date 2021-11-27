import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import date
import logging
import sys

def get_price(url, name, db_conn, today):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    logging.debug('Pulling info from {}'.format(url))
    price = soup.find("meta", property="product:price:amount").get("content")
    currency = soup.find("meta", property="product:price:currency").get("content")
    id = url.split('/')[-1]
    gameTableID = maintainGameTable(db_conn, id, name, url)
    logging.debug(gameTableID)
    insertDB(db_conn, today, name, price, currency, url, gameTableID)

def get_list(url, db_conn):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    gamelist = soup.findAll("a" , {"class":"category-product-item-title-link"})
    today = date.today()
    for game in gamelist:
        name = game.get_text(strip=True)
        gameUrl = game.get("href")
        if gameUrl:
            get_price(gameUrl, name, db_conn, today)

def insertDB(cursor, date, name, price, currency, url, table):
    sqlInsert = "INSERT INTO '{}' VALUES ('{}', \"{}\", {}, '{}', '{}')".format(table, date, name, price, currency, url)
    cursor.execute(sqlInsert)
    logging.debug(sqlInsert)
    logging.info("Insert detail of game {}".format(name))

def maintainGameTable(cursor, id, name, url):
    gameID = "g" + str(id)
    sqlSearch = "SELECT name FROM sqlite_master WHERE type='table' AND name=('{}')".format(gameID)
    cursor.execute(sqlSearch)
    if (not cursor.fetchall()):
        logging.info("{} table missing, creating table {}".format(name, gameID))
        cursor.execute("INSERT INTO switch VALUES(\"{}\", '{}', '{}')".format(name, gameID, url))
        cursor.execute("CREATE TABLE IF NOT EXISTS '{}' (date real, name text, price real, currency text, url text )".format(gameID))
    return gameID

if __name__=='__main__':
    SWITCH_URL = 'https://store.nintendo.com.hk/games/all-released-games'
    try:
        db_conn = sqlite3.connect("switch.db")
        cursor = db_conn.cursor()
        logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.DEBUG)
        logging.info("Successfully Connected to SQLite")
        cursor.execute('''CREATE TABLE IF NOT EXISTS switch
                        (name real, id integer, url text )''')
        get_list(SWITCH_URL, cursor)
        db_conn.commit()
        logging.info("Record inserted successfully!")

    except sqlite3.Error as error:
        logging.info("Failed to insert data into sqlite", error)
    finally:
        if db_conn:
            db_conn.close()
            logging.info("The SQLite connection is closed")