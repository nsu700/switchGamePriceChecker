import sqlite3
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import date

def get_price(link, name, db_conn, today):
    url = link
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    price = soup.find("meta", property="product:price:amount").get("content")
    currency = soup.find("meta", property="product:price:currency").get("content")
    insertDB(db_conn, today, name, price, currency, url)

def get_list(list, db_conn):
    url = list
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    gamelist = soup.findAll("a" , {"class":"category-product-item-title-link"})
    today = date.today()
    for game in gamelist:
        name = game.get_text(strip=True)
        url = game.get("href")
        if url:
            get_price(url, name, db_conn, today)

def insertDB(cursor, date, name, price, currency, url, table='switch'):
    sqlInsert = "INSERT INTO switch VALUES (?, ?, ?, ?, ?)"
    data = (date, name, price, currency, url)
    cursor.execute(sqlInsert, data)
    print("Insert detail of game {}".format(name))

if __name__=='__main__':
    GAME_LIST = 'https://store.nintendo.com.hk/games/all-released-games'
    FAVOURITE_LIST = [""]

    try:
        db_conn = sqlite3.connect("switch.db")
        cursor = db_conn.cursor()
        print("Successfully Connected to SQLite")
        cursor.execute('''CREATE TABLE IF NOT EXISTS switch
                        (date text, name text, price real, currency text, url text )''')
        get_list(GAME_LIST, cursor)
        db_conn.commit()
        print("Record inserted successfully into SqliteDb_developers table ", cursor.rowcount)

    except sqlite3.Error as error:
        print("Failed to insert data into sqlite", error)
    finally:
        if db_conn:
            db_conn.close()
            print("The SQLite connection is closed")