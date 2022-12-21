import requests
import re
from bs4 import BeautifulSoup
import psycopg2

# --------------
# db config
# --------------
DB_NAME = "Scrappy"
DB_USER = "postgres"
PASSWORD = "1"
HOST = "localhost"
PORT = "5432"
DB_TABLE = "news"

conn = psycopg2.connect(
    database=DB_NAME,
    user=DB_USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)
cursor = conn.cursor()

"""
# Check if exist in db
# If exist then update
# Else create new
"""


def db_insert(data):
    year = year_checker(data)
    month = month_checker(data)
    eps = eps_checker(data)

    postgres_select_query = """ SELECT * FROM news WHERE 
    YEAR='{}' AND MONTH='{}'
    """.format(year, month)
    cursor.execute(postgres_select_query)
    conn.commit()

    already_exist = cursor.fetchall()

    if not already_exist:
        postgres_insert_query = """ INSERT INTO news (YEAR, MONTH, EPS) VALUES (%s,%s,%s)"""
        record_to_insert = (year, month, eps)
        cursor.execute(postgres_insert_query, record_to_insert)

        conn.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into news table")

    else:
        postgres_update_query = """UPDATE news SET EPS=%s WHERE id=%s"""
        record_to_update = (eps, already_exist[0][3])
        cursor.execute(postgres_update_query, record_to_update)


# -----------------
# Get data from DB
# -----------------
def db_fetch():
    postgres_select_query = """ SELECT * FROM news order by YEAR DESC, MONTH DESC"""
    cursor.execute(postgres_select_query)
    conn.commit()
    data = cursor.fetchall()

    length = len(data)
    for item in range(length):
        print("{}. Date: {}-{} EPS: {}".format(item + 1, data[item][0], data[item][1], data[item][2]))


# --------------
# parsing config
# --------------
URL = "https://www.dsebd.org/old_news.php?inst=ACI&criteria=3&archive=news"
page = requests.get(URL)
soup = BeautifulSoup(page.content, "html.parser")
results = soup.find("table", class_="table-news")
news_list = results.find_all("td")

# --------------
# regex patterns
# --------------
year_pattern = "[1-3][0-9]{3}"

months = [
    "January-March",
    "April-June",
    "July-September",
    "October-December"
]

month_pattern = '|'.join(months)
pattern = "(Q[1-3] Un-audited)"
eps_pattern_up = "[0-9][.][0-9]{2}"
eps_pattern_down = "[(][0-9][.][0-9]{2}[)]"


# ------------------
# checking methods
# ------------------
def year_checker(eps_data):
    return re.search(year_pattern, eps_data).group(0)


def month_checker(eps_data):
    month = re.search(month_pattern, eps_data).group()
    num_month = (months.index(month) + 1) * 3
    if num_month < 12:
        return '0' + str(num_month)
    return str(num_month)


def eps_checker(eps_data):
    result_up = re.search(eps_pattern_up, eps_data)
    result_down = re.search(eps_pattern_down, eps_data)

    if result_down:
        return '-' + str(result_up.group(0))
    elif result_up.group(0):
        return result_up.group(0)


# ------------------
# base methods
# ------------------
def refresher():
    for news in news_list:
        text = news.text.strip()

        if re.search(pattern, text):
            raw_data = text.split(':')[1].split(';')[0]
            current = raw_data.split('against')[0]
            previous = raw_data.split('against')[1]

            db_insert(current)
            db_insert(previous)


def get_data():
    db_fetch()


# --------------
# Driver
# --------------
if __name__ == '__main__':
    refresher()
    get_data()
