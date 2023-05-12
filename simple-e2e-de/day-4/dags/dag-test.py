from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import json
import requests
from bs4 import BeautifulSoup as bs

import sqlite3


tmp_data_path = '/home/rohwid/free-course/simple-e2e-de/day-4/dags/data/temp/data-390.json'
sqlite_path = '/home/rohwid/free-course/simple-e2e-de/day-4/dags/data/db/invoice.db'

con = sqlite3.connect(sqlite_path)
cur = con.cursor()


# ========================== INGESTING DATA, from Day 1

def get_invoice_detail(invoice_id):
    url = f'https://invoice-scraping.demo.pacmann.ai/invoice/{invoice_id}'
    page = requests.get(url).text
    soup = bs(page)
    rows = soup.find('tbody').find_all('tr')
    result = []
    for row in rows:
        x = row.getText()
        brand = row.find('td', class_='brand').getText()
        type_ = row.find('td', class_='type').getText()
        price = row.find('td', class_='price').getText()
        quantity = row.find('td', class_='quantity').getText()
        data = {
            'brand': brand,
            'type': type_,
            'price': int(price.split('.')[0]),
            'quantity': quantity
        }
        result.append(data)
    return result


def get_invoice_overview(row):
    invoice_id = row.find('a').getText()
    invoice_detail_url = row.find('a')['href']
    invoice_date = row.find('td', class_='invoice_date').getText()
    country_of_origin = row.find('td', class_='country_of_origin').getText()
    seller = row.find('td', class_='seller').getText()
    distribution_area = row.find('td', class_='distribution_area').getText()
    total_price = row.find('td', class_='total_price').getText()
    
    # get invoice detail
    invoice_detail = get_invoice_detail(invoice_id)

    row_data = {
        'invoice_id': int(invoice_id),
        'invoice_detail_url': invoice_detail_url,
        'invoice_date': invoice_date,
        'country_of_origin': country_of_origin,
        'seller': seller,
        'distribution_area': distribution_area,
        'total_price': int(total_price.split('.')[0]),
        'detail': invoice_detail
    }
    return row_data


def get_all_invoice(url):
    page = requests.get(url).text
    soup = bs(page)
    rows = soup.find('tbody').find_all('tr')
    result = []
    for i, row in enumerate(rows):
        print(f'downloading row {i}')
        x = get_invoice_overview(row)
        result.append(x)
    return result


# ========================== TRANSFORMING AND STORING DATA, from Day 1


def transform_insert_invoice(invoice_data):

    invoice_master_data = []
    for row in invoice_data:
        invoice_id = row['invoice_id']
        invoice_date = row['invoice_date']
        country_of_origin = row['country_of_origin']
        seller = row['seller']
        distribution_area = row['distribution_area']
        total_price = row['total_price']
        tmp = (invoice_id, invoice_date,
               country_of_origin, seller,
               distribution_area, total_price)
        invoice_master_data.append(tmp)
    cur.executemany(
        "INSERT INTO invoice VALUES(?, ?, ?, ?, ?, ?)",
        invoice_master_data
    )
    con.commit()


def transform_insert_invoice_detail(invoice_data):

    invoice_detail_data = []
    for row in invoice_data:
        detail = row['detail']
        for d in detail:
            invoice_id = row['invoice_id']
            brand = d['brand']
            type_ = d['type']
            price = d['price']
            quantity = int(d['quantity'])
            tmp = (invoice_id, brand, type_, price, quantity)
            invoice_detail_data.append(tmp)
    cur.executemany(
        "INSERT INTO invoice_product VALUES(?, ?, ?, ?, ?)",
        invoice_detail_data
    )
    con.commit()


# ========================== CALLABLE


def scrape_invoice():
    url = 'https://invoice-scraping.demo.pacmann.ai/nextpage/390'
    data = get_all_invoice(url)
    open(tmp_data_path, 'w+').write(json.dumps(data))


def process_invoice():
    # load data from tmp path
    invoice_data = json.loads(open(tmp_data_path).read())
    transform_insert_invoice(invoice_data)


def process_invoice_detail():
    # load data from tmp path
    invoice_data = json.loads(open(tmp_data_path).read())
    transform_insert_invoice_detail(invoice_data)


def get_latest_invoice():
    data = cur.execute("""
        SELECT *
        FROM invoice
        ORDER BY invoice_id DESC
        LIMIT 2
    """).fetchall()
    for d in data:
        print(d)


def get_latest_invoice_product():
    data = cur.execute("""
        SELECT *
        FROM invoice_product
        ORDER BY invoice_id DESC
        LIMIT 2
    """).fetchall()
    for d in data:
        print(d)


with DAG(
    'tutorial-pacmann',
    start_date=datetime(2023, 5, 11),
    schedule=None,
    catchup=False
) as dag:

    task_scrape_invoice = PythonOperator(
        task_id='t_scrape_invoice',
        python_callable=scrape_invoice
    )

    task_process_invoice = PythonOperator(
        task_id='t_process_invoice',
        python_callable=process_invoice
    )

    task_process_invoice_detail = PythonOperator(
        task_id='t_process_invoice_detail',
        python_callable=process_invoice_detail
    )

    task_get_latest_invoice = PythonOperator(
        task_id='t_get_latest_invoice',
        python_callable=get_latest_invoice
    )

    task_get_latest_invoice_product = PythonOperator(
        task_id='t_get_latest_invoice_product',
        python_callable=get_latest_invoice_product
    )

    task_scrape_invoice >> \
        task_process_invoice >> task_process_invoice_detail >> \
        task_get_latest_invoice >> task_get_latest_invoice_product
