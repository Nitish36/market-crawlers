import requests
import pandas as pd
from bs4 import BeautifulSoup
from lxml import etree, html
import gspread
from gspread_dataframe import set_with_dataframe
import os
import pymongo
from bson.objectid import ObjectId
import pprint

printer = pprint.PrettyPrinter()
client = pymongo.MongoClient("mongodb://localhost:27017/")
stock_db = client.stock
collections = stock_db.list_collection_names()
def generate():
    url = "https://www.nseindia.com/api/etf"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.nseindia.com/",
    }

    req = requests.get(url, headers=headers)
    data = req.json()
    return data

def crawler():
    # Convert JSON to DataFrame
    data = generate()
    df = pd.DataFrame(data['data'])
    df = df.iloc[1:]
    df = df.drop(index=df.index[0])
    df.to_csv('datasets\\csv\\etf.csv', index=False)
    df.to_json('datasets\\json\\etf.json', orient="records")
    return df

def write_to_mongo():
    data_json = generate()
    collection = stock_db.etf
    sme_insert = collection.insert_one(data_json).inserted_id
    print("Record Inserted")
    print(sme_insert)
    data = collection.find()
    for dat in data:
        printer.pprint(dat)



def writer():
    df = crawler()
    GSHEET_NAME = 'Stock'
    TAB_NAME = 'Etf'
    credentialsPath = os.path.expanduser("credentials\\diamond-analysis-ac6758ca1ace.json")

    if os.path.isfile(credentialsPath):
        # Authenticate and open the Google Sheet
        gc = gspread.service_account(filename=credentialsPath)
        sh = gc.open(GSHEET_NAME)
        worksheet = sh.worksheet(TAB_NAME)

        set_with_dataframe(worksheet, df)

        # Now, 'df' contains the data from the Google Sheet
        print("Data loaded successfully!! Have fun!!")
        print(df)
    else:
        print(f"Credentials file not found at {credentialsPath}")


writer()