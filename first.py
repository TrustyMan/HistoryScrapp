#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 14 19:19:36 2018

@author: Chunyan
"""

from urllib.request import urlopen
import re
from datetime import datetime
import time
import mysql.connector
import _thread
from queue import Queue

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime


import bs4
from bs4 import BeautifulSoup as soup
import requests

hosturl 	= "34.73.255.82"
dbuser 		= "root"
dbpassoword = "password"
dbname 		= "yahoo_finance"

# stock_table_name = ['tbl_align', 'tbl_poly', 'tbl_aal', 'tbl_ibm', 'tbl_rrs']
# stockNames = ['ALGN', 'POLY', 'AAL', 'IBM', 'RRS']

stock_table_name = ['tbl_ibm', 'tbl_rrs']
stockNames = ['IBM', 'RRS']

finance_url = 'https://finance.yahoo.com/quote/'
# stock_url = ['ALGN/history?period1=980784000&period2=1548259200&interval=1d&filter=history&frequency=1d', 'POLY.L/history?period1=1319731200&period2=1548259200&interval=1d&filter=history&frequency=1d', 'AAL.L/history?period1=927475200&period2=1548259200&interval=1d&filter=history&frequency=1d', 'IBM/history?period1=-252403200&period2=1548259200&interval=1d&filter=history&frequency=1d', 'RRS.V/history?period1=839433600&period2=1548259200&interval=1d&filter=history&frequency=1d']

stock_url = ['IBM/history?period1=-252403200&period2=1548259200&interval=1d&filter=history&frequency=1d', 'RRS.V/history?period1=839433600&period2=1548259200&interval=1d&filter=history&frequency=1d']


path_to_chromedriver = '/usr/bin/chromedriver'

options = webdriver.ChromeOptions()
prefs = {"profile.default_content_setting_values.notifications" : 2}
# options.add_experimental_option("prefs",prefs)
options.add_argument('headless')
# options.add_argument('window-size=1200,1100')
options.add_argument("disable-infobars")
# options.add_argument("--incognito");
options.add_argument('--disable-gpu')
options.add_argument("--disable-extensions")

options.add_argument("--disable-impl-side-painting")
options.add_argument("--disable-accelerated-2d-canvas'")
options.add_argument("--disable-gpu-sandbox")
options.add_argument("--no-sandbox")
options.add_argument("--disable-extensions")
options.add_argument("--dns-prefetch-disable")

def StockDataToSql(data):
    try:
        mydb = mysql.connector.connect(
              host      = hosturl,
              user      = dbuser,
              passwd    = dbpassoword,
              database  = dbname
        )
        mycursor = mydb.cursor()

        for i in range(2):                
                for j in range(len(data[i])):
                    datastr = ""
                    for k in range(7):                                    
                        datastr = datastr + "'" + data[i][j][k] + "'" + ","

                    datastr = datastr + "'" + data[i][j][7] + "'"
                    datastr = "(" + datastr + ")"
                        
                    sql = "INSERT INTO " + stock_table_name[i] + " (symbolName, Date, Open, High, Low, Close, AdjClose, Volume) VALUES " + datastr

                    print(sql)

                    mycursor.execute(sql)
                    mydb.commit()
            
        mycursor.close()
        mydb.close()
    except Exception as e: print(e)

def getStockData():
    data =  [[], []]

    for i in range(2):
        temp_data =  list()

        geturl = finance_url+stock_url[i]

        driver = webdriver.Chrome(executable_path = path_to_chromedriver, chrome_options=options)
        
        driver.get(geturl)
        
        time.sleep(5)

        for j in range(1000): 

            t = str(10000*(j+1))
            driver.execute_script("window.scrollTo(0, "+t+")")
        
        res = driver.execute_script("return document.documentElement.outerHTML")
        driver.quit()

        page_soup = soup(res, "lxml")
        containers = page_soup.findAll("tr", {"class":"Whs(nw)"})
        # status = containers.findAll("small", {"class":"intraday__status"})
        x = 0
        for obj in containers:
            val_obj = obj.findAll("td", {"class":"Py(10px)"})
            x = x+1
            # print(val_obj[0].text, val_obj[1].text, val_obj[2].text, val_obj[3].text, val_obj[4].text, val_obj[5].text)
            # print(stockNames[i], val_obj)
            Date = ""
            Open = ""
            High = ""
            Low = ""
            Close = ""
            AdjClose = ""
            Volume = ""
            # if(len(val_obj) != 7):
            #     temp_data.append(val_obj)
            if(len(val_obj) == 2):
                Date = val_obj[0].text
                Open = val_obj[1].text
                High = ""
                Low = ""
                Close = ""
                AdjClose = ""
                Volume = ""
            else:
                Date = val_obj[0].text
                Open = val_obj[1].text
                High = val_obj[2].text
                Low = val_obj[3].text
                Close = val_obj[4].text
                AdjClose = val_obj[5].text
                Volume = val_obj[6].text

            temp = [stockNames[i], Date, Open, High, Low, Close, AdjClose, Volume]
            temp_data.append(temp)
        
        data[i] = temp_data

        time.sleep(3)
        
    return data

def main():
    start_time = time.time()
    data = getStockData()
    StockDataToSql(data)
    execute_time = time.time() - start_time
    print("Stock Scrapping Time:", execute_time)

main()