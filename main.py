#! python3
# coding: utf-8
import os, sys, time
import requests  
from bs4 import BeautifulSoup 
import re 
import lxml 
import traceback 
from multiprocessing import Pool, Process
from functools import wraps
import urllib.parse


def spend_time(func):
    @wraps(func)
    def f(*args, **kwargs):
        start = time.time()
        return_data = func(*args, **kwargs)
        end = time.time()
        sec = round(end - start, 5)
        print(f"{func.__name__}花費{sec}秒")
        return return_data
    return f


"""
讀取setting資料
"""
def load_setting(path, file_name, upper=True):
    data = dict()
    with open(os.path.join(path, file_name), "r", encoding="utf8") as f:
        for line in f.readlines():
            line = line.replace("\n", "").replace("\r", "").replace("\t", "").replace(" ", "").replace("　", "")
            line = line.split("#")[0]   #清除註解
            if (len(line) > 0):
                split_line = line.split("=")
                name = split_line[0]
                if (upper):
                    data[name] = split_line[1].upper().split(",")
                else:
                    data[name] = split_line[1].lower().split(",")
    return data


"""
取得一個頁面裡所有的html資料
"""
def get_url_data(url):
    #get http requests
    req = requests.get(url)
    if (req.status_code != 200): #如果沒有連線上 6成都是url錯，剩下的就是網路或WebServer的問題
        raise "input URL error or internet and WebServer no open and not link"
        wait(20)
        sys.exit()
    return req.text


"""
parse到yahoo stock的所有類股的網址，方便根據這些網址去爬股價
"""
@spend_time
def parse_stock_crawler(text, name_list):
    all_url_dict = dict()
    soup = BeautifulSoup(text, 'lxml')
    for name, tag in name_list.items():
        url_dict = dict()
        paragraph = soup.find(id=tag)
        rows = paragraph.find_all("a", href=re.compile("/class"))
        for row in rows:
            url_dict[row.string] = row.get("href")
        all_url_dict[name] = url_dict
    return all_url_dict


def get_page_stock(url):
    stock = dict()
    text = get_url_data(url)
    soup = BeautifulSoup(text, 'lxml')

    #get header
    header_text = soup.find("div", class_="table-header-wrapper")
    header = get_stock_row_header(header_text)
    #print(header)

    #get all row data
    paragraph = soup.find("div", class_="table-body-wrapper")
    rows = paragraph.find_all("div", class_="table-row")
    for row in rows:
        data = get_stock_row_data(row)
        head_value = dict()
        for i in range(1, len(header)):
            head_value[header[i]] = data[i]
        stock[data[0]] = head_value
    return stock


def get_stock_row_header(rows):
    header = list()
    for row in rows.find_all("div", class_="Fxs(0)"):
        header.append(row.string)
    return header


def get_stock_row_data(row):
    row_data = list()
    title = row.find_all(["div", "span"], class_="Ell")
    chinese_name = title[0].string
    stock_codename = title[1].string
    row_data.append(f"{chinese_name}/{stock_codename}")
    #print(row_data[-1])

    data_list = row.find_all("div", class_="Fxs(0)")
    first_None = True
    for data in data_list:
        if (data.string is None and first_None):
            first_None = False
        elif (first_None is False):
            string = data.string
            if (not string is None):
                string = string.replace(",", "")
            row_data.append(string)
    return row_data


def get_up_stop(url):
    setting = load_setting("./", "setting.txt")
    percentage_thrshold = float(setting["percentage_thrshold"][0])
    stock_data = get_page_stock(url)
    for name, data in stock_data.items():
        try:
            now = float(data["股價"])
            yesteday = float(data["昨收"])
            gap = yesteday - now
            percentage = (gap / yesteday) * 100
            if (percentage > percentage_thrshold):
                print(f"{name}, 股價:{data['股價']}, 昨收:{data['昨收']}, 漲幅:{round(percentage, 5)}")
        except:
            #print(name, "error")
            pass


@spend_time
def main():
    host = "https://tw.stock.yahoo.com"
    name_list = load_setting("./", "name.txt")
    setting = load_setting("./", "setting.txt")
    WORKER_NUM = int(setting["worker_num"][0])

    print(setting)

    all_url_text = get_url_data(r"https://tw.stock.yahoo.com/class")
    stock_url_dict = parse_stock_crawler(all_url_text, name_list)
    
    url_list = list()
    for get_name in setting["crawler_name"]:
        for name, url in stock_url_dict[get_name].items():
            url = urllib.parse.urljoin(host, url)
            url_list.append(url)
            #get_up_stop(url)


    #"""
    with Pool(processes=WORKER_NUM) as pool:
        pool.map(get_up_stop, url_list)
    #"""
    


#dleay time
def wait(s):
    print(f"等待 {s} 秒後結束....")
    time.sleep(s)


if (__name__ == "__main__"):
    main()
    #input("press key of enter to continue...")