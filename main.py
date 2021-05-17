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


"""
紀錄花費時間的decorator
"""
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
input:
    string path         file儲存的路徑
    string file_name    file的檔案名稱(需含附檔名)
output:
    dict data
說明:
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
input:
    string url
output:
    string text
說明:
    取得web page html data
"""
def get_url_html_data(url):
    req = requests.get(url)     #get http requests
    if (req.status_code != 200): #如果沒有連線上 6成都是url錯，剩下的就是網路或WebServer的問題
        raise "input URL error or internet and WebServer no open and not link"
    return req.text


"""
input:
    string text     yahoo stock html資料，該html應為沒有任何股價，純粹的前導引網頁
    name_list       附檔name.txt的內容，怕哪天yahoo改格式，所以換成以這樣方式來取得要get html tag的名稱
output:
    dict data:{"上市類股":{"水泥":"url"}}   
說明:
    parse yahoo stock的所有類股的網址，方便根據這些網址去爬股價。同時依照yahoo的分類來分開各類股網址，包含上市類股、上櫃類股、電子產業、概念股、集團股
"""
@spend_time
def parse_stock_crawler(text, name_list):
    all_url_dict = dict()
    soup = BeautifulSoup(text, 'lxml')
    for name, tag in name_list.items():
        url_dict = dict()
        paragraph = soup.find(id=tag)   #取得各類股的div block
        rows = paragraph.find_all("a", href=re.compile("/class"))   #取得<a href="/class"> tag來取得所有url資料
        for row in rows:
            url_dict[row.string] = row.get("href")  #取得href的string
        all_url_dict[name] = url_dict
    return all_url_dict


"""
input:
    string url
output:
    dict stock_data:{"股票名稱":{"股價":"100", "昨收":"90"}}
說明:
    取得當前股價與對應的股票名稱與代號
"""
def get_page_stock(url):
    stock_data = dict()
    text = get_url_html_data(url)
    soup = BeautifulSoup(text, 'lxml')

    #get header
    header_text = soup.find("div", class_="table-header-wrapper")
    header = get_stock_row_header(header_text)
    #print(header)

    #get all row data
    paragraph = soup.find("div", class_="table-body-wrapper")
    if (not paragraph is None):
        rows = paragraph.find_all("div", class_="table-row")    #取得所有股的row集合
        for row in rows:
            data = get_stock_row_data(row)  #取得一個股裡對應資料，如當前股價等
            head_value = dict()
            for i in range(1, len(header)):
                head_value[header[i]] = data[i]
            stock_data[data[0]] = head_value
    return stock_data


"""
input:
    BeautifulSoup rows      由BeautifulSoup所取得的資料
output:
    list header
說明:
    取得股價網頁裡，第一個row裡面的head資料。如類股名稱/代號、股價、昨收、成交量等
"""
def get_stock_row_header(rows):
    header = list()
    for row in rows.find_all("div", class_="Fxs(0)"):
        header.append(row.string)
    return header


"""
input:
    BeautifulSoup row      由BeautifulSoup所取得的資料
output:
    list row_data
說明:
    取得股價網頁裡，head資料底下的row data，實際取得各類股名稱、昨收、股價等數值資料
"""
def get_stock_row_data(row):
    row_data = list()

    #先取得類股名稱與類股代號
    title = row.find_all(["div", "span"], class_="Ell")
    chinese_name = title[0].string  #類股中文名
    stock_codename = title[1].string    #類股代號
    row_data.append(f"{chinese_name}/{stock_codename}")
    #print(row_data[-1])

    #取得其他資料
    data_row_list = row.find_all("div", class_="Fxs(0)")
    first_None = True
    for data in data_row_list:
        if (data.string is None and first_None):    #由於當前會多一個空白資料，所以這邊會先過濾掉
            first_None = False
        elif (first_None is False):
            string = data.string
            if (not string is None):
                string = string.replace(",", "")    #當前會有1,000.00的這種資料，所以要replace掉','
            row_data.append(string)
    return row_data



"""
input:
    dict stock_data
    dict setting
output:
    null
說明:
    功能-顯示漲幅或跌幅達一定水準的股票資料。透過setting["_percentage_thrshold"]控制水準。
    利用get_page_stock功能實現。
"""
def get_stop(stock_data, setting):
    up_percentage_thrshold = float(setting["up_percentage_thrshold"][0])
    down_percentage_thrshold = float(setting["down_percentage_thrshold"][0]) * (-1)
    for name, data in stock_data.items():
        try:
            now = float(data["股價"])
            yesteday = float(data["昨收"])
            percentage = ((now - yesteday) / yesteday) * 100
            if (percentage > up_percentage_thrshold):
                print(f"(漲幅通知){name}, 當前股價:{data['股價']}, 昨收:{data['昨收']}, 漲幅:{round(percentage, 5)}")
            elif (percentage < down_percentage_thrshold):
                print(f"(跌幅通知){name}, 當前股價:{data['股價']}, 昨收:{data['昨收']}, 跌幅:{round(percentage, 5)}")
        except:
            #print(name, "error")   #會有資料的股價、昨收是空值，所以需要這樣try、except
            pass


"""
input:
    string url
output:
    null
說明:
    集結執行功能
"""
def run(url):
    stock_data = get_page_stock(url)
    setting = load_setting("./", "setting.txt")
    get_stop(stock_data, setting)


@spend_time
def main():
    host = "https://tw.stock.yahoo.com"
    name_list = load_setting("./", "name.txt")
    setting = load_setting("./", "setting.txt")
    WORKER_NUM = int(setting["worker_num"][0])

    print(setting)

    all_url_text = get_url_html_data(r"https://tw.stock.yahoo.com/class")
    stock_url_dict = parse_stock_crawler(all_url_text, name_list)
    
    #取得setting裡crawler_name設定要爬的所有網址。
    url_list = list()
    for get_name in setting["crawler_name"]:
        for name, url in stock_url_dict[get_name].items():
            if (not name in setting["not_crawler_name"]):
                url = urllib.parse.urljoin(host, url)
                url_list.append(url)


    #"""
    #利用processes poll加快執行速度。
    with Pool(processes=WORKER_NUM) as pool:
        pool.map(run, url_list)
    #"""
    


#dleay time
def wait(s):
    print(f"等待 {s} 秒後結束....")
    time.sleep(s)


if (__name__ == "__main__"):
    main()
    input("press key of enter to continue...")