
# coding: utf-8

# In[ ]:


from bs4 import BeautifulSoup
import time
import urllib.request as req 
import re
from urllib.parse import urlparse
import mysql.connector
import datetime
import socket
from retrying import retry
import requests
#Internal Server Error 500が発生下場合5回までリトライする
@retry(stop_max_attempt_number=5, wait_fixed=500)
def webaccess(id,page):
    time.sleep(0.05)
    #日付取得
    year=datetime.date.today().strftime("%Y")
    month=datetime.date.today().strftime("%m")
    day=datetime.date.today().strftime("%d")
    #30年前からのデータを順に表示するようURL作成
    url="https://info.finance.yahoo.co.jp/history/?code="+str(id)+".T&sy="+str(int(year)-30)+"&sm="+str(month)+"&sd="+str(day)+"&ey="+str(year)+"&em="+str(month)+"&ed="+str(day)+"&tm=d&p="+str(page)
    #webアクセス
    res = req.urlopen(url).read()
    soup = BeautifulSoup(res,"html.parser")
    #要素抽出
    head = soup.findAll("th")
    price = soup.findAll("td")
    #社名取得
    companyname = ''
    name_commit = 0
    if int(page) == 1:
        for i in head:
            name = []
            if re.match(r'<th class="symbol"><h1>(.*)</h1></th>', str(i)):
                name.append(re.sub(r'<th class="symbol"><h1>(.*)</h1></th>', r'\1', str(i)).rstrip('\n'))
                if name_commit == 0 :
                    try:
                        cur.execute('insert into stocks.names (id,name) values ('+str(id)+',"'+str(name[0])+'")')
                        con.commit()
                        print (str(id)+","+str(name[0]))
                        name_commit = 1
                        companyname = name[0]
                    except:
                        con.rollback()
                        return 0      
        if not companyname:
            return 0    
    #価格情報取得    
    lastprice=0
    for i in price:
        td = re.match(r'<td>.*', str(i))
        date = re.match(r'<td>(.*年.*)</td>', str(i))
        if td:
            if date:
                stocks = []
                stocks.append(id)
                stocks.append(companyname)
                stocks.append(re.sub(r'<td>(.*)</td>', r'\1', ((date.group().replace("年","-")).replace("月","-")).replace("日","")).replace(",","") )
            else:
                stocks.append(re.sub(r'<td>(.*)</td>', r'\1', str(i)).replace(",","").rstrip('\n'))
                
            if len(stocks)==9 :
                try:
                    cur.execute('insert into stocks.prices (id,date,open,high,low,close,volume,fixed) values ('+str(stocks[0])+',"'+str(stocks[2])+'",'+str(int(stocks[3]))+','+str(int(stocks[4]))+','+str(int(stocks[5]))+','+str(int(stocks[6]))+','+str(int(stocks[7]))+','+str(int(stocks[8]))+')')
                    con.commit()
                    print(str(stocks[0])+',"'+str(stocks[2])+'",'+str(int(stocks[3]))+','+str(int(stocks[4]))+','+str(int(stocks[5]))+','+str(int(stocks[6]))+','+str(int(stocks[7]))+','+str(int(stocks[8])))
                    lastprice=int(stocks[3])
                    stocks = []
                except:
                    con.rollback()
                    return 0                                            
    return lastprice       

#タイムアウト設定
socket.setdefaulttimeout(36000)
# mysqlに接続する
con = mysql.connector.connect(
     user='name',
     passwd='pass',
     host='localhost',
     db='mysql'
)
# 定期的にpingを送り接続が切れていれば再接続する
con.ping(reconnect=True)
#クエリ実行
cur = con.cursor()
for id in range(1300, 10000):
    time.sleep(0.5)
    for page in range(1, 300):
        if webaccess(id,page)==0:
            break
            
# 接続を閉じる
con.close


