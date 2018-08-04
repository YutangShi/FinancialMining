

import requests
import sys
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import re
sys.path.append('/home/linsam/github')
from FinancialMining.CrawlerCode import BasedClass

'''

self = CrawlerGoldPrice()
self.main()

'''

class CrawlerGoldPrice(BasedClass.Crawler):
    
    def __init__(self):
        super(CrawlerGoldPrice, self).__init__()
        self.url = 'https://data.gold.org/charts/goldprice/jsonp/currency/usd/weight/oz/width/1025/period/'        
        self.database = 'Financial_DataSet'
    
    def millisecond2date(self,ms):
        # ms = 1533081662000
        ms = int(ms)
        date = str( self.days2date(ms) )
        days = int( ms/1000/60/60/24 )
        
        second = ms/1000 - days*60*60*24
        hour = int(second/60/60)
        minute = int( ( second - hour*60*60 )/60 )-1
        
        if hour < 0:
            hour = '00' 
        elif hour < 10:
            hour = '0' + str(hour)
        else:
            hour = str(hour)
            
        if minute < 0:
            minute = '00'   
        elif minute < 10:
            minute = '0'+ str(minute)
        else:
            minute = str(minute)
        #second = (second - hour*60*60 - minute*60)#hour
        value = date + ' ' + hour + ':' + minute + ':00'
        return value

    def get_value(self,start,end):
        start = str( self.date2days( start ) )
        end = str( self.date2days( end ) )

        url = self.url + start + ',' + end + '/'
        res = requests.get(url,verify = True)     
        soup = BeautifulSoup(res.text, "lxml")
        
        tem = re.findall('[0-9]+,[0-9]+.[0-9]+',soup.text)
        data = pd.DataFrame()
        for te in tem:

            dt = self.millisecond2date( te.split(',')[0] )
            gold_price = float( te.split(',')[1] )
            
            value = pd.DataFrame()
            value['datetime'] = [dt]
            value['Price'] = [gold_price]
            
            data = data.append(value)
        data.index = range(len(data))
        
        return data

    def crawler(self):
        self.data = pd.DataFrame()
        self.log = pd.DataFrame()
        # today, the data isn't update, we get yesterday data
        for i in range(len(self.date)-2):
            print(i)
            log = pd.DataFrame()
            start = self.date[i]
            end = self.date[i+1]
            data = self.get_value(start,end)    
            log['start'] = [start]
            log['end'] = [end]
            log['len'] = [len(data)]
            
            self.log = self.log.append(log)
            self.data = self.data.append(data)
            
    def main(self):
        old_date = self.get_max_old_date(date_name = 'datetime', datatable = 'GoldPrice')
        self.date = self.create_date(str(old_date.date()))
        self.crawler()
        
        date = [ datetime.datetime.strptime(d,'%Y-%m-%d %H:%M:%S') for d in self.data.datetime ]
        self.data = self.data[ [ d > old_date for d in date ] ]
        
        self.data.index = range(len(self.data))
        


def crawler_history():
    date_name = 'GoldPrice'
    # get hostory by download https://www.gold.org/data/gold-price file
    data = pd.read_csv('glod.csv',skiprows = 1)
    
    date = [ datetime.datetime.strptime(d,'%Y/%m/%d').date() for d in data.date ]
    data = data[ [ d < datetime.datetime.strptime('2018-1-1','%Y-%m-%d').date() for d in date ] ]    
    data['date'] = [ d.replace('/','-') + ' 00:00:00' for d in data.date ]
    data.columns = ['datetime','Price']
    
    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    try:
        C2S.create_table(data.columns,dt_col = ['datetime'])
    except:
        123
    C2S.upload2sql( data )
    print('create process table')
    BasedClass.create_datatable(date_name)
    
def auto_crawler_new():
    date_name = 'GoldPrice'
    CGP = CrawlerGoldPrice()
    CGP.main()
    
    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    C2S.upload2sql(CGP.data)
    #-------------------------------------------------
    print('save crawler process')
    BasedClass.save_crawler_process(date_name)

def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)
















#e = 1533214382000


















