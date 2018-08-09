


# https://www.xe.com/currencytables/?from=USD&date=1995-11-16
import os
path = os.listdir('/home')[0]
import requests
import sys
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import re
sys.path.append('/home/'+ path +'/github')
from FinancialMining.CrawlerCode import BasedClass

'''
self = CrawlerExchangeRate()

'''

class CrawlerExchangeRate(BasedClass.Crawler):
    
    def __init__(self):
        self.all_country = ['GBP British Pound',
                            'CAD Canadian Dollar',
                            'CNY Chinese Yuan',
                            'EUR Euro',
                            'JPY Japanese Yen',
                            'TWD Taiwanese New Dollar']

    def create_soup(self,country):
        # country = self.all_country[5]
        country = country.split(' ')[0]
        url = 'https://api.ofx.com/PublicSite.ApiService/SpotRateHistory/allTime/USD/'
        url = url + country
        url = url + '?DecimalPlaces=6&ReportingInterval=daily'
        
        res = requests.get(url,verify = True)
        res.encoding = 'big5'      
        soup = BeautifulSoup(res.text, "lxml")        
        return soup                    
    def get_value(self,i):
        
        country = self.all_country[i]#i=1
        soup = self.create_soup(country)
        
        PointInTime = re.findall('"PointInTime":[0-9]*',soup.text)        
        InterbankRate = re.findall('"InterbankRate":[0-9]*[.]*[0-9]*',soup.text)
        InverseInterbankRate = re.findall('"InverseInterbankRate":[0-9]*[.]*[0-9]*',soup.text)

        PointInTime = [ int( pt.replace('"PointInTime":','') ) for pt in PointInTime]
        InterbankRate = [ float( ir.replace('"InterbankRate":','') ) for ir in InterbankRate]
        InverseInterbankRate = [ float( iir.replace('"InverseInterbankRate":','') ) for iir in InverseInterbankRate]        
        date = [ str( self.days2date(pt) ) for pt in PointInTime ]
        
        data = pd.DataFrame()
        data['InterbankRate'] = InterbankRate
        data['InverseInterbankRate'] = InverseInterbankRate
        data['date'] = date
        data['country'] = country
        
        return data
    
    def crawler(self):
        if 'USD US Dollar' in self.all_country: 
            self.all_country.remove('USD US Dollar')
        #------------------------------------------------------------------------
        self.data = pd.DataFrame()
        for i in range(len(self.all_country)):# i = 0
            print(str(i)+'/'+str(len(self.all_country)))
            data = self.get_value(i)
            self.data = self.data.append(data)
        self.data.index = range(len(self.data))
    def main(self):
        self.crawler()

'''

self = AutoCrawlerExchangeRate()

'''
class AutoCrawlerExchangeRate(CrawlerExchangeRate):
    def __init__(self):
        super(AutoCrawlerExchangeRate, self).__init__()        
        self.database = 'Financial_DataSet'
        
    def create_date(self):        
        self.old_date = pd.DataFrame()
        country, date = [], []
        for c in self.all_country:
        #c = self.all_country[0]
            d = self.get_max_old_date(date_name = 'date',
                                      datatable = 'ExchangeRate',
                                      select = 'country',
                                      select_value = c)
            country.append(c)
            date.append(d)
        
        self.old_date['country'] = country
        self.old_date['date'] = date
            
    def main(self):
        self.create_date()
        self.crawler()

        data = pd.DataFrame()
        for i in range(len(self.old_date['country'])):
            print(i)
            country = self.old_date['country'][i]
            tem = self.data[ self.data['country'] == country ]
            date = [ datetime.datetime.strptime(d,'%Y-%m-%d').date() for d in tem['date'] ]
            data = data.append( tem[ [ d > self.old_date['date'][i] for d in date ] ] )
            
        data.index = range(len(data))
        self.data = data
        
def crawler_history():
    date_name = 'ExchangeRate'
    CER = CrawlerExchangeRate()
    CER.main()

    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    try:
        C2S.create_table(CER.data.columns,text_col = ['country'])
    except:
        123
    C2S.upload2sql( CER.data, no_float_col = ['date','country'])
    
    print('create process table')
    BasedClass.create_datatable('ExchangeRate')
    
def auto_crawler_new():
    date_name = 'ExchangeRate'
    ACCOP = AutoCrawlerExchangeRate()
    ACCOP.main()
    
    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    C2S.upload2sql(ACCOP.data, no_float_col = ['date','country'])
    #------------------------------------------------------
    print('save crawler process')
    BasedClass.save_crawler_process('ExchangeRate')    

def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)






