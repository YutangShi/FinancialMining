


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
    
    def get_all_country(self):
        
        url = 'https://www.ofx.com/en-au/forex-news/historical-exchange-rates/'
        
        res = requests.get(url,verify = True)     
        res.encoding = 'big5'      
        soup = BeautifulSoup(res.text, "lxml")        
    
        self.all_country = []
        tem = soup.find_all('optgroup',{'label':'All Currencies'})
        tem2 = tem[0].find_all('option')
        
        for te in tem2:
            self.all_country.append( te.text )    
    
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
        
        '''
        def myfun(i):
            value = pd.DataFrame( self.get_value(i) )
            return value
        
        #num_cores = multiprocessing.cpu_count()
        self.data = pd.DataFrame()

        results = Parallel(n_jobs=num_cores)(
        delayed(myfun)(i) 
        for i in range(len(self.all_country))
        )
        # list to data frame   
        if len(results) != 0:
            self.data = pd.concat(results)
        '''
        #------------------------------------------------------------------------
        
        self.data = pd.DataFrame()
        for i in range(len(self.all_country)):# i = 0
            print(str(i)+'/'+str(len(self.all_country)))
            data = self.get_value(i)
            self.data = self.data.append(data)
        
    def main(self):
        self.get_all_country()
        self.crawler()
        self.data.index = range(len(self.data))

'''

self = AutoCrawlerExchangeRate()

'''
class AutoCrawlerExchangeRate(CrawlerExchangeRate):
    def __init__(self):
        super(AutoCrawlerExchangeRate, self).__init__()        
        self.database = 'ExchangeRate'
        
    def get_sql_country(self):
        self.get_all_country()
        sql_text = 'SHOW TABLES'
        tem = BasedClass.execute_sql2(self.database,sql_text)
        all_table = [ te[0] for te in tem ]
        all_country = []
        for te in all_table:
            for all_c in self.all_country:
                if te in all_c :
                    all_country.append(all_c)
        return all_country

    def crawler(self):
        if 'USD US Dollar' in self.all_country: 
            self.all_country.remove('USD US Dollar')
        #------------------------------------------------------------------------

        self.data = pd.DataFrame()
        for i in range(len(self.all_country)):# i = 0
            print(str(i)+'/'+str(len(self.all_country)))
            data = self.get_value(i)
            self.data = self.data.append(data)
        
    def create_date(self):
        sql_table_name = self.all_country[0].split(' ')[0]
        self.old_date = self.get_max_old_date(datatable = sql_table_name)
        
        today = datetime.datetime.now().date()
        delta = today - self.old_date
        
        self.date = [ str( self.old_date + datetime.timedelta(i+1) ) for i in range(delta.days-1) ]
            
    def main(self,country):
        self.all_country = [country]
        self.create_date()
        self.crawler()
        
        col = list( self.data.columns )
        col.remove('country')
        self.data = self.data[col]
        date = pd.to_datetime(self.data.date)
        self.data = self.data[ date >= pd.to_datetime(self.date[0]) ]
        
        self.data.index = range(len(self.data))
        

def crawler_history():
    
    CER = CrawlerExchangeRate()
    CER.main()
    #CER.data
    #CER.all_country
    # del data of length(data) < 500
    all_country = list( pd.Series(CER.data['country']).unique() )# country = 'EUR'
    for country in all_country:
        if len( CER.data[CER.data['country'] == country] ) < 500:
            #print( len( CER.data[CER.data['country'] == country] ) )
            all_country.remove(country)

    col = list( CER.data.columns )
    col.remove('country')
    for i in range(len(all_country)):#56
        country = all_country[i]
        sql_name = country.split(' ')[0]
        print(str(i)+'/'+str(len(all_country)))
        C2S = BasedClass.Crawler2SQL(sql_name,'ExchangeRate')
        try:
            C2S.create_table(col)
        except:
            123
        C2S.upload2sql( CER.data[CER.data['country'] == country][col] , no_float_col = ['date','Country'])
        
    print('create process table')
    BasedClass.create_datatable('ExchangeRate')
    
def auto_crawler_new():
    
    ACCOP = AutoCrawlerExchangeRate()
    all_country = ACCOP.get_sql_country()
    # for
    for country in all_country:
        print(country)
        ACCOP.main(country)
        
        date_name = country.split(' ')[0]
        C2S = BasedClass.Crawler2SQL(date_name,'ExchangeRate')
        C2S.upload2sql(ACCOP.data)

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






