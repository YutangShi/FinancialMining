

'''
https://www2.moeaboe.gov.tw/oil102/oil2017/A02/A0201/daytable.asp
西德州 WTI
布蘭特 Brent
杜拜   Dubai
'''
from joblib import Parallel, delayed
import multiprocessing

import requests
import sys
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import re
sys.path.append('/home/linsam/github')
from FinancialMining.CrawlerCode import BasedClass

'''

self = CrawlerCrudeOilPrices()
self.create_date()

'''
class CrawlerCrudeOilPrices(BasedClass.Crawler):
    
    def __init__(self):
        super(CrawlerCrudeOilPrices, self).__init__()
        self.url = 'https://www2.moeaboe.gov.tw/oil102/oil2017/A02/A0201/daytable.asp'

    '''def create_date(self):
        start = datetime.datetime.strptime( '2000-01-01',"%Y-%m-%d").date()
        end = datetime.date.today()
        
        day_len = (end - start).days   
        self.date = [ str( start + datetime.timedelta(days = dat) ) for dat in range(day_len) ]
        '''
    def create_soup(self,date):
        tem = date.split('-')
        year = str( int( tem[0] ) )
        month = str( int( tem[1] ) )
        day = str( int( tem[2] ) )
        form_data = {
                'opt':'search',
                'setform': 'week',
                'S_year': year,
                'S_month': month,
                'S_day': day
                }
        res = requests.get(self.url,verify = True,data = form_data)     
        #res = requests.post(url,verify = True,data = form_data)  
        res.encoding = 'big5'      
        soup = BeautifulSoup(res.text, "lxml")        
        return soup                    
    def get_value(self,i):
        date = self.date[i]
        soup = self.create_soup(date)
        
        tem = soup.find_all('center')
        value = pd.DataFrame()
        colname = ['WTI','Dubai','Brent']
        for te in tem:
            #print(te.text)
            if '無資料' in te.text:
                for i in range(len(colname)):
                    value[colname[i]] = [float(-1)]
                value['date'] = date
                #return ''
        
        tem = soup.find_all('td',{'align':"center"})[0].find_all('div')
        
        for i in range(len(colname)):
            try:
                value[colname[i]] = [float( re.search('[0-9]+.[0-9]+',tem[i].text).group(0) )]
            except:
                value[colname[i]] = [float(-1)]
        value['date'] = date

        return value
    
    def crawler(self):
        # 買進金額	賣出金額	買賣差額
        # 西德州 WTI
        # 布蘭特 Brent
        # 杜拜   Dubai
        #------------------------------------------------------------------------
        def myfun(i):
            value = pd.DataFrame( self.get_value(i) )
            return value
        
        num_cores = multiprocessing.cpu_count()
        self.data = pd.DataFrame()
        
        results = Parallel(n_jobs=num_cores)(
        delayed(myfun)(i) 
        for i in range(len(self.date))
        )
        # list to data frame   
        if len(results) != 0:
            self.data = pd.concat(results)
        
        # 0:02:39.857333
        #------------------------------------------------------------------------
        '''
        self.data = pd.DataFrame()
        s = datetime.datetime.now()
        for i in range(len(self.date)):# len(self.date)
        #for i in range(10):
            if i % 100 == 0 :
                print(str(i)+'/'+str(len(self.date)))
            value = self.get_value(i)
            #if str(type(value)) != "<class 'str'>":
            self.data = self.data.append(value)
        t = datetime.datetime.now() - s
        print(t)
        # 0:35:33.378418
        '''
    def main(self):
        old_date = datetime.datetime.strptime( '2000-01-01',"%Y-%m-%d").date() + datetime.timedelta(days = -1)
        old_date = str( old_date )
        self.date = self.create_date(old_date)
        self.crawler()
        self.data.index = range(len(self.data))
        
'''

self = AutoCrawlerCrudeOilPrices()

'''
class AutoCrawlerCrudeOilPrices(CrawlerCrudeOilPrices):
    def __init__(self):
        super(AutoCrawlerCrudeOilPrices, self).__init__()    
        self.database = 'Financial_DataSet'

    def main(self):
        self.old_date = str( self.get_max_old_date(datatable = 'CrudeOilPrices') )
        self.date = self.create_date(self.old_date)
        self.crawler()
        self.data.index = range(len(self.data))
        

def crawler_history():
    
    CCOP = CrawlerCrudeOilPrices()
    CCOP.main()
    #CII.data
    C2S = BasedClass.Crawler2SQL('CrudeOilPrices','Financial_DataSet')
    try:
        C2S.create_table(CCOP.data.columns)
    except:
        123
        
    C2S.upload2sql( CCOP.data )
    print('create process table')
    BasedClass.create_datatable('CrudeOilPrices')
    
def auto_crawler_new():
    date_name = 'CrudeOilPrices'
    ACCOP = AutoCrawlerCrudeOilPrices()
    ACCOP.main()

    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    C2S.upload2sql(ACCOP.data)
    #-------------------------------------------------
    print('save crawler process')
    BasedClass.save_crawler_process('CrudeOilPrices')   

def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        # python3 /home/linsam/project/Financial_Crawler/CrawlerFinancialStatements.py new
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)






