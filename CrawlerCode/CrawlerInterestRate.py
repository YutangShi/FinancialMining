

'''
https://www.investing.com/central-banks


'''
#from joblib import Parallel, delayed
#import multiprocessing
import requests
import sys
import datetime
from bs4 import BeautifulSoup
import pandas as pd
sys.path.append('/home/linsam/github')
from FinancialMining.CrawlerCode import BasedClass

'''

self = CrawlerInterestRate()
self.get_full_country_name()

'''

class CrawlerInterestRate(BasedClass.Crawler):

    def __init__(self):
        self.headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                        'Connection': 'keep-alive',
                        'Host': 'www.investing.com',
                        'Referer': 'https://www.investing.com/central-banks/',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
                        'X-Requested-With': 'XMLHttpRequest'
                        }
        index_url = 'https://www.investing.com/central-banks'
        res = requests.get(index_url,verify = True,headers = self.headers)        
        res.encoding = 'big5'
        self.soup = BeautifulSoup(res.text, "lxml")        
        
    def get_full_country_name(self):

        # find full name country
        tem = self.soup.find_all('td',{'class':'bold left noWrap'})
        full_name = [ te.find('a').text.replace("'",'').replace(' ','_') for te in tem ]
        country = [ te.find('span').text.replace('(','').replace(')','') for te in tem ]
        
        full_country = pd.DataFrame( )
        full_country['full_name'] = full_name
        full_country['country'] = country
        
        self.full_country = full_country
            #return full_country
    def get_value(self,i):
        
        def get_url_id_and_country(tem,i):
        
            # find url id and country
            url_id = tem[i].attrs['id']
            url_country = tem[i].text
            
            return url_id,url_country
        #-------------------------------------------------------------------------
        # get url id nad country
        tem = self.soup.find(id="chartPerfTabsMenu").find_all('li')
        url_id, url_country = get_url_id_and_country(tem,i)
        
        # find data
        url = 'https://www.investing.com/central-banks/ajaxchart?bankid=' + url_id
        res = requests.get(url,verify = True,headers = self.headers)

        # read to json
        data = res.json()
        data = data['data']
        
        # json to dataframe 
        data = pd.DataFrame(data)
        data.columns = ['date','interest_rate']
        
        full_country_name = self.full_country[ self.full_country.country == url_country ].full_name
        full_country_name = full_country_name[full_country_name.index[0]]
        data['country'] = url_country
        data['full_country_name'] = full_country_name
        
        # day of int to date
        days = [ str( self.days2date(day) ) for day in data.date ]
        data.date = days

        return data
    
    def crawler(self):

        self.data = pd.DataFrame()
        for i in range(len(self.full_country)):
            #print(i)
            data = self.get_value(i)
            self.data = self.data.append(data)
    
    def main(self):
        self.get_full_country_name()
        self.crawler()
        self.data.index = range(len(self.data))


'''

self = AutoCrawlerInterestRate()

'''
class AutoCrawlerInterestRate(CrawlerInterestRate):
    def __init__(self):
        super(AutoCrawlerInterestRate, self).__init__()    
        self.database = 'Financial_DataSet'

    def crawler(self):

        self.data = pd.DataFrame()
        for i in range(len(self.full_country)):# i=0
            #print(i)
            new_data = self.get_value(i)
            country = new_data.country[0]
            old_date = self.get_max_old_date(datatable = 'InterestRate', 
                                             select = 'country',
                                             select_value = country)# country = 'FED'
            new_date = [ datetime.datetime.strptime( da , '%Y-%m-%d' ).date() for da in new_data.date ]            
            update_date = [ new > old_date for new in new_date ]
            
            if sum(update_date)>0 :
                data = new_data[update_date]
                self.data = self.data.append(data)
        
    def main(self):
        self.get_full_country_name()
        self.crawler()
        self.data.index = range(len(self.data))

def crawler_history():
    
    CIR = CrawlerInterestRate()
    CIR.main()
    
    C2S = BasedClass.Crawler2SQL('InterestRate','Financial_DataSet')
    try:
        C2S.create_table(CIR.data.columns,text_col = ['country','full_country_name'])
    except:
        123
        
    C2S.upload2sql( CIR.data ,no_float_col = ['country','full_country_name','date'])
    print('create process table')
    BasedClass.create_datatable('InterestRate')
    
def auto_crawler_new():
    date_name = 'InterestRate'
    ACIR = AutoCrawlerInterestRate()
    ACIR.main()
    
    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    C2S.upload2sql(ACIR.data,no_float_col = ['country','full_country_name','date'])
    #-------------------------------------------------
    print('save crawler process')
    BasedClass.save_crawler_process('InterestRate')   

def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)





