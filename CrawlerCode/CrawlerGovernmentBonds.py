
'''
G8
俄羅斯、美國、加拿大、英國、法國、德國、義大利及日本
'''

import os
path = os.listdir('/home')[0]
import requests
import sys
import pandas as pd
import datetime
import re
from bs4 import BeautifulSoup
sys.path.append('/home/'+ path +'/github')
from FinancialMining.CrawlerCode import BasedClass

'''
self = CrawlerGovernmentBonds()
self.main()

'''
class CrawlerGovernmentBonds(BasedClass.Crawler):
    
    def __init__(self):
        super(CrawlerGovernmentBonds, self).__init__()       
        self.database = 'Financial_DataSet'
        
    def get_country_url(self):
 
        index_url = 'https://www.investing.com/rates-bonds/'
        headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'www.investing.com',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
        }
        res = requests.get(index_url,verify = True,headers = headers)  
        soup = BeautifulSoup(res.text, "lxml")
        tem = soup.find_all('option',{'data-country-id':re.compile('[0-9]+')})
        
        country_url = [ 'https://www.investing.com' + te['value'] for te in tem ]
        # G8 and china
        select = ['canada','china','france','germany',
                  'japan','russia','uk','usa','italy']    
        self.country_url = []
        for i in range(len(country_url)):
            tem = country_url[i].replace('https://www.investing.com/rates-bonds/','')
            tem = tem.replace('-government-bonds','')
            if tem in select:
                self.country_url.append(country_url[i])
                
    def get_curr_id_name(self):
        
        #url = self.country_url[6]
        def get_value(url):
            headers = {'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Host': 'www.investing.com',
            'Referer': url,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'}
            
            res = requests.get(url,verify = True,headers = headers)  
            soup = BeautifulSoup(res.text, "lxml")
            
            tem = soup.find_all('span',{'data-id':re.compile('[0-9]*'),
                                        'data-name':re.compile('[-]+')
                                        })
            curr_id = [ te['data-id'] for te in tem ]
            data_name = [ te['data-name'] for te in tem ]
            return curr_id, data_name
        #------------------------------------------------
        self.curr_id, self.data_name = [], []
        for i in range(len(self.country_url)):# i = 6
            print(i)
            url = self.country_url[i]
            ci, dn = get_value(url)
            [ self.curr_id.append(c) for c in ci ]
            [ self.data_name.append(d) for d in dn ]
            
    def get_value(self,cid,header,st_date,end_date):
        def take_value(tem2):
        
            date = int( tem2[0]['data-real-value'] )
            date = int( date/60/60/24 )
            date = str( datetime.date(1970,1,1) + datetime.timedelta(days = date) )
            v = [ float( tem2[i].text ) for i in range(1,5) ]
            price, Open, High, Low = v
            
            Change = float( tem2[5].text.replace('%','').replace(',',''))/100
            
            data = pd.DataFrame( [ date, price, Open, High, Low, Change] ).T
            return data

        bonds_url = 'https://www.investing.com/instruments/HistoricalDataAjax'
        form_data = {'curr_id': cid,
        'header' : header,
        'st_date': st_date,
        'end_date': end_date,
        'interval_sec': 'Daily',
        'sort_col': 'date',
        'sort_ord': 'DESC',
        'action': 'historical_data'}
        
        headers = {'Accept': 'text/plain, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Length': '192',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'www.investing.com',
        'Origin': 'https://www.investing.com',
        'Referer': 'https://www.investing.com/rates-bonds/france-1-month-bond-yield-historical-data',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'}
        
        res = requests.post(bonds_url,
                            verify = True,
                            headers = headers,
                            data = form_data)  
        
        soup = BeautifulSoup(res.text, "lxml")
        tem = soup.find_all('tr')
        
        colname = tem[0].find_all('th')
        colname = [ c.text.replace(' %','Percent') for c in colname ] 

        data = pd.DataFrame()
        for i in range(1,len(tem)-1):
            tem2 = tem[i].find_all('td')
            value = take_value(tem2) 
            data = data.append(value)
        
        data.columns = colname
        data_name = re.search('[0-9]+[-]+[a-zA-Z]+',header).group(0)
        data['data_name'] = data_name
        data['country'] = header.split(data_name)[0].replace(' ','_')
        return data
    
    def get_st_date(self,header):
        return '01/01/1970'
    
    def get_end_date(self):
        
        end_date = datetime.datetime.now().date()
        y = str( end_date.year )
        m = str( end_date.month ) if end_date.month > 9 else '0' + str(end_date.month)
        d = str( end_date.day ) if end_date.day > 9 else '0' + str(end_date.day)
        
        return m + '/' + d + '/' + y
    
    def crawler(self):
        
        data = pd.DataFrame()
        for j in range(len(self.curr_id)):# j = 9
            print(str(j)+'/'+str(len(self.curr_id)))
            cid = self.curr_id[j]
            header = self.data_name[j] + ' Bond Yield Historical Data'
            st_date,end_date = self.get_st_date(header), self.get_end_date()
            
            value = self.get_value(cid,header,st_date,end_date)
            value['curr_id'] = cid
            data = data.append(value)
        data.index = range(len(data))
        self.data = data
        
    def main(self):
        self.get_country_url()
        self.get_curr_id_name()
        self.crawler()

#-------------------------------------------------------------
'''
self = AutoCrawlerFinancialStatements()
self.main()
self.data
'''
class AutoCrawlerGovernmentBonds(CrawlerGovernmentBonds):
    def __init__(self):
        self.database = 'Financial_DataSet'
        
    def get_st_date(self,header):
        data_name = re.search('[0-9]+[-]+[a-zA-Z]+',header).group(0)
        country = header.split(data_name)[0].replace(' ','')
        
        st_date = self.get_max_old_date(date_name = 'date',
                                        datatable = 'GovernmentBonds',
                                        select = ['country','data_name'],
                                        select_value = [country,data_name])
        
        y = str( st_date.year )
        m = str( st_date.month ) if st_date.month > 9 else '0' + str(st_date.month)
        d = str( st_date.day ) if st_date.day > 9 else '0' + str(st_date.day)
        
        return m + '/' + d + '/' + y
    
    def get_curr_id_name(self):
        self.data_name = []
        curr_id = BasedClass.execute_sql2(
                self.database,
                'SELECT DISTINCT `curr_id` FROM `GovernmentBonds` WHERE 1')
        self.curr_id = [ c[0] for c in curr_id ]
        
        country = BasedClass.execute_sql2(
                self.database,
                'SELECT DISTINCT `country` FROM `GovernmentBonds` WHERE 1') 
        country = [ c[0] for c in country ]
        
        for c in country:
            #c = country[0]
            sql_text = ( 'SELECT DISTINCT `data_name` FROM `GovernmentBonds` ' + 
                        'WHERE `country` = "' + c + '"' )
            value = BasedClass.execute_sql2(
                    self.database,
                    sql_text)
            value = [ d[0] for d in value ]
            
            [ self.data_name.append( c + ' ' + v ) for v in value ]
        
    def main(self):
        self.get_curr_id_name()
        self.crawler()
        
#-------------------------------------------------------------
def crawler_history():
    date_name = 'GovernmentBonds'
    self = CrawlerGovernmentBonds()
    self.main()
    
    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    try:
        C2S.create_table(self.data.columns,text_col = ['data_name','country','curr_id'])
    except:
        123
    C2S.upload2sql( self.data,no_float_col = ['Date','data_name','country','curr_id'] )
    print('create process table')
    BasedClass.create_datatable(date_name)
    
def auto_crawler_new():
    date_name = 'GovernmentBonds'
    ACGB = AutoCrawlerGovernmentBonds()
    ACGB.main()
    
    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    C2S.upload2sql(ACGB.data,no_float_col = ['Date','data_name','country','curr_id'])
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









