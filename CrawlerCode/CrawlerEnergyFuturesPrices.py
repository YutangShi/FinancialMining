
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
self = CrawlerEnergyFuturesPrices()
self.main()

'''
class CrawlerEnergyFuturesPrices(BasedClass.Crawler):
    
    def __init__(self):
        super(CrawlerEnergyFuturesPrices, self).__init__()       
        self.database = 'Financial_DataSet'
        self.based_url = 'https://www.investing.com/commodities/'
        self.kind = 'energies'

    def get_curr_id_name(self):
        
        index_url = self.based_url + self.kind
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
        tem = soup.find_all('span',{'data-id':re.compile('[0-9]+')})
        tem = tem[:7]
        self.data_name = [ te['data-name'] for te in tem ]
        self.curr_id = [ te['data-id'] for te in tem ]
        
    def get_st_date(self,header):
        return '01/01/1970'
        #return '01/01/2018'
    
    def get_end_date(self):
        end_date = datetime.datetime.now().date()
        end_date = end_date + datetime.timedelta(-2)
        y = str( end_date.year )
        m = str( end_date.month ) if end_date.month > 9 else '0' + str(end_date.month)
        d = str( end_date.day ) if end_date.day > 9 else '0' + str(end_date.day)
        
        return m + '/' + d + '/' + y

    def get_value(self,cid,header,st_date,end_date):
        def take_value(tem2):
        
            date = int( tem2[0]['data-real-value'] )
            date = int( date/60/60/24 )
            date = str( datetime.date(1970,1,1) + datetime.timedelta(days = date) )
            v = [ float( tem2[i].text.replace(',','') ) for i in range(1,5) ]
            price, Open, High, Low = v
            
            if tem2[5].text == '-':
                Vol = float(0)
            elif 'K' in tem2[5].text:
                Vol = float( tem2[5].text.replace('K','') )*1000
                
            elif 'M' in tem2[5].text:
                Vol = float( tem2[5].text.replace('M','') )*1000000        
                
            Change = float( tem2[6].text.replace('%','').replace(',',''))/100
            
            data = pd.DataFrame( [ date, price, Open, High, Low,Vol , Change] ).T
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
        'Content-Length': '183',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'www.investing.com',
        'Origin': 'https://www.investing.com',
        'Referer': 'https://www.investing.com/commodities/brent-oil-historical-data',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'}
        
        res = requests.post(bonds_url,
                            verify = True,
                            headers = headers,
                            data = form_data)  
        
        soup = BeautifulSoup(res.text, "lxml")
        tem = soup.find_all('tr')
        
        colname = tem[0].find_all('th')
        colname = [ c.text.replace(' %','Percent').replace('.','') for c in colname ] 

        data = pd.DataFrame()
        for i in range(1,len(tem)-1):
            tem2 = tem[i].find_all('td')
            value = take_value(tem2) 
            data = data.append(value)
        
        if len(data) >0 :
            data.columns = colname
            data_name = header.replace(' Historical Data','')
            data['data_name'] = data_name
            #data['country'] = header.split(' '+data_name)[0]
        return data

    def crawler(self):
        
        data = pd.DataFrame()
        for j in range(len(self.curr_id)):# j = 2; j = 100
            print(str(j)+'/'+str(len(self.curr_id)))
            cid = self.curr_id[j]
            header = self.data_name[j] + ' Historical Data'
            st_date,end_date = self.get_st_date(header), self.get_end_date()
            
            value = self.get_value(cid,header,st_date,end_date)
            value['curr_id'] = cid
            data = data.append(value)
        data.index = range(len(data))
        self.data = data

    def main(self):
        self.get_curr_id_name()
        self.crawler()

#-------------------------------------------------------------
'''
self = AutoCrawlerEnergyFuturesPrices()
self.main()
self.data
'''
class AutoCrawlerEnergyFuturesPrices(CrawlerEnergyFuturesPrices):
    def __init__(self):
        self.database = 'Financial_DataSet'
        
    def get_st_date(self,header):
        data_name = header.replace(' Historical Data','')

        st_date = self.get_max_old_date(date_name = 'date',
                                        datatable = 'EnergyFuturesPrices',
                                        select = ['data_name'],
                                        select_value = [data_name])
        st_date = st_date + datetime.timedelta(1)
        y = str( st_date.year )
        m = str( st_date.month ) if st_date.month > 9 else '0' + str(st_date.month)
        d = str( st_date.day ) if st_date.day > 9 else '0' + str(st_date.day)
        
        return m + '/' + d + '/' + y
    
    def get_curr_id_name(self):
        self.data_name = []
        curr_id = BasedClass.execute_sql2(
                self.database,
                'SELECT DISTINCT `curr_id` FROM `EnergyFuturesPrices` WHERE 1')
        self.curr_id = [ c[0] for c in curr_id ]
        
        for c in self.curr_id:
            #c = country[0]
            sql_text = ( 'SELECT DISTINCT `data_name` FROM `EnergyFuturesPrices` ' + 
                        'WHERE `curr_id` = "' + c + '"' )
            value = BasedClass.execute_sql2(
                    self.database,
                    sql_text)
            value = [ d[0] for d in value ]
            
            [ self.data_name.append( v ) for v in value ]
        
    def main(self):
        self.get_curr_id_name()
        self.crawler()

#-------------------------------------------------------------
def crawler_history():
    date_name = 'EnergyFuturesPrices'
    self = CrawlerEnergyFuturesPrices()
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
    
    date_name = 'EnergyFuturesPrices'
    self = AutoCrawlerEnergyFuturesPrices()
    self.main()
    
    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    C2S.upload2sql(self.data,no_float_col = ['Date','data_name','country','curr_id'])
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















