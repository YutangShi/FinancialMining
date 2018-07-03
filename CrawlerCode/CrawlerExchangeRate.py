

'''
https://www.xe.com/currencytables/?from=USD&date=1995-11-16

US Dollar
Euro
British Pound
Indian Rupee
Australian Dollar
Canadian Dollar
Singapore Dollar
Swiss Franc
Malaysian Ringgit
Japanese Yen
Chinese Yuan Renminbi
New Zealand Dollar
Hungarian Forint
Hong Kong Dollar
South African Rand
Philippine Piso
Swedish Krona
Indonesian Rupiah
Saudi Arabian Riyal
South Korean Won
Egyptian Pound
Norwegian Krone
Danish Krone
Pakistani Rupee
Israeli Shekel
Chilean Peso
Taiwan New Dollar
Icelandic Krona
Jamaican Dollar
Trinidadian Dollar
Barbadian or Bajan Dollar
Bermudian Dollar
Bahamian Dollar

'''
import requests
import sys
from bs4 import BeautifulSoup
import pandas as pd
import datetime,re
sys.path.append('/home/linsam/github')
sys.path.append('/home/linsam/github/FinancialMining/CrawlerCode')
sys.path.append('/home/linsam/github/FinancialMining/FinancialOpenData')
import stock_sql
import load_data
import Key
#import CrawlerStockDividend
import CrawlerCrudeOilPrices


host = Key.host
user = Key.user
password = Key.password

'''

self = CrawlerExchangeRate()


'''

class CrawlerExchangeRate:
    
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
        
        def days2date(day):
            #day = 631497600000
            day = int( day/1000/60/60/24 )
            value = datetime.date(1970,1,1) + datetime.timedelta(days = day-1)
            return value 
        
        country = self.all_country[i]#i=1
        soup = self.create_soup(country)
        
        PointInTime = re.findall('"PointInTime":[0-9]*',soup.text)        
        InterbankRate = re.findall('"InterbankRate":[0-9]*[.]*[0-9]*',soup.text)
        InverseInterbankRate = re.findall('"InverseInterbankRate":[0-9]*[.]*[0-9]*',soup.text)

        PointInTime = [ int( pt.replace('"PointInTime":','') ) for pt in PointInTime]
        InterbankRate = [ float( ir.replace('"InterbankRate":','') ) for ir in InterbankRate]
        InverseInterbankRate = [ float( iir.replace('"InverseInterbankRate":','') ) for iir in InverseInterbankRate]        
        date = [ str( days2date(pt) ) for pt in PointInTime ]
        
        data = pd.DataFrame()
        data['InterbankRate'] = InterbankRate
        data['InverseInterbankRate'] = InterbankRate
        data['date'] = date
        data['country'] = country
        
        return data
    
    def crawler(self):
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

self = AutoCrawlerExchangeRate(host,user,password)

'''
class AutoCrawlerExchangeRate(CrawlerExchangeRate):
    def __init__(self,host,user,password):
        super(AutoCrawlerExchangeRate, self).__init__()        
        self.host = host
        self.user = user
        self.password = password
        self.database = 'Financial_DataSet'
    def get_max_old_date(self):
        sql_text = "SELECT MAX(date) FROM `ExchangeRate`"
        tem = load_data.execute_sql2(self.host,self.user,self.password,self.database,sql_text)
        self.old_date = tem[0][0]

        
    def create_date(self):
        self.get_max_old_date()
        
        today = datetime.datetime.now().date()
        delta = today - self.old_date
        
        self.date = [ str( self.old_date + datetime.timedelta(i+1) ) for i in range(delta.days-1) ]
            
    def main(self):
        self.create_date()
        self.crawler()
        self.data.index = range(len(self.data))
        

def crawler_history():
    
    CER = CrawlerExchangeRate()
    CER.main()
    #CII.data
    
    C2S = CrawlerCrudeOilPrices.Crawler2SQL(host,user,password,'ExchangeRate','Financial_DataSet')
    try:
        C2S.create_table(CER.data.columns,other_col = ['Country'])
    except:
        123
    
    C2S.upload2sql( CER.data , no_float_col = ['date','Country'])

def auto_crawler_new():
    date_name = 'CrudeOilPrices'
    ACCOP = AutoCrawlerExchangeRate(host,user,password)
    ACCOP.main()

    C2S = CrawlerCrudeOilPrices.Crawler2SQL(host,user,password,date_name,'Financial_DataSet')
    C2S.upload2sql(ACCOP.data)
    #-------------------------------------------------
    # update last renew date
    try:
        sql_string = 'create table '+ date_name +' ( name text(100),CrawlerDate datetime)'
        Key.creat_datatable(host,user,password,'python',sql_string,date_name)
    except:
        123
    text = 'insert into '+ date_name +' (name,CrawlerDate) values(%s,%s)'
    
    tem = str( datetime.datetime.now() )
    time = re.split('\.',tem)[0]
    value = (date_name,time)

    stock_sql.Update2Sql(host,user,password,
                         'python',text,value)   

def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        # python3 /home/linsam/project/Financial_Crawler/CrawlerFinancialStatements.py new
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)






