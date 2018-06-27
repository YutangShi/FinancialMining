

'''
https://www2.moeaboe.gov.tw/oil102/oil2017/A02/A0201/daytable.asp
西德州 WTI
布蘭特 Brent
杜拜   Dubai
'''
import requests
import sys
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import datetime,re
sys.path.append('/home/linsam/github')
sys.path.append('/home/linsam/github/FinancialMining/CrawlerCode')
sys.path.append('/home/linsam/github/FinancialMining/FinancialOpenData')
import stock_sql
import load_data
import Key
import CrawlerStockDividend


host = Key.host
user = Key.user
password = Key.password


class Crawler2SQL(CrawlerStockDividend.Crawler2SQL):    

    def create_table(self,colname):
        # colname = data.columns
        sql_string = 'create table ' + self.dataset_name + '('
        
        for col in colname:
            if col == 'date':
                sql_string = sql_string + col + ' Date,'
            else:
                sql_string = sql_string + col + ' FLOAT(16),'
            
        sql_string = sql_string[:len(sql_string)-1] + ')'
    
        self.creat_sql_file(sql_string,'Financial_DataSet')  

    def upload2sql(self,data ):
       
        def create_upload_string(data,dataset_name,i):
            colname = data.columns
            upload_string = ('insert into ' + dataset_name + '(')
            for col in colname:
                if data[col][i] not in ['NaT','']:
                    upload_string = upload_string+col+','
            upload_string = upload_string[:len(upload_string)-1] +') values('
            
            for col in colname:
                if data[col][i] not in ['NaT','']:
                    upload_string = upload_string+'%s,'
                    
            upload_string = upload_string[:len(upload_string)-1] + ')'
            return upload_string
        
        def create_upload_value(data,i):
            
            colname = data.columns
            value = []
            for col in colname:
                tem = data[col][i]            
                if col in ['date']:
                    value.append( tem )
                else:
                    value.append( float( tem ) )
            return value

        # database = 'Financial_DataSet'
        conn = ( pymysql.connect(host = self.host,# SQL IP
                         port = 3306,
                         user = self.user,
                         password = self.password,
                         database = self.database,  
                         charset="utf8") )             
         
        for i in range(len(data)):
            #print(str(i)+'/'+str(len(data)))
            upload_string = create_upload_string(data,self.dataset_name,i)
            value =  create_upload_value(data,i)
            
            ( conn.cursor().execute( upload_string,tuple(value) ) )
             
        conn.commit()
        conn.close()     


'''

self = CrawlerCrudeOilPrices()
self.create_date()

'''
class CrawlerCrudeOilPrices:
    
    def __init__(self):
        self.url = 'https://www2.moeaboe.gov.tw/oil102/oil2017/A02/A0201/daytable.asp'

    def create_date(self):
        year = [ str(i) for i in range(2000,2018,1) ]
        month = [ '0' + str(i) for i in range(1,10) ]
        [ month.append(str(m)) for m in range(10,13,1)  ]
        days = [ '0' + str(i) for i in range(1,10) ]
        [ days.append(str(d)) for d in range(10,32,1)  ]
        
        self.date = []
        for y in year:
            for m in month:
                for d in days:
                    self.date.append(y+'-'+m+'-'+d)
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
                    value[colname[i]] = [float('nan')]
                value['date'] = date
                #return ''
        
        tem = soup.find_all('td',{'align':"center"})[0].find_all('div')
        
        for i in range(len(colname)):
            try:
                value[colname[i]] = [float( re.search('[0-9]+.[0-9]+',tem[i].text).group(0) )]
            except:
                value[colname[i]] = [float('nan')]
        value['date'] = date

        return value
    
    def crawler(self):
        # 買進金額	賣出金額	買賣差額
        # 西德州 WTI
        # 布蘭特 Brent
        # 杜拜   Dubai
        #------------------------------------------------------------------------
        self.data = pd.DataFrame()
        for i in range(len(self.date)):
        #for i in range(10):
            print(str(i)+'/'+str(len(self.date)))
            value = self.get_value(i)
            #if str(type(value)) != "<class 'str'>":
            self.data = self.data.append(value)

    def main(self):
        self.create_date()
        self.crawler()
        self.data.index = range(len(self.data))

    
    



