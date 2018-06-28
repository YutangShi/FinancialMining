

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
        start = datetime.datetime.strptime( '2000-01-01',"%Y-%m-%d").date()
        end = datetime.date.today()
        
        day_len = (end - start).days   
        self.date = [ str( start + datetime.timedelta(days = dat) ) for dat in range(day_len) ]

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

'''

self = AutoCrawlerCrudeOilPrices(host,user,password)

'''
class AutoCrawlerCrudeOilPrices(CrawlerCrudeOilPrices):
    def __init__(self,host,user,password):
        super(AutoCrawlerCrudeOilPrices, self).__init__()        
        self.host = host
        self.user = user
        self.password = password
        self.database = 'Financial_DataSet'
    def get_max_old_date(self):
        sql_text = "SELECT MAX(date) FROM `CrudeOilPrices`"
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
    
    CCOP = CrawlerCrudeOilPrices()
    CCOP.main()
    #CII.data

    C2S = Crawler2SQL(host,user,password,'CrudeOilPrices','Financial_DataSet')
    try:
        C2S.create_table(CCOP.data.columns)
    except:
        123
    
    C2S.upload2sql( CCOP.data )

def auto_crawler_new():
    date_name = 'CrudeOilPrices'
    ACCOP = AutoCrawlerCrudeOilPrices(host,user,password)
    ACCOP.main()

    C2S = Crawler2SQL(host,user,password,date_name,'Financial_DataSet')
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






