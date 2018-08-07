
# https://stock.wearn.com/dividend.asp?kind=2317
import os
path = os.listdir('/home')[0]
import requests
import sys
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import pymysql
import re
sys.path.append('/home/'+ path +'/github')
import Key
from FinancialMining.CrawlerCode import BasedClass
from FinancialMining.OpenData import Load

#-----------------------------------------------------------------
# 股東會日期 Shareholders meeting date
# 盈餘配股(元/股)	 Retained_Earnings 公積配股(元/股) Capital_Reserve
# 除權交易日 Ex_right_trading_day
# 員工配股(總張數) total employee bonus stock shares
# 現金股利 Cash dividend
# 除息交易日 Ex-dividend transaction day
# 員工紅利(總金額)(仟元) total employee bonus shares 
# 董監酬勞 (仟元) Directors remuneration

'''

self = CrawlerStockDividend()

'''
class CrawlerStockDividend(BasedClass.Crawler):
    
    def __init__(self):
        super(CrawlerStockDividend, self).__init__()
        self.stock_id_set = self.stock_info.stock_id

    def create_url_set(self):
        
        self.url_set = []
        for j in range(len(self.stock_id_set)):
            #print(str(j)+'/'+str(len(self.stock_id_set)))
            index_url = 'https://stock.wearn.com/dividend.asp?kind='
            index_url = index_url + self.stock_id_set[j]
            self.url_set.append(index_url) 
        
    def get_value(self,k):
        
        def datechabge(date):# date = data.meeting_data
           return np.array( [ str(date[i]).split(' ')[0] for i in range(len(date)) ] )
       
        def take_value(soup,index):# index=4
            tr = soup.find_all('tr',{'class':'stockalllistbg2'})# tr is row
            tr2 = soup.find_all('tr',{'class':'stockalllistbg1'})
            
            data = []
            for i in range(len(tr)):
                td = tr[i].find_all('td')# td is value
                x = td[index].text.replace('\xa0','')
                if x == '':
                    data.append('')
                else:
                    x = x.replace('\n','').replace('\r','').replace(' ','').replace(',','')
                    data.append(x)
                
            for i in range(len(tr2)):
                td = tr2[i].find_all('td')
                x = td[index].text.replace('\xa0','')
                if x == '':
                    data.append('')
                else:
                    x = x.replace('\n','').replace('\r','').replace(' ','').replace(',','')
                    data.append(x)
                
            return data
        
        def change_date2AD(date):
            new_date = []
            for dat in date:
                if dat == '':
                    new_date.append('')
                else:
                    year = ( str( int( dat.split('/')[0] ) + 1911 ) )
                    month = ( dat.split('/')[1] )
                    day = ( dat.split('/')[2] )
                    new_date.append(year+'-'+month+'-'+day)
                    
            return new_date
            
        # index_url = 'https://stock.wearn.com/dividend.asp?kind=1101'
        index_url = self.url_set[k]# k=45
        
        res = requests.get(index_url,verify = True)        
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text,"lxml")

        col_name = ['meeting_data',
                    'Retained_Earnings',
                    'Capital_Reserve',
                    'Ex_right_trading_day',
                    'total_employee_bonus_stock_shares',
                    'Cash_dividend',
                    'Ex_dividend_transaction_day',
                    'total_employee_bonus_shares',
                    'Directors_remuneration']
        
        data = pd.DataFrame()
        for i in range(9):
            value = take_value(soup,i)
            if col_name[i] in ['meeting_data','Ex_right_trading_day','Ex_dividend_transaction_day']:
                value = change_date2AD(value)
            data[col_name[i]] = value
        # sort by date, so translate date type
        data.meeting_data = pd.to_datetime(data.meeting_data)
        data = data.sort_values('meeting_data',ascending=False)
        data.Ex_right_trading_day = pd.to_datetime(data.Ex_right_trading_day)
        data.Ex_dividend_transaction_day = pd.to_datetime(data.Ex_dividend_transaction_day)
        
        data.meeting_data = datechabge( data.meeting_data )
        data.Ex_right_trading_day = datechabge( data.Ex_right_trading_day )
        data.Ex_dividend_transaction_day = datechabge( data.Ex_dividend_transaction_day )
        
        data.index = range(len(data))
        stock_id = index_url.replace('https://stock.wearn.com/dividend.asp?kind=','')
        data['stock_id'] = stock_id 
        data
        return data
    
    def main(self):
        self.create_url_set()
#-----------------------------------------------------------------------------
'''
self = AutoCrawlerStockDividend()

'''
class AutoCrawlerStockDividend(CrawlerStockDividend):
    def __init__(self):
        super(AutoCrawlerStockDividend, self).__init__()   
        self.database = 'Financial_DataSet'
        
    def create_url_set(self):
        
        self.url_set = []
        for j in range(len(self.stock_id_set)):
            #print(str(j)+'/'+str(len(self.stock_id_set)))
            index_url = 'https://stock.wearn.com/dividend.asp?kind='
            index_url = index_url + self.stock_id_set[j]
            self.url_set.append(index_url) 
            
    def get_data_id(self):

        sql_text = "SELECT id FROM `StockDividend` WHERE `meeting_data` = '"
        sql_text = sql_text + str(self.new_date) + "' AND `stock_id` LIKE "
        sql_text = sql_text + self.new_data['stock_id'] 
        self.data_id = BasedClass.execute_sql2(self.database,sql_text)[0][0]     

    def change_sql_data(self,col):# col = change_name[0]
        if str( self.new_data[col] ) == 'NaT':
            return ''
        else:
            tem = str( self.new_data[col] )
            if 'day' in col:
                tem = re.search('[0-9]+-[0-9]+-[0-9]+',tem).group(0)
                
            text = " UPDATE `StockDividend`" + " SET `"  + col + "` = "
            text = text + " " + tem  + " "
            text = text + "   WHERE `id` = " + str(self.data_id) +"; "

            return text        
    def get_new(self):
        def UPDATE_sql(host,user,password,database,sql_text):
            # text = sql_text
            conn = ( pymysql.connect(host = host,
                             port = 3306,
                             user = user,
                             password = password,
                             database = database,  
                             charset="utf8") )  
            cursor = conn.cursor()    
            try:   
                for i in range(len(sql_text)):
                    cursor.execute(sql_text[i])
                conn.commit()
                conn.close()
                return 1
            except:
                conn.close()
                return 0
        
        old_date = Load.Load(database = 'StockDividend', select = self.stock).sort_values('meeting_data')
        self.old_date = str( old_date.iloc[len(old_date)-1]['meeting_data'] )
        self.new_date = self.new_data['meeting_data']
        
        change_name = list( self.new_data.index )
        sql_text = []
        
        if self.old_date == self.new_date:
            [ change_name.remove(col) for col in ['meeting_data','stock_id'] ]
            self.get_data_id()
            for col in change_name:
                tem = self.change_sql_data(col)
                if tem != '':
                    sql_text.append( tem )
            # update new value, 
            # because Ex_right_trading_day & Ex-dividend transaction day 
            # always slower announcement
            UPDATE_sql(Key.host,
                       Key.user,
                       Key.password,
                       self.database,
                       sql_text)
                    
        elif self.old_date < self.new_date:
            # if new date > old data, then add new data
            data = pd.DataFrame(self.new_data)
            data = data.T
            C2S = BasedClass.Crawler2SQL('StockDividend','Financial_DataSet')
            C2S.upload2sql(data,
                           no_float_col = ['meeting_data',
                                           'Ex_right_trading_day',
                                           'Ex_dividend_transaction_day',
                                           'stock_id'])
    def main(self):
        self.create_url_set()
        for i in range(len(self.url_set)):
            print(str(i)+'/'+str(len(self.url_set)))# i=0
            data = self.get_value(i)
            if len(data) == 0 :
                123
            else:
                self.new_data = data.iloc[0]
                self.stock = self.stock_id_set[i]
                self.get_new() 
#-----------------------------------------------------------------------------

def crawler_history():
    CTD = CrawlerStockDividend()
    CTD.main()
    C2S = BasedClass.Crawler2SQL('StockDividend','Financial_DataSet')
    try:
        C2S.create_table()
    except:
        123
    for i in range(len(CTD.url_set)):
        print(str(i)+'/'+str(len(CTD.url_set)))#i=0
        data = CTD.get_value(i)
        C2S.upload2sql(data,
                       no_float_col = ['meeting_data',
                                       'Ex_right_trading_day',
                                       'Ex_dividend_transaction_day',
                                       'stock_id']) 
    print('create process table')
    BasedClass.create_datatable('StockDividend')
    
def auto_crawler_new():

    ACSD = AutoCrawlerStockDividend()
    ACSD.main()
    # save crawler process
    print('save crawler process')
    BasedClass.save_crawler_process('StockDividend')   
    
def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        # python3 /home/linsam/project/Financial_Crawler/CrawlerFinancialStatements.py new
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)



