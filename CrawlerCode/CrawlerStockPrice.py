

from pandas_datareader import data as pdr
import sys
import pandas as pd
import datetime
import fix_yahoo_finance as yf
import numpy as np
import re
sys.path.append('/home/linsam/github')
sys.path.append('/home/linsam/github/FinancialMining/CrawlerCode')
sys.path.append('/home/linsam/github/FinancialMining/FinancialOpenData')
import stock_sql 
from Key import host,user,password
import load_data
import BasedClass

#-------------------------------------------------------------------   
'''
# GET TAIWAN STOCK INFO, TO CRAWLER ALL STOCK PRICE
stock_info = load_data.StockInfo().load()
# get history stock price

self = CrawlerHistoryStockPrice(stock_info)
self.main(0)
'''
class CrawlerHistoryStockPrice:

    def __init__(self,stock_info):
        #super(CrawlerHistoryStockPrice, self).__init__()   
        self.stock_info = stock_info
        yf.pdr_override() # <== that's all it takes :-)   

    def main(self,i):
        end = str( datetime.datetime.now().date() + datetime.timedelta(1) )

        # stock_id = '1593'
        stock = str( self.stock_info.stock_id[i] ) + '.TW'
        stock2 = str( self.stock_info.stock_id[i] ) + '.TWO'
        #print(stock_info.stock_id[i])
        bo = 1
        while( bo ):
            # crawler data
            data = pdr.get_data_yahoo(stock, start = '1900-1-10', end = end)
            data['stock_id'] = stock
            self.dataset_name = '_'+stock
            if len(data) == 0:
                data = pdr.get_data_yahoo(stock2, start = '1900-1-10', end = end)
                data['stock_id'] = stock2
                self.dataset_name = '_'+stock2
            if len(data) != 0:
                bo=0

        data['Date'] = data.index
        data.index = range(len(data))
        data['Adj_Close'] = data['Adj Close']# space don't input mysql col name
        del data['Adj Close']        
        data.Date = np.array( [ str(data.Date[i]).split(' ')[0] for i in range(len(data)) ] )
        self.data = data


#------------------------------------------------------------------------
'''
# stock = '0050'
self = AutoCrawlerStockPrice(stock,stock_id)

self.get_start_and_today()

'''
class AutoCrawlerStockPrice(BasedClass.Crawler):
    
    def __init__(self,stock,stock_id):
        super(AutoCrawlerStockPrice, self).__init__()
        
        yf.pdr_override()
        self.stock = str( stock )
        self.stock_id = stock_id
          
    def get_start_and_today(self):
        #stock = '2330'
        if self.check_stock(self.stock,self.stock_id.stock_cid) :
            self.data_name = self.change_stock_name(self.stock,self.stock_id)
        else:
            return ''
        
        sql_text = "SELECT `Date` FROM `"+self.data_name+"` ORDER BY `Date` DESC LIMIT 1"
        start = load_data.execute_sql2(
                host = host,
                user = user,
                password = password,
                database = 'StockPrice',
                sql_text = sql_text)

        self.start = str( start[0][0] )
        self.today = str( datetime.datetime.now().date() + datetime.timedelta(days=1) )

    def get_new_data(self):
        
        def datechabge(date):# date = self.data.meeting_data
           date = np.array( [ str(date[i]).split(' ')[0] for i in range(len(date)) ] )
           return date   
        
        tem = self.stock_id[ self.stock_id['stock_cid'] == str(self.stock)]['stock_id']
        self.stock = np.array(tem)[0]
        
        self.data = pdr.get_data_yahoo( self.stock, start =  self.start, end = self.today)
        #self.data = pdr.get_data_yahoo( '2330.TW', start =  '2018-4-9', end = '2018-5-9')
        #------------------------------------------------------------------
        self.data['Date'] = self.data.index
        self.data['stock_id'] = self.stock
        self.data = self.data[self.data['Date'] > np.datetime64( self.start )]
        self.data.index = range(len(self.data))
        self.data.Date = datechabge( self.data.Date )
        
        self.data['Adj_Close'] = self.data['Adj Close']
        del self.data['Adj Close']

    def main(self):
        self.get_start_and_today()
        self.get_new_data()
        #self.upload_data2sql()
  
def auto_crawler_new():   
    
    def take_stock_id_by_sql():
        #---------------------------------------------------------------                         
        tem = load_data.execute_sql2(
                host = host,
                user = user,
                password = password,
                database = 'StockPrice',
                sql_text = 'SHOW TABLES')                      
        stock_cid = [ d[0][1:].replace('_','.').split('.')[0] for d in tem ]
        stock_id = [ d[0][1:].replace('_','.') for d in tem ]
        
        stock_id = pd.DataFrame({'stock_id' : stock_id,
                                 'stock_cid' : stock_cid})    
        
        return stock_id
     
    #------------------------------------------------------------------------------
    # main

    print('get stock id')
    stock_id = take_stock_id_by_sql()
    #-----------------------------------------------   
    print( 'crawler data and upload 2 sql' )
    i = 1 
    for stock in stock_id['stock_cid']:# stock = stock_id['stock_cid'][i]
        print(str(i)+'/'+str(len(stock_id)) + ' : ' + stock)
        ACSP = AutoCrawlerStockPrice(stock,stock_id)
        ACSP.main()
        
        if len(ACSP.data) == 0:
            print("data doesn't need to upload")
        else:
            C2S = BasedClass.Crawler2SQL(ACSP.data_name,'StockPrice')
            C2S.upload2sql(ACSP.data,
                           no_float_col = ['Date','stock_id'],
                           int_col = ['Volume'])            
        #BasedClass.save_crawler_process(ACSP.data_name)
        i=i+1
        # stock='0053'
    #------------------------------------------------------
    print('save crawler process')
    BasedClass.save_crawler_process('StockPrice')   

def crawler_history():
    
    # GET TAIWAN STOCK INFO, TO CRAWLER ALL STOCK PRICE
    stock_info = load_data.StockInfo.load()
    # get history stock price
    CHSP = CrawlerHistoryStockPrice(stock_info)
    print( 'crawler data and upload 2 sql' )
    for i in range(len(stock_info)):#i=0
        print(str(i)+'/'+str(len(stock_info)))
        CHSP.main(i)
        dataset_name = CHSP.dataset_name.replace('.','_')
        C2S = BasedClass.Crawler2SQL(dataset_name,'StockPrice')
        try:
            C2S.create_table(CHSP.data.columns,text_col = ['stock_id'],BIGINT_col = ['Volume'])
        except:
            123
        C2S.upload2sql(CHSP.data,
                       no_float_col = ['Date','stock_id'],
                       int_col = ['Volume'])
    print('create process table')
    BasedClass.create_datatable('StockPrice')
    
def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)
    
    
    
