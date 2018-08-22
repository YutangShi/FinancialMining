
import os
path = os.listdir('/home')[0]
from pandas_datareader import data as pdr
import sys
import pandas as pd
import datetime
import fix_yahoo_finance as yf
import math
sys.path.append('/home/'+ path +'/github')
from FinancialMining.CrawlerCode import BasedClass
#-------------------------------------------------------------------   
'''
# get history stock price

self = CrawlerHistoryStockPrice()
'''
class CrawlerHistoryStockPrice(BasedClass.Crawler):

    def __init__(self):
        super(CrawlerHistoryStockPrice, self).__init__()   
        yf.pdr_override() # <== that's all it takes :-)   

    def create_stock(self):
        stock = self.stock_info['stock_id']
        value = [ s + '.TW'  for s in stock ]
        value2 = [ s + '.TWO'  for s in stock ]
        value.extend( value2 )
        self.stock = value
        #self.stock = self.stock[:20]
    def start(self):
        return '1900-1-10'
    def crawler(self):
        def change_muilt_columns(data):
            column0 = list( set( data.columns.get_level_values(0) ) )
            column1 = list( set( data.columns.get_level_values(1) ) )
            
            new_data = pd.DataFrame()
            for col1 in column1:
                value = pd.DataFrame()
                for col0 in column0:
                    tem2 = pd.DataFrame( data.loc[:,col0][col1] )
                    value[col0] = tem2[col1]
                    #tem2.columns = [col0]
                #col1 = col1.replace('.','_')# MySQL can't input '.'
                value['stock'] = col1
                value['date'] = tem2.index
                value.index = range(len(value))
                new_data = new_data.append( value )  
                
            return new_data
        #----------------------------------------------------------------------------
        end = str( datetime.datetime.now().date() + datetime.timedelta(1) )
        data = pdr.get_data_yahoo( self.stock, start =  self.start(), end = end)
        # data = pdr.get_data_yahoo( ['4430.TW','4430.TWo'], start =  self.start(), end = end)
        new_data = change_muilt_columns(data)
        #--------------------------------------------
        new_data.index = range(len(new_data))
        new_data['date'] = [ str( d.date() ) for d in new_data['date'] ]        
        col = list( new_data.columns )
        col = [ c.replace(' ','_') for c in col ]
        new_data.columns = col        
        #--------------------------------------------
        bo = [ math.isnan( d ) == 0 for d in new_data['Close'] ]
        new_data = new_data[bo]
        self.data = new_data

    def main(self):
        self.create_stock()
        self.crawler()

#------------------------------------------------------------------------
'''
# stock = '0050'
self = AutoCrawlerStockPrice()
self.get_start_and_today()

'''
class AutoCrawlerStockPrice(CrawlerHistoryStockPrice):
    def __init__(self):
        super(AutoCrawlerStockPrice, self).__init__()
        yf.pdr_override()
        
    def create_stock(self):
        sql = 'SELECT DISTINCT `stock` FROM `StockPrice` WHERE 1'
        tem = BasedClass.execute_sql2(
                database = 'Financial_DataSet',
                sql_text = sql)
        
        self.stock = [ t[0] for t in tem ]
        
    def start(self):
        date = []
        for stock in self.stock:
            sql = "SELECT MAX(`date`) FROM `StockPrice` WHERE `stock` = '" + stock + "'"
            tem = BasedClass.execute_sql2(
                    database = 'Financial_DataSet',
                    sql_text = sql)
            date.append( tem[0][0] )
        start = str( max(date) )

        return start
    
    def select_new_data(self):
        
        new_data = pd.DataFrame()
        
        for stock in self.stock:
            sql = "SELECT MAX(`date`) FROM `StockPrice` WHERE `stock` = '" + stock + "'"
            tem = BasedClass.execute_sql2(
                    database = 'Financial_DataSet',
                    sql_text = sql)
            
            max_date = tem[0][0]
            data = self.data[ self.data['stock'] == stock ]
            date = [ datetime.datetime.strptime(d,'%Y-%m-%d').date() > max_date for d in data['date'] ]
            new_data = new_data.append( data[date] )
        
        self.new_data = new_data
        
    def main(self):
        self.create_stock()
        print('crawler')
        self.crawler()
        print('select new data')
        self.select_new_data()

#-------------------------------------------------------------
def crawler_history():
    
    dataset_name = 'StockPrice'
    self = CrawlerHistoryStockPrice()
    self.main()
    print( 'crawler data and upload 2 sql' )
    C2S = BasedClass.Crawler2SQL(dataset_name,'Financial_DataSet')
    try:
        C2S.create_table(self.data.columns,
                         text_col = ['stock'],
                         BIGINT_col = ['Volume'])
    except:
        123
    C2S.upload2sql(self.data,
                   no_float_col = ['date','stock'],
                   int_col = ['Volume'])
    print('create process table')
    BasedClass.create_datatable('StockPrice')
#-------------------------------------------------------------
def auto_crawler_new(): 
    
    dataset_name = 'StockPrice'
    self = AutoCrawlerStockPrice()
    self.main()
    print( 'crawler data and upload 2 sql' )
    C2S = BasedClass.Crawler2SQL(dataset_name,'Financial_DataSet')
    C2S.upload2sql(self.new_data,
                   no_float_col = ['date','stock'],
                   int_col = ['Volume'])
    #------------------------------------------------------
    print('save crawler process')
    BasedClass.save_crawler_process('StockPrice')   

def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)

