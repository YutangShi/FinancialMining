

import pandas as pd
from selenium import webdriver
import numpy as np
import sys
import pymysql
from selenium.webdriver.firefox.options import Options
sys.path.append('/home/linsam/github/FinancialMining/CrawlerCode')
sys.path.append('/home/linsam/github/FinancialMining/FinancialOpenData')
import load_data
import BasedClass
from Key import host,user,password


'''
database = 'Financial_DataSet'
self = CrawlerStockID(host,user,password,database)
'''

class CrawlerStockID:
    def __init__(self,host,user,password,database):
        
        Firefox_options = Options()
        Firefox_options.add_argument("--headless")
        Firefox_options.add_argument("--window-size=1920x1080")
        
        self.driver = webdriver.Firefox(firefox_options=Firefox_options)        
        #self.driver = webdriver.Firefox()
        self.host = host
        self.user = user
        self.password = password
        self.database = database
    
    #-------------------------------------------------------------------
    def find_stock_class_name(self):
        url = 'https://goodinfo.tw/StockInfo/StockList.asp'
        
        self.driver.get(url)
        stock_class_name = []
        for i in range(2,11):
            #print(i)
            # //*[@id="STOCK_LIST_ALL"]/tbody/tr[2]
            # //*[@id="STOCK_LIST_ALL"]/tbody/tr[10]
            stock_index = self.driver.find_element_by_xpath(
                    '//*[@id="STOCK_LIST_ALL"]/tbody/tr['+str(i)+']')
            x = stock_index.text
            x = x.split(' ')
            for te in x:
                if te !='': stock_class_name.append( te )
        #return stock_class_id
        self.stock_class_name = stock_class_name
    
    def find_all_stock_id(self,i):
        url = 'https://goodinfo.tw/StockInfo/StockList.asp?MARKET_CAT=全部&INDUSTRY_CAT='
        #url = url + '00'
        url = url + str( self.stock_class_name[i] )# '半導體業'
        url = url + '&SHEET=交易狀況&SHEET2=日&RPT_TIME=最新資料'
    
        self.driver.get(url)
        
        tem = self.driver.find_elements_by_class_name('solid_1_padding_3_1_tbl')
        try:
            tex = tem[1]
        except:
            return 0,0,0
        tem2 = tex.text.split('\n')
        
        stock_id = []
        stock_name = []
        stock_class = []
        for tex in tem2:
            if tex == '代號' or tex == '名稱':
                123
            else:
                #print(tex)
                te = tex.split(' ')
                stock_id.append(te[0])
                stock_name.append(te[1])
                stock_class.append(self.stock_class_name[i])
                
        return stock_id,stock_name,stock_class

    def run(self):
        self.find_stock_class_name()

        stock_id,stock_name,stock_class = [],[],[]
        for i in range(len(self.stock_class_name)):
        #for i in range(3): # for test
            print(str(i)+'/'+str(len(self.stock_class_name)))
            tem_stock_id,tem_stock_name,tem_stock_class = self.find_all_stock_id(i)
            if tem_stock_id == 0:
                print('break')
            else:
                stock_id.append(tem_stock_id)
                stock_name.append(tem_stock_name)
                stock_class.append(tem_stock_class)
            
        stock_id = np.concatenate(stock_id, axis=0)
        stock_name = np.concatenate(stock_name, axis=0)
        stock_class = np.concatenate(stock_class, axis=0)
        
        self.stock_info = pd.DataFrame({'stock_id':stock_id,
                                        'stock_name':stock_name,
                                        'stock_class':stock_class})
        self.driver.quit()
    def upload_stock_info2sql(self):

        conn = ( pymysql.connect(host = self.host,# SQL IP
                                 port = 3306,
                                 user = self.user,
                                 password = self.password,
                                 database = self.database,  
                                 charset = "utf8") )       
        
        # upload stock info to sql
        for i in range(len(self.stock_info)):
            print(i)
            ( conn.cursor().execute( 'insert into ' + 'StockInfo2' + 
                         '(stock_id,stock_name,stock_class)'+' values(%s,%s,%s)',
                          (self.stock_info['stock_id'][i],
                           self.stock_info['stock_name'][i],
                           self.stock_info['stock_class'][i]) ) )
        conn.commit()
        conn.close()    

def main():
    database = 'Financial_DataSet'
    CSID = CrawlerStockID(host,user,password,database)
    CSID.run()

    C2S = BasedClass.Crawler2SQL('StockInfo','Financial_DataSet')
    try:
        C2S.create_table(CSID.data.columns)
    except:
        123

    # upload stock info
    load_data.execute_sql2(database,'TRUNCATE table `StockInfo` ')
    CSID.upload_stock_info2sql()    
    # 
    
if __name__ == '__main__':
    main()
    
    
    
    