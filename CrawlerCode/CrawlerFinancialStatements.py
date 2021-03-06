
import os
path = os.listdir('/home')[0]
import requests
import sys
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
sys.path.append('/home/'+ path +'/github')
from FinancialMining.CrawlerCode import BasedClass
#------------------------------------------------------------------------------
# self = CrawlerFinancialStatements()
class CrawlerFinancialStatements(BasedClass.Crawler):
    
    def __init__(self):
        super(CrawlerFinancialStatements, self).__init__()
        self.stock_id_set = self.stock_info.stock_id

    def create_url(self):
        url = []
        for j in range(len(self.stock_id_set)):#j=0
            print(str(j)+'/'+str(len(self.stock_id_set)))
            index_url = 'https://stock.wearn.com/income.asp?kind='
            index_url = index_url + self.stock_id_set[j]
            
            res = requests.get(index_url,verify = True)        
            res.encoding = 'big5'
            soup = BeautifulSoup(res.text, "lxml")
            tem = soup.find_all('a') 
            
            for i in range(len(tem)):
                if 'Income_detial.asp' in str(tem[i]):
                    #print(str(i)+str(tem[i]))
                    url.append( 'https://stock.wearn.com/' + tem[i]['href'])
                    
        return url
    #---------------------------------------------------------------------
    def take_stock_value(self,url):
        
        res = requests.get(url,verify = True)      
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text, "lxml")
        
        index = [0,1,2,3,5,6,7,10,11,13,14,15,17]
        # 營業收入	營業成本 	營業毛利	 毛利率 營業費用	營業淨利	  營業利益率 
        # 稅前純益 稅前純益率 所得稅	  稅後純益	稅後純益率 EPS    
        stock_value = pd.DataFrame()
        col_name = ['REV','COST','PRO','GM','OE','OI','OIM','BTAX','BTAXM','TAX','NI','NIM','EPS']
        tem = soup.find_all('td')
        try:
            for i in range(len(index)):
                value = tem[ index[i] ].text.replace(',','')
                #print(value)
                if '%' in value:
                    value = value.replace('%','')
                    value = float(value)/100
                elif value == '':
                    value = -1
                else:
                    value = float(value)
                stock_value[col_name[i]] = [value]
                
            return stock_value ,1    
        except:
            return '',0       
        
    def take_year_and_quar(self,url):
        yearquar = re.search('y=[0-9]*',url).group(0).replace('y=','')
        
        quar = int( yearquar[len(yearquar)-2:len(yearquar)] )
        year = int( yearquar[0:len(yearquar)-2] )
        
        return year,quar            
    def crawler(self):
        # main
        self.url_set = self.create_url()

        stock_financial_statements = pd.DataFrame()        
        for i in range(len(self.url_set)) :# i=0
            print(str(i)+'/'+str(len(self.url_set)))
            url = self.url_set[i]
            stock_value,bo = self.take_stock_value(url)
            if bo == 0:
                print('error')
            elif bo == 1:
                stock_value['stock_id'] = re.search('kind=[0-9]*',url).group(0).replace('kind=','')
                stock_value['year'],stock_value['quar'] = self.take_year_and_quar(url)
                stock_value['url'] = url
                stock_financial_statements = stock_financial_statements.append(stock_value)

        stock_financial_statements = stock_financial_statements.sort_values(['stock_id','year','quar'])
        stock_financial_statements.index = range(len(stock_financial_statements))
        self.stock_financial_statements = stock_financial_statements
        # CFS.stock_financial_statements = stock_financial_statements
    #------------------------------------------------------------------------------
    def fix(self):
        def find_error_index(data,error_col,j):
            error_index = []
            for i in range(len(data)):
                #print(i)
                try:
                    float(data[error_col[j]][i])
                    #fix_data = fix_data.append(self.stock_financial_statements.iloc[i])
                except:
                    error_index.append(i)
            return error_index
        #---------------------------------------------------------------------
                        
        col_name = list( self.stock_financial_statements.columns    )
        [col_name.remove(x) for x in ['stock_id','year','quar','url'] ]
        error_col = []
        for i in range(len(col_name)):
            try:
                np.float32(self.stock_financial_statements[col_name[i]])
            except:
                print(col_name[i])
                error_col.append(col_name[i])
        
        error_index = []
        for i in range(len(error_col)):
            print(i)
            tem = find_error_index(self.stock_financial_statements,error_col,i)
            error_index.append(set(tem))
            
        error = []
        for i in range(len(error_index)):
            if i == 0 :
                error = set( error_index[0] & error_index[1] )
            else:
                error = set( error & error_index[i] )
        if len(error) != 0:
            error = list(error)

            fix_data = self.stock_financial_statements.drop(
                    self.stock_financial_statements.index[error])
    
            for i in range(len(col_name)):
                fix_data[col_name[i]] = np.float32(fix_data[col_name[i]])
    
            fix_data.index = range(len(fix_data))
            self.stock_financial_statements = fix_data
            del fix_data
#----------------------------------------------------------------------------------------------------

class AutoCrawlerFinancialStatements(CrawlerFinancialStatements):
    def __init__(self,database):
        self.database = database
        
    def get_stock_id_set(self):
        
        data =  BasedClass.execute_sql2(
                database = self.database,
                sql_text = 'SELECT distinct `stock_id` FROM FinancialStatements')
        
        self.stock_id_set = [ da[0] for da in data]
    
    def get_yearquar(self,stock):
        
        sql_text = "SELECT year,quar FROM FinancialStatements  WHERE stock_id = "
        sql_text = sql_text + stock 
        
        tem =  BasedClass.execute_sql2(
                database = self.database,
                sql_text = sql_text)
        year,quar = [],[]
        for te in tem:
            year.append(te[0]-1911)
            quar.append(te[1])

        return year,quar

    def crawler(self):

        stock_financial_statements = pd.DataFrame()        
        for i in range(len(self.url)) :# 
            print(str(i)+'/'+str(len(self.url)))
            url = self.url[i]
            stock_value,bo = self.take_stock_value(url)
            if bo == 0:
                print('error')
            elif bo == 1:
                stock_value['stock_id'] = re.search('kind=[0-9]*',url).group(0).replace('kind=','')
                stock_value['year'],stock_value['quar'] = self.take_year_and_quar(url)
                stock_value['url'] = url
                stock_financial_statements = stock_financial_statements.append(stock_value)
        if len(stock_financial_statements) > 0:
            stock_financial_statements = stock_financial_statements.sort_values(['stock_id','year','quar'])
            stock_financial_statements.index = range(len(stock_financial_statements))
        #self.stock_financial_statements = stock_financial_statements
        return stock_financial_statements

    def crawler_new_data(self):
        def get_new_url(tem,date):
            url = []
            for i in range(len(tem)):
                if 'Income_detial.asp' in str(tem[i]):
                    #print(i)
                    x = tem[i].text
                    y = int( re.search('[0-9]+年',x).group(0).replace('年','') )
                    q = int( re.search('[0-9]+季',x).group(0).replace('季','') )
                    da = y*10 + q
                    if da not in date:
                        url.append( 'https://stock.wearn.com/' + tem[i]['href'] )    
            return url
        #------------------------------------------------------------------------------
        #------------------------------------------------------------------------------
        self.stock_financial_statements = pd.DataFrame()
        for i in range(len(self.stock_id_set)):
            print(str(i)+'/'+str(len(self.stock_id_set)))
            stock = self.stock_id_set[i]
            year,quar = self.get_yearquar(stock)
            date = [year[i]*10 + quar[i] for i in range(len(year)) ]
            
            index_url = 'https://stock.wearn.com/income.asp?kind='
            index_url = index_url + stock
      
            res = requests.post(index_url,verify = True)  
            res.encoding = 'big5'
            soup = BeautifulSoup(res.text, "lxml")#
            tem = soup.find_all('a') 
            
            self.url = get_new_url(tem,date)
            if self.url == []:
                123
            else:
                tem = self.crawler()
                self.stock_financial_statements = self.stock_financial_statements.append(tem)
        if len(self.stock_financial_statements) > 0:
            self.stock_financial_statements = self.stock_financial_statements.sort_values(['stock_id','year','quar'])
            self.stock_financial_statements.index = range(len(self.stock_financial_statements))
            self.stock_financial_statements['year'] = self.stock_financial_statements['year'] + 1911
            
    def main(self):
        self.get_stock_id_set()
        self.crawler_new_data()
#----------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------
def crawler_history():
    
    CFS = CrawlerFinancialStatements()
    CFS.crawler()
    CFS.fix()
    CFS.stock_financial_statements['year'] = CFS.stock_financial_statements['year'] + 1911
    
    C2S = BasedClass.Crawler2SQL('FinancialStatements','Financial_DataSet')
    try:
        C2S.create_table()
    except:
        123
    C2S.upload2sql(CFS.stock_financial_statements,
                   no_float_col = ['stock_id','url'],
                   int_col = ['year','quar'])
    
    print('create process table')
    BasedClass.create_datatable('FinancialStatements')
    
def auto_crawler_new():
    ACFS = AutoCrawlerFinancialStatements(database = 'Financial_DataSet')
    # self = ACFS
    ACFS.main()
    if len(ACFS.stock_financial_statements) != 0 :    
        try:
            ACFS.fix()
        except:
            123
        if ACFS.stock_financial_statements.columns[0] == 0:
            ACFS.stock_financial_statements = ACFS.stock_financial_statements.T
            
        C2S = BasedClass.Crawler2SQL('FinancialStatements','Financial_DataSet')
        C2S.upload2sql(ACFS.stock_financial_statements,
                       no_float_col = ['stock_id','url'],
                       int_col = ['year','quar'])
    #------------------------------------------------------
    print('save crawler process')
    BasedClass.save_crawler_process('FinancialStatements')   
    
    
def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        # python3 /home/linsam/project/Financial_Crawler/CrawlerFinancialStatements.py new
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)




