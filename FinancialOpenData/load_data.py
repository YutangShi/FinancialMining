

import pandas as pd
import numpy as np
import pymysql
host = '114.34.138.146'
user = 'guest'
password = '123'

#---------------------------------------------------------

def execute_sql2(host,user,password,database,sql_text):
    
    conn = ( pymysql.connect(host = host,# SQL IP
                     port = 3306,
                     user = user,# 帳號
                     password = password,# 密碼
                     database = database,  # 資料庫名稱
                     charset="utf8") )   #  編碼
                             
    cursor = conn.cursor()    
    # sql_text = "SELECT * FROM `_0050_TW` ORDER BY `Date` DESC LIMIT 1"
    try:   
        cursor.execute(sql_text)
        data = cursor.fetchall()
        conn.close()
        return data
    except:
        conn.close()
        return ''
#---------------------------------------------------------
# based class 
class LoadDate:
    def __init__(self,database='',data_name=''):
        self.data_name = data_name        
        self.database = database   
        tem = execute_sql2(
                host = host,
                user = user,
                password = password,
                database = self.database,
                sql_text = 'SELECT distinct `stock_id` FROM `' + self.data_name + '`')

        self.stock_cid = [ te[0] for te in tem ]        

        
    def check_stock(self):
        tem = list( set([self.stock]) & set(self.stock_cid) )
        if len(tem) == 0:
            print( self.stock + " isn't exist")
            return 0
        else:
            return 1

    def get_col_name(self):
       
        tem_col_name = execute_sql2(
                host = host,
                user = user,
                password = password,
                database = self.database,
                sql_text = 'SHOW columns FROM '+ self.data_name )
    
        col_name = []
        for i in range(len(tem_col_name)):
            col_name.append( tem_col_name[i][0] )
        if 'id' in col_name:
            col_name.remove('id')    
        self.col_name = col_name
    
    def execute2sql(self,text,col=''):
        tem = execute_sql2(
            host = host,
            user = user,
            password = password,
            database = self.database,
            sql_text = text)
        
        if col=='Date':
            tem = [np.datetime64(x[0]) for x in tem]
            tem = pd.DataFrame(tem)
            self.data[col] = tem.loc[:,0]
        else:
            tem = np.concatenate(tem, axis=0)
            tem = pd.DataFrame(tem)
            self.data[col] = tem.T.iloc[0]
    
    def get_data(self,all_data = ''):
        
        self.data = pd.DataFrame()
        for j in range(len(self.col_name)):
            #print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + self.data_name
            
            if all_data == 'T': 
                123
            else:
                text = text + " WHERE `stock_id` LIKE '"+str(self.stock)+"'"
                
            self.execute2sql(text,col)

        return self.data
    
    def load(self,stock):
     
        def get_value(stock):
            stock = str( stock )
            if self.check_stock() ==0:
                return pd.DataFrame()
            
            self.get_col_name()
            data = self.get_data()
                    
            return data
        
        data = pd.DataFrame()
        if str( type(stock) ) == "<class 'str'>":
            stock = [stock]
            
        for st in stock:# stock = ['2330','12']
            self.stock = st
            data = data.append( get_value(st) )
            
        return data 
    
    '''def load(self,stock):
        
        self.stock = str( stock )
        if self.check_stock() ==0:
            return pd.DataFrame()
        
        self.get_col_name()
        data = self.get_data()
                
        return data'''
    
#--------------------------------------------------------------- 
''' test StockInfo
SI = cStockInfo()
data = SI.load()
'''
class cStockInfo(LoadDate):
    def __init__(self):
        super(StockInfo, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'FinancialStatements')
        self.data_name = 'StockInfo'
        
    def load(self):                        
        self.get_col_name()
        #---------------------------------------------------------------   
        self.data = pd.DataFrame()
        self.data = self.get_data(all_data='T')
                
        return self.data
    
#-------------------------------------------------------------
''' test StockPrice
self = cStockPrice()  
data = self.load('2330')
data = self.load(['2330','12'])
'''
class cStockPrice(LoadDate):
    #---------------------------------------------------------------    
    def __init__(self):    
        self.database = 'StockPrice'
        tem = execute_sql2(host = host,user = user,password = password,
                           database = self.database,
                           sql_text = 'SHOW TABLES')
        #---------------------------------------------------------------                         
        stock_id,stock_cid = [],[]
        for d in tem:
            d = d[0][1:].replace('_','.')
            stock_id.append( d )
            stock_cid.append( d.split('.')[0] )
    
        self.stock_id = pd.DataFrame({
                'stock_id' : stock_id,
                'stock_cid' : stock_cid})
        #---------------------------------------------------------------
    
    def load(self,stock):# stock = '2330'
        def change_stock_name(stock_id,stock):
            try:
                tem = stock_id[ stock_id['stock_cid'] == str(stock)].stock_id
                data_name = tem[ tem.index[0] ]
                data_name = '_' + data_name.replace('.','_')
                return data_name
            except:
                print( stock + " isn't exist")
                return 0  
            
        def get_value(stock):
        
            stock = str( stock )
            self.data_name = change_stock_name(self.stock_id,stock)
            if self.data_name == 0 :
                return pd.DataFrame()
            self.get_col_name()
        
            self.data = pd.DataFrame()
            for j in range(len(self.col_name)):
                #print(j)
                col = self.col_name[j]
                text = 'select ' + col + ' from ' + self.data_name
                self.execute2sql(text,col)
                    
            return self.data   
        
        data = pd.DataFrame()
        if str( type(stock) ) == "<class 'str'>":
            stock = [stock]

        for st in stock:# stock = ['2330','12']
            data = data.append( get_value(st) )
            
        return data
        
    def load_all(self):

        data = pd.DataFrame()

        for stock in self.stock_id['stock_cid']:
            print(stock)
            data = data.append( self.load(stock) )
            
        return data        
#--------------------------------------------------------------- 
''' test FinancialStatements
self = FinancialStatements()  
data = self.load('2330')# 讀取 2330 歷史財報
data = self.load(['2330','2002'])# 讀取 2330,2002 歷史財報
# 16min 58s
data = self.load_all()# 讀取 '所有股票' 歷史財報
'''    
        
class cFinancialStatements(LoadDate):
    def __init__(self):
        super(FinancialStatements, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'FinancialStatements')
        
    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
        
#--------------------------------------------------------------- 
''' test StockDividend
SD = StockDividend()
data = SD.load('2330')
data.iloc[8]
'''
class cStockDividend(LoadDate):
    def __init__(self):
        super(StockDividend, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'StockDividend')
        #self.data_name = 'StockInfo'    
    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
        
#--------------------------------------------------------------- 
''' test InstitutionalInvestors
II = InstitutionalInvestors()
data = II.load()
'''
class cInstitutionalInvestors(LoadDate):
    def __init__(self):
        super(InstitutionalInvestors, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'InstitutionalInvestors')
    def load(self):                        
        self.get_col_name()
        #---------------------------------------------------------------   
        self.data = pd.DataFrame()
        self.data = self.get_data(all_data='T')
                
        return self.data
        
#--------------------------------------------------------------- 
''' test StockDividend
COP = CrudeOilPrices()
data = COP.load_all()
data
'''
class cCrudeOilPrices(LoadDate):
    def __init__(self):
        super(CrudeOilPrices, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'CrudeOilPrices')
        #self.data_name = 'StockInfo'    
        #del self.load
        #delattr(self, self.load)
    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
    
    def load(self):
        raise(AttributeError, "Hidden attribute")
       
#-------------------------------------------------------------
''' test StockPrice
self = ExchangeRate()  
data = self.load(['GBP'])
data = self.load(['GBP','HKD'])
'''

class cExchangeRate(LoadDate):
    #---------------------------------------------------------------    
    def __init__(self):    
        self.database = 'ExchangeRate'
        tem = execute_sql2(host = host,user = user,password = password,
                           database = self.database,
                           sql_text = 'SHOW TABLES')
        self.all_country = [ te[0] for te in tem ]
        #---------------------------------------------------------------
    
    def load(self,country):# stock = '2330'
        def check(country,all_country):

            if country in all_country:
                return 1
            else:
                print( country + " isn't exist")
                return 0  
            
        def get_value(country):

            bo = check(country,self.all_country)
            if bo == 0 :
                return pd.DataFrame()
            self.data_name = country
            self.get_col_name()
        
            self.data = pd.DataFrame()
            for j in range(len(self.col_name)):
                #print(j)
                col = self.col_name[j]
                text = 'select ' + col + ' from ' + self.data_name
                self.execute2sql(text,col)
            self.data['country'] = country
            
            return self.data   
        
        data = pd.DataFrame()
        if str( type(country) ) == "<class 'str'>":
            country = [country]

        for co in country:# stock = ['2330','12']
            data = data.append( get_value(co) )
            
        return data
        
    def load_all(self):

        data = pd.DataFrame()

        for country in self.all_country:
            print(country)
            data = data.append( self.load(country) )
            
        return data             
#-----------------------------------------------------------
SI = cStockInfo()        
SP = cStockPrice()  
FS = cFinancialStatements()  
SD = cStockDividend()  
II = cInstitutionalInvestors()
COP = cCrudeOilPrices()
ER = cExchangeRate()  

#-----------------------------------------------------------
# for user
StockInfo = cStockInfo()        
StockPrice = cStockPrice()  
FinancialStatements = cFinancialStatements()  
StockDividend = cStockDividend()  
InstitutionalInvestors = cInstitutionalInvestors()
CrudeOilPrices = cCrudeOilPrices()
ExchangeRate = cExchangeRate()  




