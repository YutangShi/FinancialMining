

import pandas as pd
import numpy as np
import pymysql
import re
host = '114.32.89.248'
#host = 'localhost'
user = 'guest'
password = '123'

#---------------------------------------------------------

def execute_sql2(database,sql_text):
    
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
'''
self = LoadData()
'''
class LoadData:
    def __init__(self,database = '',data_name = ''):
        self.data_name = data_name        
        self.database = database   
        tem = execute_sql2(
                database = self.database,
                sql_text = 'SELECT distinct `stock_id` FROM `' + self.data_name + '`')

        self.stock_cid = [ te[0] for te in tem ]        
        self.list_col_name = ''
        self.list_data_name = ''
        self.drop_col = []
        
    def check_stock(self,stock):
        tem = list( set([stock]) & set(self.stock_cid) )
        if len(tem) == 0:
            print( self.stock + " isn't exist")
            return 0
        else:
            return 1

    def get_col_name(self):
       
        tem_col_name = execute_sql2(
                database = self.database,
                sql_text = 'SHOW columns FROM '+ self.data_name )
    
        col_name = []
        for i in range(len(tem_col_name)):
            col_name.append( tem_col_name[i][0] )
        if 'id' in col_name:
            col_name.remove('id')  
        
        for drop in self.drop_col:
            col_name.remove(drop)
        
        self.col_name = col_name
    
    def execute2sql(self,text,col=''):
        tem = execute_sql2(
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
    #-----------------------------------------------------
    def get_data(self,all_data = '',col_name = 'stock_id'):
        
        self.data = pd.DataFrame()
        for j in range(len(self.col_name)):
            #print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + self.data_name
            
            if all_data == 'T': 
                123
            else:
                text = text + " WHERE `" + col_name + "` LIKE '"+str(self.stock)+"'"
                
            self.execute2sql(text,col)

        return self.data
    
    def get_value(self,stock):
        stock = str( stock )
        if self.check_stock(stock) ==0:
            return pd.DataFrame()
        
        self.get_col_name()
        data = self.get_data()
                
        return data
    
    def load(self,stock):

        self.data = pd.DataFrame()
        if str( type(stock) ) == "<class 'str'>":
            stock = [stock]
            
        for st in stock:# stock = ['2330','12']
            self.stock = st
            self.data = self.data.append( self.get_value(st) )
            
        return self.data 
    
    def datalist(self):
        
        tem = execute_sql2(
                database = self.database,
                sql_text = 'SELECT distinct `' + self.list_col_name + '` FROM `' + self.data_name + '`')
        value = [ te[0] for te in tem ]
        return value
#---------------------------------------------------------------

class ClassStockInfo(LoadData):
    def __init__(self):
        super(ClassStockInfo, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'StockInfo')
        self.data_name = 'StockInfo'
        self.list_col_name = 'stock_id'
        
    def load(self,stock):                        
        self.get_col_name()
        self.data = pd.DataFrame()
        
        if str( type(stock) ) == "<class 'str'>":
            stock = [stock]

        for st in stock:# stock = ['2330','2002']
            self.stock = st
            self.data = self.data.append( self.get_value(st) )    
            
        self.data.index = range(len(self.data))
        
        return self.data
    
    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
    

def StockInfo(select = [],load_all = False,datalist = False):
    
    self = ClassStockInfo()  
    stock = select
    data = pd.DataFrame()
    
    if stock != [] and load_all == False and datalist == False:
        data = self.load(stock) # stock = '2002'
        
    elif stock == [] and load_all == True and datalist == False:
        data = self.load_all()
        
    elif stock == [] and load_all == False and datalist == True:
        data = list( self.datalist() )
    
    return data
'''
import sys
sys.path.append('E:/text_mining')
from OpenData import LoadData

data = LoadData.StockInfo('2330')
data = LoadData.StockInfo(['2330','2002'])
data = LoadData.StockInfo(load_all = True)
data = LoadData.StockInfo(datalist = True)

'''
#---------------------------------------------------------------   
# self = StockPrice()
class ClassStockPrice(LoadData):
    #---------------------------------------------------------------    
    def __init__(self):
        super(ClassStockPrice, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'StockInfo')  
        
        self.database = 'StockPrice'
        tem = execute_sql2(database = self.database,
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
    def datalist(self):
        return self.stock_id.stock_cid
        
def StockPrice(select = [],load_all = False,datalist = False):
    
    self = ClassStockPrice()  
    stock = select
    data = pd.DataFrame()
    
    if stock != [] and load_all == False and datalist == False:
        data = self.load(stock)
        
    elif stock == [] and load_all == True and datalist == False:
        data = self.load_all()
        
    elif stock == [] and load_all == False and datalist == True:
        data = list( self.datalist() )
        
    return data

''' 
import sys
sys.path.append('E:/text_mining')
from OpenData import LoadData
data = LoadData.StockPrice('2330')
data = LoadData.StockPrice(['2330','2002'])
data = LoadData.StockPrice(load_all = True)
data = LoadData.StockPrice(datalist = True)
'''
#---------------------------------------------------------------
class ClassFinancialStatements(LoadData):
    def __init__(self):
        super(ClassFinancialStatements, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'FinancialStatements')
        
        self.list_col_name = 'stock_id'
        self.drop_col = ['url']
    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
    
def FinancialStatements(select = [],load_all = False,datalist = False):
    
    self = ClassFinancialStatements()  
    stock = select
    data = pd.DataFrame()
    
    if stock != [] and load_all == False and datalist == False:
        data = self.load(stock)
        
    elif stock == [] and load_all == True and datalist == False:
        data = self.load_all()
        
    elif stock == [] and load_all == False and datalist == True:
        data = list( self.datalist() )
    
    return data
'''
import sys
sys.path.append('E:/text_mining')
from OpenData import LoadData
data = LoadData.FinancialStatements('2330')
data = LoadData.FinancialStatements(['2330','2002'])
data = LoadData.FinancialStatements(load_all = True)
data = LoadData.FinancialStatements(datalist = True)
'''
#---------------------------------------------------------------
class ClassStockDividend(LoadData):
    def __init__(self):
        super(ClassStockDividend, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'StockDividend')
        self.list_col_name = 'stock_id'   
    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
    
def StockDividend(select = [],load_all = False,datalist = False):
    
    self = ClassStockDividend()  
    stock = select
    data = pd.DataFrame()
    
    if stock != [] and load_all == False and datalist == False:
        data = self.load(stock)
        
    elif stock == [] and load_all == True and datalist == False:
        data = self.load_all()
        
    elif stock == [] and load_all == False and datalist == True:
        data = list( self.datalist() )
    
    return data
'''
import sys
sys.path.append('E:/text_mining')
from OpenData import LoadData
data = LoadData.StockDividend('2330')
data = LoadData.StockDividend(['2330','2002'])
data = LoadData.StockDividend(load_all = True)
data = LoadData.StockDividend(datalist = True)
'''
#---------------------------------------------------------------
class ClassInstitutionalInvestors(LoadData):
    def __init__(self):
        super(ClassInstitutionalInvestors, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'InstitutionalInvestors')
        
    def load(self):                        
        self.get_col_name()
        #---------------------------------------------------------------   
        self.data = pd.DataFrame()
        self.data = self.get_data(all_data='T')
                
        return self.data 
    
def InstitutionalInvestors():
    
    self = ClassInstitutionalInvestors()  
    data = pd.DataFrame()
    data = self.load()
        
    return data

'''
import sys
sys.path.append('E:/text_mining')
from OpenData import LoadData

data = LoadData.InstitutionalInvestors()

'''
#---------------------------------------------------------------
# self = ClassCrudeOilPrices()
class ClassCrudeOilPrices(LoadData):
    def __init__(self):
        super(ClassCrudeOilPrices, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'CrudeOilPrices')

    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
    
    def load(self):
        raise(AttributeError, "Hidden attribute")  
    
        
def CrudeOilPrices():
    
    self = ClassCrudeOilPrices()  
    data = pd.DataFrame()
    data = self.load_all()
        
    return data
'''

import sys
sys.path.append('E:/text_mining')
from OpenData import LoadData

data = LoadData.CrudeOilPrices()


'''
#---------------------------------------------------------------
# self = ClassExchangeRate()
class ClassExchangeRate(LoadData):
    
    def __init__(self):
        super(ClassExchangeRate, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'ExchangeRate')
        self.list_col_name = 'country'   
    
    def get_data(self,all_data = '',country = [] ):
        
        self.data = pd.DataFrame()
        for j in range(len(self.col_name)):
            #print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + self.data_name
            
            if all_data == 'T': 
                123
            else:
                text = text + " WHERE `country` LIKE '"+str( country )+"'"
                
            self.execute2sql(text,col)

        return self.data
        
    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
    
    def load(self,country ):                        
        self.get_col_name()
        #---------------------------------------------------------------   
        data = pd.DataFrame()
        if str( type(country) ) == "<class 'str'>":
            country = [country]
            
        for cou in country:# 
            #print(cou)
            value = self.get_data(country = cou)
            data = data.append( value )        
 
        return data 
        
        return value
    
def ExchangeRate(select = [],load_all = False,datalist = False):
    
    self = ClassExchangeRate()  
    country = select
    data = pd.DataFrame()
    
    if country != [] and load_all == False and datalist == False:
        data = self.load(country)
        
    elif country == [] and load_all == True and datalist == False:
        data = self.load_all()
        
    elif country == [] and load_all == False and datalist == True:
        data = list( self.datalist() )
        
    return data

'''
import sys
sys.path.append('/home/sam/github')
from FinancialMining.OpenData import Load

datalist = Load.ExchangeRate(datalist = True)
data = Load.ExchangeRate(datalist[0])
data = Load.ExchangeRate([datalist[0],datalist[1]])
data = Load.ExchangeRate(load_all = True)

'''
#---------------------------------------------------------------
class ClassInterestRate(LoadData):
    def __init__(self):
        super(ClassInterestRate, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'InterestRate')
        self.list_col_name = 'country'   

    def get_data(self,all_data = '',country = [] ):
        
        self.data = pd.DataFrame()
        for j in range(len(self.col_name)):
            #print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + self.data_name
            
            if all_data == 'T': 
                123
            else:
                text = text + " WHERE `country` LIKE '"+str( country )+"'"
                
            self.execute2sql(text,col)

        return self.data
        
    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
    
    def load(self,country ):                        
        self.get_col_name()
        #---------------------------------------------------------------   
        data = pd.DataFrame()
        if str( type(country) ) == "<class 'str'>":
            country = [country]
            
        for cou in country:# 
            #print(cou)
            value = self.get_data(country = cou)
            data = data.append( value )        
 
        return data
        
def InterestRate(select = [],load_all = False,datalist = False):
    
    self = ClassInterestRate()  
    country = select
    data = pd.DataFrame()
    
    if country != [] and load_all == False and datalist == False:
        data = self.load(country)
        
    elif country == [] and load_all == True and datalist == False:
        data = self.load_all()
        
    elif country == [] and load_all == False and datalist == True:
        data = list( self.datalist() )
        
    return data
'''
import sys
sys.path.append('E:/text_mining')
from OpenData import LoadData
data = LoadData.InterestRate('ECB')
data = LoadData.InterestRate(['FED','ECB'])
data = LoadData.InterestRate(load_all = True)
data = LoadData.InterestRate(datalist = True)
'''
#---------------------------------------------------------------
class ClassGoldPrice(LoadData):
    def __init__(self):
        super(ClassGoldPrice, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'GoldPrice')

    def load(self):                        
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data      
 

def GoldPrice(select = [],load_all = False,datalist = False):
    
    self = ClassGoldPrice()  
    data = pd.DataFrame()
    #country = select
    data = self.load()
        
    return data
'''
import sys
sys.path.append('E:/text_mining')
from OpenData import LoadData
data = Lo
'''
#---------------------------------------------------------------
class ClassGovernmentBonds(LoadData):
    def __init__(self):
        super(ClassGovernmentBonds, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'GovernmentBonds')

    def get_data(self , all_data = '' , country = '' , data_name = '' ):
        
        self.data = pd.DataFrame()
        for j in range(len(self.col_name)):
            #print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + self.data_name
            
            if all_data == 'T': 
                123
            else:
                text = text + " WHERE `country` = '"+str( country )+"'"
                text = text + " AND `data_name` = '"+str( data_name )+"'"
                
            self.execute2sql(text,col)

        return self.data
        
    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
    
    def load(self, governmentbond ):                        
        self.get_col_name()
        #---------------------------------------------------------------   
        data = pd.DataFrame()
        if str( type(governmentbond) ) == "<class 'str'>":
            governmentbond = [governmentbond]
            
        for gb in governmentbond:# 
            tem = re.search('[0-9]+',gb).group(0)
            country = gb.split(tem)[0]
            country = country[:len(country)-1]
            data_name = gb.replace(country + ' ','')
            
            value = self.get_data(country = country , 
                                  data_name = data_name )
            data = data.append( value )        
 
        return data  
    
    def datalist(self):
        data_name = []
        curr_id = execute_sql2(
                self.database,
                'SELECT DISTINCT `curr_id` FROM `GovernmentBonds` WHERE 1')
        self.curr_id = [ c[0] for c in curr_id ]
        
        country = execute_sql2(
                self.database,
                'SELECT DISTINCT `country` FROM `GovernmentBonds` WHERE 1') 
        country = [ c[0] for c in country ]
        
        for c in country:
            #c = country[0]
            sql_text = ( 'SELECT DISTINCT `data_name` FROM `GovernmentBonds` ' + 
                        'WHERE `country` = "' + c + '"' )
            value = execute_sql2(
                    self.database,
                    sql_text)
            value = [ d[0] for d in value ]
            [ data_name.append( c + ' ' + v ) for v in value ]     
            
        return data_name
    
def GovernmentBonds(select = [],load_all = False,datalist = False):
    
    self = ClassGovernmentBonds()  
    governmentbond = select
    data = pd.DataFrame()
    
    if governmentbond != [] and load_all == False and datalist == False:
        data = self.load(governmentbond)
        
    elif governmentbond == [] and load_all == True and datalist == False:
        data = self.load_all()
        
    elif governmentbond == [] and load_all == False and datalist == True:
        data = list( self.datalist() )
        
    data = data.drop('curr_id', axis=1)
    
    return data
'''
import sys
sys.path.append('/home/sam/github')
from FinancialMining.OpenData import Load

datalist = Load.GovernmentBonds(datalist = True)
data = Load.GovernmentBonds( datalist[0] )
data = Load.GovernmentBonds([datalist[0],datalist[1]])
data = Load.GovernmentBonds(load_all = True)

data = Load.GovernmentBonds( 'abc' )

'''
#---------------------------------------------------------------

def Load(database = '', select = [], load_all = False, datalist = False):
    
    if database == 'StockInfo':
        data = StockInfo(select = select, load_all = load_all, datalist = datalist)
        
    elif database == 'StockPrice':
        data = StockPrice(select = select, load_all = load_all, datalist = datalist)
    
    elif database == 'FinancialStatements':
        data = FinancialStatements(select = select, load_all = load_all, datalist = datalist)

    elif database == 'StockDividend':
        data = StockDividend(select = select, load_all = load_all, datalist = datalist)

    elif database == 'InstitutionalInvestors':
        data = InstitutionalInvestors()

    elif database == 'CrudeOilPrices':
        data = CrudeOilPrices()

    elif database == 'ExchangeRate':
        data = ExchangeRate(select = select, load_all = load_all, datalist = datalist)
        
    elif database == 'InterestRate':
        data = InterestRate(select = select, load_all = load_all, datalist = datalist)
    
    elif database == 'GoldPrice':
        data = GoldPrice()
        
    else:
        raise(AttributeError, "Hidden attribute")  

    return data
    #123
    
'''

import sys
sys.path.append('/home/linsam/github')
from FinancialMining.OpenData.Load import Load

parameters database : 
    StockInfo, StockPrice, FinancialStatements, 
    StockDividend, InstitutionalInvestors, CrudeOilPrices,
    ExchangeRate, InterestRate, GoldPrice
    
database = 'GoldPrice'

data = Load(database = database, load_all = True)




'''
#---------------------------------------------------------------
#---------------------------------------------------------------






