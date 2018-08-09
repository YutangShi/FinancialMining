#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul  8 22:15:25 2018

@author: linsam
"""


import sys
import pymysql
import datetime
import re
sys.path.append('/home/linsam/github')
import Key
from FinancialMining.OpenData import Load

def execute_sql2(database,sql_text):
    
    conn = ( pymysql.connect(host = Key.host,# SQL IP
                     port = 3306,
                     user = Key.user,# 帳號
                     password = Key.password,# 密碼
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

class Crawler:
    def __init__(self):
        self.stock_info = Load.Load(database = 'StockInfo', load_all = True)
        #self.host = host
        #self.user = user
        #self.password = password
        #self.dataset_name = dataset_name
        #self.database = database
    def get_max_old_date(self,date_name = 'date',
                         datatable = 'BondsInterestRates',
                         select = [],
                         select_value = []):
        # select = ['country','TypesOfBonds']; select_value = ['AUS','ten_years']
        sql_text = "SELECT MAX(" + date_name + ") FROM `" + datatable + "` "
        
        if str( type(select) ) == "<class 'str'>":
            select = [select]     
            
        if str( type(select_value) ) == "<class 'str'>":
            select_value = [select_value]   
            
        if len(select) > 0:
            sql_text = sql_text + ' WHERE ' 
            for i in range(len(select)):
                sql_text = sql_text + '`' + select[i] + '` = "' + select_value[i] + '"'
                if i == (len(select)-1): 123
                else:
                    sql_text = sql_text + ' and '
                    
        tem = execute_sql2(self.database,sql_text)
        old_date = tem[0][0]
        
        return old_date
        
    def date2days(self,date):
        # date = '2018-08-03'
        date = datetime.datetime.strptime(date,'%Y-%m-%d').date()
        value = (date - datetime.date(1970,1,1)).days
        value = value*60*60*24*1000
        return value        
    
    def days2date(self,day):
        #day = 631497600000
        # 60s = 1min
        # 60min = 1hr
        day = int(day)
        day = int( day/1000/60/60/24 )
        value = datetime.date(1970,1,1) + datetime.timedelta(days = day)
        return value 
    
    def check_stock(self,stock,stock_cid):# stock_cid = stock_id.stock_cid
        #stock = '2330'
        tem = list( set([stock]) & set(stock_cid) )
        if len(tem) == 0:
            print( stock + " isn't exist")
            return 0
        else:
            return 1
        
    def change_stock_name(self,stock,stock_id):
        tem = stock_id[ stock_id['stock_cid'] == str(stock)].stock_id
        data_name = tem[ tem.index[0] ]
        data_name = '_' + data_name.replace('.','_')
        
        return data_name
    
    def create_date(self,start):# start = '2018-07-31'
        start = datetime.datetime.strptime( start,"%Y-%m-%d").date() + datetime.timedelta(days = 1)
        end = datetime.date.today()
        
        day_len = (end - start).days 
        date = [ str( start + datetime.timedelta(days = dat) ) for dat in range(day_len) ]
        return date
#------------------------------------------------------------
'''
self = Crawler2SQL(ACSP.data_name,'StockPrice')
'''        
class Crawler2SQL:   
    
    def __init__(self,dataset_name,database):
        self.host = Key.host
        self.user = Key.user
        self.password = Key.password
        self.dataset_name = dataset_name
        self.database = database

    def creat_sql_file(self,sql_string,database):
        
        conn = ( pymysql.connect(host = self.host,# SQL IP
                                 port = 3306,
                                 user = self.user,# 帳號
                                 password = self.password,# 密碼
                                 database = self.database,  # 資料庫名稱
                                 charset="utf8") )   #  編碼           
        c=conn.cursor()
        c.execute( sql_string )
        c.execute('ALTER TABLE `'+self.dataset_name+'` ADD id BIGINT(64) NOT NULL AUTO_INCREMENT PRIMARY KEY;')
        c.close() 
        conn.close()
        
    def create_table(self,colname,text_col = [''],BIGINT_col = [''],dt_col = ['']):
        # colname = data.columns
        sql_string = 'create table ' + self.dataset_name + '('
        
        for col in colname:
            if col in ['date','Date','datetime']:
                sql_string = sql_string + col + ' Date,'
            elif col in text_col:
                sql_string = sql_string + col + ' TEXT(100),'
            elif col in BIGINT_col:
                sql_string = sql_string + col + ' BIGINT(64),'
            elif col in dt_col:
                sql_string = sql_string + col + ' DATETIME,'
            else:
                sql_string = sql_string + col + ' FLOAT(16),'
            
        sql_string = sql_string[:len(sql_string)-1] + ')'
        self.creat_sql_file(sql_string,self.database)  


    def upload2sql(self,data,no_float_col = ['date','Date','datetime'],int_col = [''] ):
       
        def create_upload_string(data,dataset_name,i):# dataset_name = self.dataset_name
            colname = data.columns
            upload_string = ('insert into ' + dataset_name + '(')
            for col in colname:
                if data[col][i] not in ['NaT','']:
                    #col = col.replace(' ','_')
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
                if tem in ['NaT','']:
                    123                     
                elif col in no_float_col:
                    value.append( tem )
                elif col in int_col:
                    value.append( int( tem ) )
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
        data.index = range(len(data))
        for i in range(len(data)):
            #print(str(i)+'/'+str(len(data)))
            #i = 0
            upload_string = create_upload_string(data,self.dataset_name,i)
            value =  create_upload_value(data,i)
            
            ( conn.cursor().execute( upload_string,tuple(value) ) )
             
        conn.commit()
        conn.close()     


def save_crawler_process(datatable_name):# datetable_name = 'StockPrice'
    
    text = 'insert into ' + datatable_name + ' (name,CrawlerDate) values(%s,%s)'
    tem = str( datetime.datetime.now() )
    time = re.split('\.',tem)[0]
    value = (datatable_name,time)
    
    #today = datetime.datetime.now().strftime("%Y-%m-%d")
    conn = ( pymysql.connect(host = Key.host,
                     port = 3306,
                     user = Key.user,
                     password = Key.password,
                     database = 'python', 
                     charset="utf8") )   
    
    cursor = conn.cursor() 
    #---------------------------------------------------------------------------        
    cursor.execute(text,value )
    conn.commit()
    cursor.close()
    conn.close()  
    
def create_datatable(datatable):
    
    sql_string = 'create table '+ datatable +' ( name text(100),CrawlerDate datetime)'
    conn = ( pymysql.connect(host = Key.host,# SQL IP
                             port = 3306,
                             user = Key.user,# 帳號
                             password = Key.password,# 密碼
                             database = 'python',  # 資料庫名稱
                             charset="utf8") )   #  編碼        
    try:
        c=conn.cursor()
        c.execute( sql_string )
        c.execute('ALTER TABLE `' + datatable + '` ADD id BIGINT(64) NOT NULL AUTO_INCREMENT PRIMARY KEY;')
        c.close() 
        conn.close()
    except:
        c.close() 
        conn.close()        
    
    