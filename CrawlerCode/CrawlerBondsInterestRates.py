# -*- coding: utf-8 -*-
"""
Created on Mon Aug  6 21:28:34 2018

@author: Owner
"""

# data from 
# https://data.oecd.org/interest/long-term-interest-rates.htm#indicator-chart

import os
path = os.listdir('/home')[0]
import requests
import sys
import pandas as pd
import datetime
sys.path.append('/home/'+ path +'/github')
from FinancialMining.CrawlerCode import BasedClass


'''
self = CrawlerBondsInterestRates()
self.main()
'''
class CrawlerBondsInterestRates(BasedClass.Crawler):
    
    def __init__(self):
        super(CrawlerBondsInterestRates, self).__init__()
        self.url = 'https://data.gold.org/charts/goldprice/jsonp/currency/usd/weight/oz/width/1025/period/'        
        self.database = 'Financial_DataSet'
        self.headers = {'Accept-Language': 'en',
                        'Origin':'https://data.oecd.org',
                        'Referer':'https://data.oecd.org/interest/long-term-interest-rates.htm',
                        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
                        }
    def get_SPandEP(self,country,TypesOfBonds):# country = 'AUS' ; TypesOfBonds = 'ten_years'
        
        def change(date):

            year = str( date.year )
            month = str( date.month )
            if int( month ) < 10: month = '0' + month 
            
            value = year + '-' + month
            return value
        
        old_date = self.get_max_old_date(date_name = 'date',
                                         datatable = 'BondsInterestRates',
                                         select = ['country','TypesOfBonds'],
                                         select_value = [country,TypesOfBonds])
        startPeriod = change(old_date)
        endPeriod = change( datetime.datetime.now().date() )
        
        return startPeriod, endPeriod
        
    def get_all_country_SPandEP(self):
        
        sql_text = 'SELECT DISTINCT `country` FROM `BondsInterestRates` WHERE 1'
        tem = BasedClass.execute_sql2(self.database,sql_text)
        all_country = [ te[0] for te in tem ]
        
        sql_text = 'SELECT DISTINCT `TypesOfBonds` FROM `BondsInterestRates` WHERE 1'
        tem = BasedClass.execute_sql2(self.database,sql_text)
        TypesOfBonds = [ te[0] for te in tem ]
        
        data = pd.DataFrame()
        startPeriod, endPeriod = [],[]
        for a in all_country:
            for t in TypesOfBonds:
                value = pd.DataFrame([a,t]).T
                data = data.append(value)
        
        data.columns = ['country','TypesOfBonds']
        data = data.sort_values('TypesOfBonds')
        data.index = range(len(data))

        for i in range(len( data['TypesOfBonds'].unique() )):
            TypesOfBonds = data['TypesOfBonds'][i]
            
            tem = data[ data['TypesOfBonds'] == TypesOfBonds ] 
            se = [ self.get_SPandEP(c,TypesOfBonds) for c in tem['country'] ]
            
            [ startPeriod.append( tem[0] ) for tem in se ]
            [ endPeriod.append( tem[1] ) for tem in se ]
            
        data['startPeriod'] = startPeriod
        data['endPeriod'] = endPeriod

        return data
        
    def get_value(self,country,startPeriod,endPeriod,TypesOfBonds):
        
        if TypesOfBonds == 'ten_years':
            # ten years
            url = 'https://stats.oecd.org/sdmx-json/data/DP_LIVE/' + country 
            url = url + '.LTINT.TOT.PC_PA.M/OECD?json-lang=en&dimensionAtObservation=allDimensions&'
            url = url + 'startPeriod=' + startPeriod + '&' + 'endPeriod=' + endPeriod
            
        elif TypesOfBonds == 'three_month':
            # short three-month
            url = 'https://stats.oecd.org/sdmx-json/data/DP_LIVE/' + country 
            url = url + '.STINT.TOT.PC_PA.M/OECD?json-lang=en&dimensionAtObservation=allDimensions&'
            url = url + 'startPeriod=' + startPeriod + '&' + 'endPeriod=' + endPeriod
        
        
        res = requests.get(url,verify = True,headers = self.headers)        
        res.encoding = 'big5'
        data = res.json()
        # get value        
        value = data['dataSets'][0]['observations']
        value = pd.DataFrame(value).T
        value = pd.DataFrame(value[0])
        #
        value['country'] = country
        value['TypesOfBonds'] = TypesOfBonds
        value.index = range(len(value))

        value.columns = ['value','country','TypesOfBonds']
        
        date = data['structure']['dimensions']['observation'][5]['values']
        
        value['date'] = [ d['id']+'-01' for d in date ]
        
        return value

    def crawler(self):
        self.data = pd.DataFrame()
        for i in range(len(self.all_country_SPandEP)):
            country = self.all_country_SPandEP['country'][i]
            startPeriod= self.all_country_SPandEP['startPeriod'][i]
            endPeriod = self.all_country_SPandEP['endPeriod'][i]
            TypesOfBonds = self.all_country_SPandEP['TypesOfBonds'][i]
            
            value = self.get_value(country,startPeriod,endPeriod,TypesOfBonds)
            self.data = self.data.append(value)
        
        self.data.index = range(len(self.data))
        #return data    
    
    def main(self):
        self.all_country_SPandEP = self.get_all_country_SPandEP()
        self.crawler()

def crawler_history():
    date_name = 'BondsInterestRates'
    # get hostory by download https://data.oecd.org/interest/long-term-interest-rates.html file
    file_path = '/home/' + path + '/github/FinancialMining/CrawlerCode/'
    file_name = ['long.csv','short.csv']
    value_list = ['three_month','ten_years']
    for i in range(len(file_name)):

        data = pd.read_csv( file_path + file_name[i] ,skiprows = 1)
        
        data = data[['LOCATION','TIME','Value']]
        data.columns = ['country','date','value']
        data['TypesOfBonds'] = value_list[i]
        data.date = [ d+'-01' for d in data.date ]
        
        C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
        try:
            C2S.create_table(data.columns,text_col = ['country','TypesOfBonds'])
        except:
            123
        C2S.upload2sql( data , no_float_col = ['date','country','TypesOfBonds'])
    print('create process table')
    BasedClass.create_datatable(date_name)

def auto_crawler_new():
    date_name = 'BondsInterestRates'
    CBIR = CrawlerBondsInterestRates()
    CBIR.main()
    
    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    C2S.upload2sql(CBIR.data, no_float_col = ['date','country','TypesOfBonds'])
    #-------------------------------------------------
    print('save crawler process')
    BasedClass.save_crawler_process(date_name)

def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)












