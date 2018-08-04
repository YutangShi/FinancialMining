

import requests
import sys
from bs4 import BeautifulSoup
import pandas as pd
sys.path.append('/home/linsam/github')
from FinancialMining.CrawlerCode import BasedClass

'''
self = CrawlerInstitutionalInvestors()
self.main()

'''
class CrawlerInstitutionalInvestors(BasedClass.Crawler):
    def __init__(self):
        super(CrawlerInstitutionalInvestors, self).__init__()
        self.url = 'https://stock.wearn.com/fundthree.asp?mode=search'

    def create_soup(self,date):
        form_data = {
                'yearE': int(date.split('-')[0])-1911,
                'monthE': date.split('-')[1],
                'dayE': date.split('-')[2],
                'Submit1': '(unable to decode value)'
                }
        res = requests.get(self.url,verify = True,data = form_data)     
        #res = requests.post(url,verify = True,data = form_data)  
        res.encoding = 'big5'      
        soup = BeautifulSoup(res.text, "lxml")        
        return soup                    
    def get_value(self,i):#i=0
        date = self.date[i]
        soup = self.create_soup(date)

        if '自營商(自行買賣)' not in soup.text :
            return ''  
        # (億元)
        tem = soup.find_all('td')
        x = []
        for i in range(len(tem)):
            if tem[i].text != '三大法人買賣超(億)':
                te = tem[i].text
                for tex in ['\xa0','+',' ',','] :
                    te = te.replace(tex,'')
                x.append( te ) 
            else:
                break
        x = x[1:]
        buy_set,sell_set,difference_set = [],[],[]
        
        for i in [ 1+i*4 for i in range(5) ]:
            try:
                buy_set.append(float(x[i]))
                sell_set.append(float(x[i+1]))
                difference_set.append(float(x[i+2]))
            except:
                buy_set.append(float(0))
                sell_set.append(float(0))
                difference_set.append(float(0))
        
        buy_set[1] = buy_set[0]+buy_set[1]
        buy_set = buy_set[1:]
        sell_set[1] = sell_set[0]+sell_set[1]
        sell_set = sell_set[1:]
        difference_set[1] = difference_set[0]+difference_set[1]
        difference_set = difference_set[1:]            
                
        colname = ['Dealer_buy','Dealer_sell','Dealer_difference',
                   'Investment_Trust_buy','Investment_Trust_sell','Investment_Trust_difference',
                   'Foreign_Investor_buy','Foreign_Investor_sell','Foreign_Investor_difference',
                   'total_buy','total_sell','total_difference']
        value = pd.DataFrame()
        for i in range(len(buy_set)):
            value[colname[0+i*3]] = [buy_set[i]]
            value[colname[1+i*3]] = [sell_set[i]]
            value[colname[2+i*3]] = [difference_set[i]]
        value['date'] = date
        return value
    def crawler(self):
        # 買進金額	賣出金額	買賣差額
        # 自營商 Dealer
        # 投信 Investment Trust 
        # 外資 Foreign Investor
        #------------------------------------------------------------------------
        self.data = pd.DataFrame()
        for i in range(len(self.date)):
        #for i in range(10):
            print(str(i)+'/'+str(len(self.date)))
            value = self.get_value(i)
            if str(type(value)) != "<class 'str'>":
                self.data = self.data.append(value)
        # 93+1911
    def main(self):
        self.date = self.create_date('2004-01-01')
        self.crawler()
        self.data.index = range(len(self.data))
        
        
'''
self = AutoCrawlerInstitutionalInvestors()
self.main()

'''
class AutoCrawlerInstitutionalInvestors(CrawlerInstitutionalInvestors):
    def __init__(self):
        super(AutoCrawlerInstitutionalInvestors, self).__init__()        
        
        self.database = 'Financial_DataSet'

    def main(self):
        self.old_date = str( self.get_max_old_date(datatable = 'InstitutionalInvestors') )
        self.date = self.create_date(self.old_date)
        self.crawler()
        self.data.index = range(len(self.data))
        
        
def crawler_history():
    
    CII = CrawlerInstitutionalInvestors()
    CII.main()
    #CII.data

    C2S = BasedClass.Crawler2SQL('InstitutionalInvestors','Financial_DataSet')
    try:
        C2S.create_table(CII.data.columns)
    except:
        123
    
    C2S.upload2sql(CII.data)
    print('create process table')
    BasedClass.create_datatable('InstitutionalInvestors')
    
def auto_crawler_new():
    ACII = AutoCrawlerInstitutionalInvestors()
    ACII.main()

    C2S = BasedClass.Crawler2SQL('InstitutionalInvestors','Financial_DataSet')
    C2S.upload2sql(ACII.data)

    print('save crawler process')
    BasedClass.save_crawler_process('InstitutionalInvestors')   
    
    
def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        # python3 /home/linsam/project/Financial_Crawler/CrawlerFinancialStatements.py new
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)

