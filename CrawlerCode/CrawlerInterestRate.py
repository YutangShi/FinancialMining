
'''
https://www.xe.com/currencytables/?from=USD&date=1995-11-16

US Dollar
Euro
British Pound
Indian Rupee
Australian Dollar
Canadian Dollar
Singapore Dollar
Swiss Franc
Malaysian Ringgit
Japanese Yen
Chinese Yuan Renminbi
New Zealand Dollar
Hungarian Forint
Hong Kong Dollar
South African Rand
Philippine Piso
Swedish Krona
Indonesian Rupiah
Saudi Arabian Riyal
South Korean Won
Egyptian Pound
Norwegian Krone
Danish Krone
Pakistani Rupee
Israeli Shekel
Chilean Peso
Taiwan New Dollar
Icelandic Krona
Jamaican Dollar
Trinidadian Dollar
Barbadian or Bajan Dollar
Bermudian Dollar
Bahamian Dollar

'''
from joblib import Parallel, delayed
import multiprocessing
import requests
import sys
from bs4 import BeautifulSoup
import pandas as pd
sys.path.append('/home/linsam/github/FinancialMining/CrawlerCode')
sys.path.append('/home/linsam/github/FinancialMining/FinancialOpenData')
import load_data
import BasedClass


'''
self = CrawlerInterestRate()

'''

class CrawlerInterestRate(BasedClass.Crawler):

    def __init__(self):
        self.headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                        'Connection': 'keep-alive',
                        'Host': 'www.investing.com',
                        'Referer': 'https://www.investing.com/central-banks/',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
                        'X-Requested-With': 'XMLHttpRequest'
                        }
    
    def get_value(self):
        def get_full_country_name(headers):
            index_url = 'https://www.investing.com/central-banks'
    
            res = requests.get(index_url,verify = True,headers = headers)        
            res.encoding = 'big5'
            soup = BeautifulSoup(res.text, "lxml")
    
            # find full name country
            tem = soup.find_all('td',{'class':'bold left noWrap'})
            full_name = [ te.find('a').text for te in tem ]
            country = [ te.find('span').text.replace('(','').replace(')','') for te in tem ]
        
            full_country = pd.DataFrame( )
            full_country['full_name'] = full_name
            full_country['country'] = country
            
            return full_country
# find url id and country
tem = soup.find_all(id="chartPerfTabsMenu")[0]
tem = tem.find_all('li')
url_id = tem[0].attrs['id']
url_country = tem[0].text

# find data
url = 'https://www.investing.com/central-banks/ajaxchart?bankid=' + url_id
res = requests.get(url,verify = True,headers = headers)

# read to json
data = res.json()
data = data['data']

# json to dataframe 
data = pd.DataFrame(data)
data.columns = ['date','interest_rate']
data['country'] = url_country
# day of int to date


days = [ test(day) for day in data.date ]
data.date = days

