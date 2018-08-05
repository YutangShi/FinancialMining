## Financial Mining Outline 
##### Financial Open Data ( Development )
     目前現有 data 如下
     1. 台股股票(代號、名稱、產業)資訊 
     2. taiwan stock prices 
     3. taiwan stock Financial Statements 
     4. taiwan stock Stock Dividend 
     5. taiwan stock Institutional Investors buy and sell 
     6. 國際油價
     7. exchange rate
     8. interest rate
     9. gold price

##### Financial Visualize ( Development )
##### Financial Predict ( Development )

------------------------------------------------------------
### UPDATE
##### 2018/8/5
1. 央行利率 100% ( Contains G8 )

          FED Federal Reserve System 美國
          ECB European Central Bank 歐洲
          BOE Bank of England 英國
          SNB Swiss National Bank 瑞士
          RBA Reserve Bank of Australia 澳洲
          BOC Bank of Canada 加拿大
          RBNZ Reserve Bank of New Zealand 紐西蘭
          BOJ Bank of Japan 日本
          CBR The Central Bank of the Russian Federation 俄羅斯
          RBI Reserve Bank of India 印度
          PBOC People's Bank of China  中國
          BCB Banco Central do Brasil 巴西
2. Gold Price 100%
3. Government bond  https://data.oecd.org/interest/long-term-interest-rates.htm
 
##### 2018/7/5 
1. 國際油價 讀取範例 Load data example. (100%)
3. 各國匯率 from https://www.xe.com/currencytables/?from=USD&date=1995-11-16 https://www.ofx.com/en-au/forex-news/historical-exchange-rates/ (100%)

##### 2018/7/2 未來爬蟲順序
2. 央行利率 from https://tradingeconomics.com/search.aspx?q=Interest%20Rate
4. Inflation (通貨膨脹) from https://tradingeconomics.com/russia/inflation-cpi
5. Consumer Price Index (CPI) from https://tradingeconomics.com/russia/consumer-price-index-cpi
6. Output Gap from https://tradingeconomics.com/russia/gdp-deflator
7. GDP Deflator at time from https://tradingeconomics.com/russia/gdp
8. S&P 500 from yahoo finance
9. 黃金價格 from https://www.gold.org/data/gold-price
-------------------------------------------------------------------------------------------------

## Financial Open Data ( [Development](https://github.com/f496328mm/FinancialMining/tree/master/OpenData)  )

### 1.Financial Crawler
#### 1.1 股票股價
     1.1.1 台股歷史股價 Crawler history Taiwan stocks prices. (100%)
     1.1.2 自動更新台股股價 Auto crawler new taiwan stocks prices. (100%)
     1.1.3 讀取範例 Load data example (100%)
     1.1.4 三大法人(外資) 歷史買賣 history taiwan stock Institutional Investors buy and sell (100%)
     1.1.5 自動更新外資買賣 Auto crawler new taiwan stocks Institutional Investors buy and sell. (100%)
     1.1.6 S&P 500指數，並爬取該 500 家股票股價 ->>>
#### 1.2 各國匯率 
     1.2.1 以美元為基準，對於未來其他金融 data ，較容易進行轉換。( https://www.xe.com/currencytables/?from=USD&date=1995-11-16 )
#### 1.3 國際油價
     1.3.1 https://www2.moeaboe.gov.tw/oil102/oil2017/A02/A0201/dayoil.asp 100%
     1.3.2 WTI https://www.eia.gov/dnav/pet/hist/rwtcD.htm
     1.3.3 Brent https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=RBRTE&f=W
     1.3.4 讀取範例 Load data example.
#### 1.4 央行利率
     1.4.1 https://tradingeconomics.com/euro-area/interest-rate
     1.4.2 https://www.investing.com/central-banks/
#### 1.5 債券價格
     1.5.1 G7 country ( 美國、加拿大、英國、法國、德國、義大利及日本 )->>>
     1.5.2 data from https://data.oecd.org/interest/short-term-interest-rates.htm
     1.5.3 data from https://www.investing.com/rates-bonds/
     1.5.4 data from G7 central bank
     
#### 1.6 財報
     1.6.1 台股歷年財報 history taiwan stock Financial Statements (100%) 
     1.6.2 台股歷史配股 history taiwan stock Stock Dividend (100%)
     1.6.3 每日自動爬取最新財報 (100%)
     1.6.4 每日自動爬取最新配股 (100%)
#### 1.7 other 
     1.7.1 Output Gap (產出缺口)
     1.7.2 Inflation (通貨膨脹) https://tradingeconomics.com/russia/inflation-cpi
     1.7.3 GDP Deflator at time (國內生產總值平減指數) https://tradingeconomics.com/russia/gdp-deflator
     1.7.4 期貨
     1.7.5 基金
     1.7.6 黃金價格 https://www.gold.org/data/gold-price
     1.7.7 房地產價格
     1.7.8 Consumer Price Index (CPI) https://tradingeconomics.com/russia/consumer-price-index-cpi
     1.7.9 GDP https://tradingeconomics.com/russia/gdp

### 2. Load Data example
[click](https://github.com/f496328mm/FinancialMining/tree/master/FinancialOpenData)
-------------------------------------------------------------------------------------------------

## Financial Visualize
## Financial Predict






