
# Financial Open Data

平台網址：http://114.32.89.248/phpmyadmin/ <br>
user : guest <br>
password : 123 <br>

* ### [Load Example](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Example.md)<br>
  * [Taiwan Stock Information](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Example.md#load-taiwan-stock-information)
  * [Taiwan Stock Pnformation](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Example.md#load-taiwan-stock-price)
  * [Taiwan Stock Financial Statements](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Example.md#load-taiwan-stock-financialstatements)
  * [Taiwan Stock Dividend](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Example.md#load-taiwan-stock-stockdividend)
  * [Taiwan Stock Institutional Investors](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Example.md#load-taiwan-stock-institutionalinvestors-buy-and-sell)
  * [taiwan stock information](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Example.md#load-taiwan-stock-information)
  * [Crudeoil Prices](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Example.md#load-crudeoilprices)
  * [Exchange Rate](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Example.md#load-exchangerate)
  * [Central Band Interest Rate](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Example.md#load-central-band-interestrate)
  
* ### [Parameters](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Parameters.md)<br>
  * [database](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Parameters.md#database---stockinfo-stockprice-financialstatements-stockdividend-institutionalinvestors-crudeoilprices-exchangerate-interestrate---defult--)
  * [datalist](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Parameters.md#datalist--true-or-false--defult--flase-)
  * [select](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Parameters.md#select--stock-id-2002--or-country-name-eur-etc---defult--)
  * [load_all](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/Parameters.md#load_all--true-or-false--defult--flase-)
  
* ### [Variable Introduction](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/VariableIntroduction.md)
  * [Taiwan Stock Info](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/VariableIntroduction.md#1-taiwan-stock-info)
  * [Taiwan Stock Prices](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/VariableIntroduction.md#2-taiwan-stock-prices)
  * [Taiwa Stock Financial Statements](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/VariableIntroduction.md#3-taiwan-stock-financial-statements)
  * [Taiwan Stock Dividend](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/VariableIntroduction.md#4-taiwan-stock-dividend)
  * [Taiwan Stock Institutional Investors](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/VariableIntroduction.md#5-taiwan-stock-institutional-investors)
  * [Crude Oil Prices](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/VariableIntroduction.md#6-crude-oil-prices)
  * [Exchange Rate](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/VariableIntroduction.md#7-exchange-rate)
  * [Interest Rate](https://github.com/f496328mm/FinancialMining/blob/master/OpenData/VariableIntroduction.md#8-interest-rate)
  
------------------------------------------------------------
#### 目前現有 data 如下

1. 台股股票一般資訊 ( 代號、名稱、產業 ) <br>
2. history taiwan stock prices ( 台股歷史股價 )<br>
3. history taiwan stock Financial Statements ( 台股歷史財報 1997 ~ now )<br>
4. history taiwan stock Stock Dividend ( 台股歷史配股 1991 ~ now )<br>
5. history taiwan stock Institutional Investors buy and sell ( 台股歷史外資買賣 2004 ~ now )<br>
6. crude oil prices ( Oil Prices 2000 ~ now )<br>
7. exchange rate ( Exchange Rate 1990 ~ now )<br>
8. interest rate ( Interest Rate 1990 ~ now )<br>

<!---請先下載
[ load_data.py ](https://github.com/f496328mm/FinancialMining/blob/master/FinancialOpenData/load_data.py) <br>
ps : 可藉由 stock_id, data 進行資料合併--->



