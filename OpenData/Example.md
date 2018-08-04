
#### Load Taiwan Stock information
      from FinancialMining.OpenData.Load import Load

      database = 'StockInfo'

      datalist = Load(database = database,datalist = True)
      data_2002 = Load(database = database, select = '2002')
      mulit_data = Load(database = database, select = ['2002','2330'])
      all_data = Load(database = database, load_all = True)
-----------------------------------------------------------------
#### Load Taiwan Stock Price
        from FinancialMining.OpenData.Load import Load

        database = 'StockPrice'

        datalist = Load(database = database,datalist = True)#### get stock list
        data_2002 = Load(database = database, select = '2002') #### select stock 2002
        mulit_data = Load(database = database, select = ['2002','2330'])#### select mulit stock
        all_data = Load(database = database, load_all = True)#### select all stock
-----------------------------------------------------------------
#### Load Taiwan Stock FinancialStatements
        from FinancialMining.OpenData.Load import Load

        database = 'FinancialStatements'
        datalist = Load(database = database,datalist = True)#### get stock list
        data = Load(database = database, select = '2002') #### select stock 2002
        data = Load(database = database, select = ['2002','2330'])#### select mulit stock
        data = Load(database = database, load_all = True)#### select all stock
-----------------------------------------------------------------
#### Load Taiwan Stock StockDividend

      from FinancialMining.OpenData.Load import Load

      database = 'StockDividend'
      datalist = Load(database = database,datalist = True)#### get stock list
      data = Load(database = database, select = '2002') #### select stock 2002
      data = Load(database = database, select = ['2002','2330'])#### select mulit stock
      data = Load(database = database, load_all = True)#### select all stock
-----------------------------------------------------------------
#### Load Taiwan Stock InstitutionalInvestors buy and sell
        from FinancialMining.OpenData.Load import Load

        database = 'InstitutionalInvestors'
        data = Load(database = database) 
-----------------------------------------------------------------
#### Load CrudeOilPrices
        from FinancialMining.OpenData.Load import Load

        database = 'CrudeOilPrices'
        data = Load(database = database)
-----------------------------------------------------------------
#### Load ExchangeRate
        from FinancialMining.OpenData.Load import Load

        database = 'ExchangeRate'
        datalist = Load(database = database,datalist = True)#### get country list
        data = Load(database = database, select = 'GBP') #### select country GBP
        data = Load(database = database, select = ['GBP','HKD'])#### select mulit country
        data = Load(database = database, load_all = True)#### select all stock
-----------------------------------------------------------------
#### Load Central Band InterestRate
        from FinancialMining.OpenData.Load import Load

        database = 'InterestRate'
        datalist = Load(database = database,datalist = True)#### get country list
        data = Load(database = database, select = 'FED') #### select country FED
        data = Load(database = database, select = ['FED','ECB'])#### select mulit country
        data = Load(database = database, load_all = True)#### select all stock
-----------------------------------------------------------------


