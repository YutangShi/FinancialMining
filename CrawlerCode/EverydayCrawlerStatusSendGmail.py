


import os
import sys
import pandas as pd
import numpy as np
from selenium import webdriver
import time
import datetime
from selenium.webdriver.firefox.options import Options
sys.path.append('/home/linsam/github/FinancialMining/CrawlerCode')
sys.path.append('/home/linsam/github/FinancialMining/FinancialOpenData')
sys.path.append('/home/linsam/github')
import load_data
import Key
#----------------------------------------------------------------------------
# self = EverydayCrawlerStatus()
class EverydayCrawlerStatus:
    def __init__(self):
        self.host = load_data.host
        self.user = load_data.user
        self.password = load_data.password
        self.database = 'python'
        
        tem = load_data.execute_sql2(self.host,self.user,self.password,self.database,'SHOW TABLES')
        tem = np.concatenate(tem, axis=0)
        self.datatable = [ te for te in tem ]
        self.datatable.remove('new')

    def CrawlerStatus(self):
        self.cdate = pd.DataFrame()
        for dt in self.datatable:
            text = "SELECT name,CrawlerDate FROM `" + dt + "` where id = ( SELECT max(id) FROM `" + dt + "` )"
            tem = load_data.execute_sql2(self.host,
                                         self.user,
                                         self.password,
                                         self.database,
                                         text)
            tem = pd.DataFrame( np.concatenate(tem, axis=0) )
            self.cdate = self.cdate.append(tem.T)
        
        self.cdate.index = range(len(self.cdate))
        self.cdate.columns = ['name','date']

    def change_status(self):
        #self.CrawlerStatus()
        
        self.cdate['status'] = ''
        today = str( datetime.datetime.now() ).split(' ')[0]
        for i in range(len(self.cdate)):
            tem = str( self.cdate['date'][i] ).split(' ')[0]
            
            if tem == today :
                self.cdate['status'][i] = 'ok'
            
            
            
#----------------------------------------------------------------------------    
# self = SendGmail(ECS.cdate)
class SendGmail:
    def __init__(self,cdate):
        Firefox_options = Options()
        Firefox_options.add_argument("--headless")
        Firefox_options.add_argument("--window-size=1920x1080")
        
        self.driver = webdriver.Firefox(firefox_options=Firefox_options)
        #self.driver = webdriver.Firefox()
        url = 'https://mail.google.com/mail/?tab=wm'
        self.driver.get(url)
        self.email = Key.email
        self.email_password = Key.email_password
        self.cdate = cdate
    
    #-------------------------------------------------------------
    def main(self):
        def create_title(cdate):
            
            ok_sum = np.sum( cdate['status'] == 'ok' )
            if ok_sum == len(cdate):
                title = 'ok'
            else:
                title = 'not ok'
            return title
        
        def create_text(cdate):
            text = ''
            for i in range(len(cdate)):
                text = text + cdate['name'][i] + ' : ' + str( cdate['date'][i] ) + '\n'
            return text
        #-------------------------------------------------------------
        bo = 1
        while(bo):
            try:
                self.driver.find_element_by_id('identifierId').send_keys(self.email)
        
                self.driver.find_element_by_id('identifierNext').click()
                bo = 0
            except:
                bo = 1        
        #-------------------------------------------------------------
        bo = 1
        while(bo):
            try:
                self.driver.find_element_by_name('password').send_keys(self.email_password)
                
                self.driver.find_element_by_id('passwordNext').click()
                bo = 0
            except:
                bo = 1
        #-------------------------------------------------------------
        self.driver.find_element_by_id('passwordNext').click()
        time.sleep(1)
        # go to send email
        url = 'https://mail.google.com/mail/u/0/?tab=wm#inbox?compose=new'
        self.driver.get(url)
        # key to someone email
        time.sleep(1)
        self.driver.find_element_by_name('to').send_keys(self.email)
        
        # key title
        title = create_title(self.cdate)
        self.driver.find_element_by_name('subjectbox').send_keys(title)# title
        self.driver.find_element_by_class_name('Ap').click()
        
        # key article text
        text = create_text(self.cdate)
        self.driver.find_element_by_class_name('Ap').send_keys(text)# text
        
        # click send email
        button = self.driver.find_elements_by_xpath('//*[@role="button"]')
        for i in range(len(button)):
            if button[i].text == '傳送' :   
                button[i].click()
                break
        time.sleep(2)
        # quit
        self.driver.quit()
        
        
def main():
    
    ECS = EverydayCrawlerStatus()
    ECS.CrawlerStatus()
    ECS.change_status()
    
    SG = SendGmail(ECS.cdate)
    SG.main()
    
    
if __name__ == '__main__':
    main()
    
    
    
    
    
    
    
    
    

