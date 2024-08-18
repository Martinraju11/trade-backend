import logging
from kiteconnect import KiteConnect
import csv
import time
import logging
from os import path
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import sys,pyotp

global kite,kws,access_token

kws = ""
kite = ""
access_token = ''
todays_dt = time.strftime("%Y_%m_%d", time.localtime())

def login_check(access_token,kite):
 try:
  kite.set_access_token(access_token)
  cv = (kite.profile())
  return 1
 except Exception as e:
  print('\nIt may be the firt time login so retrying, exception Message is :'+str(e))
  return 0


#element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "myDynamicElement"))).click()
#element.click()


def login(url, username, password2, pinn,client_secret,kite,file_name):
 for i in range(0,10):
  while True:
   try:
     print('\nZerodha Kite Login in progress.')
     chrome_opt = webdriver.ChromeOptions()
     chrome_opt.add_argument('--headless')
    # Use 'options' instead of 'chrome_options'
     driver = webdriver.Chrome(options=chrome_opt)

     driver.get(url)
     WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"userid\"]"))).send_keys(username)
     WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"password\"]"))).send_keys(password2)
     #driver.find_element_by_xpath("//*[@id=\"container\"]/div/div/div/form/div[4]/button").click()
     WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"container\"]/div/div/div[2]/form/div[4]/button"))).click()
     time.sleep(5)
     get_url = driver.current_url
     print('TOTP',pyotp.TOTP(pinn).now());#exit()
     print('get_url printing1:',get_url)
     #WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"pin\"]"))).send_keys(pinn)
     ##used till Sep 24: WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"totp\"]"))).send_keys(pyotp.TOTP(pinn).now())
     #used till March 18 WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"container\"]/div/div/div[2]/form/div[2]/input"))).send_keys(pyotp.TOTP(pinn).now())
     WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"container\"]/div[2]/div/div/form/div[1]/input"))).send_keys(pyotp.TOTP(pinn).now())
     time.sleep(5)
     #driver.find_element_by_xpath("//*[@id=\"container\"]/div/div/div[2]/form/div[3]/button").click()
     #WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"container\"]/div/div/div[2]/form/div[3]/button"))).click()
     #time.sleep(5)
     new_url = driver.current_url 
     print('get_url printing2:',new_url)
     time.sleep(5)
     driver.quit()
     line_word = new_url.split('request_token=')
     secpart = line_word[1]
     line_word2 = secpart.split('&')
     #print ('request_token::::',line_word2[0],line_word2)
     request_token = line_word2[0]
     data = kite.generate_session(request_token, api_secret=client_secret)
     access_token = data['access_token']
     
     with open(file_name,"w") as f:
      f.write(access_token)
     return access_token
   except Exception as e:
    logging.error('WEB Login Exception, Try to run ALGO AGAIN, exception Message is :'+str(e))
    time.sleep(5)
    continue
   break

def get_access_token(user_name,passwd,login_pin,client_secret,api_key,strategy):
 for i in range(0,100):
  while True:
   try:
    kite = KiteConnect(api_key=api_key)
    URL = 'https://kite.trade/connect/login?api_key=' + api_key
    file_name = strategy+'_'+user_name + '_access_tkn_' + todays_dt + '.txt'
    if (path.exists(file_name)):
     ##print('File',file_name,' exist.' )
     with open(file_name, 'r') as file:
      access_token = file.read() #.replace('\n', '')
     retval = login_check(access_token,kite)
     if (retval == 1):
      print('')
      return access_token,api_key
     else:
        #print('login test Not success')
        print('\nZerodha Kite Login in-progress.')
        access_token = login(URL, user_name, passwd, login_pin,client_secret,kite,file_name)
    else:
     print('\nFile',file_name,'Not exist, So creating it and calling access token generation.' )
     access_token = login(URL, user_name, passwd, login_pin,client_secret,kite,file_name)
     #print("ACCESS TOKEN received successfully.")
     return access_token,api_key
   except Exception as e:
    print("exception occured:" + str(e))
    time.sleep(10)
    continue
   break
#att,ap = get_access_token()
#print(att,ap)