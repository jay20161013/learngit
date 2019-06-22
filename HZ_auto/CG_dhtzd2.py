import pyautogui
import time
import openpyxl
from selenium import webdriver
#读取Excel表格数据

iedriver = R"C:\bak\IEDriverServer.exe"
browser = webdriver.Ie(iedriver)
browser.get('http://192.168.81.249/friends/login.jsp')
# browser.find_element_by_xpath('/html/body/div[1]/div/div[4]/span/a[1]').click()
# time.sleep(1)

# 输入账号密码 点击登录
browser.find_element_by_xpath('//*[@name="user"]').send_keys('lh')
browser.find_element_by_xpath('//*[@name="password"]').send_keys('414871250@')
browser.find_element_by_xpath('//*[@name="imageField"]').click()

for handle in browser.window_handles:
    browser.switch_to_window(handle)



#browser.close()

browser.get('http://192.168.81.249/friends/buy/buy_in_add.vw')
browser.find_element_by_xpath('//*[@msg="备注"]').send_keys('上海准成品仓1111')
browser.find_element_by_xpath('//*[@id="v_storeid"]').send_keys('上海采购过渡仓库')
pyautogui.typewrite(['enter'], '0.25')
browser.find_element_by_xpath('//*[@id="dwmc"]').send_keys('上海南方')
pyautogui.typewrite(['enter'], '0.25')
pyautogui.moveTo(1130,45,duration=0.0025)
pyautogui.click()
time.sleep(2)
browser.find_element_by_xpath('//*[@name="cpbm_1"]').send_keys('100008')
pyautogui.typewrite(['enter'], '0.25')

#
