import pyautogui
import time
import openpyxl
from selenium import webdriver
from selenium.webdriver.common.by import By
#读取Excel表格数据

wb=openpyxl.load_workbook(r'c:\cs.xlsx')
sheet=wb.get_sheet_by_name('报缴用表')
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
browser.find_element_by_xpath('//*[@msg="备注"]').send_keys(sheet.cell(2,13).value)
browser.find_element_by_xpath('//*[@id="v_storeid"]').send_keys('上海采购过渡仓库')
pyautogui.typewrite(['enter'], '0.25')

browser.find_element_by_xpath('//*[@id="dwmc"]').send_keys('上海南方')
pyautogui.typewrite(['enter'], '0.25')
print(sheet.max_row)
100015

pyautogui.moveTo(1130,45,duration=0.0025)
pyautogui.click(clicks=sheet.max_row*2, interval=0.25)   # clicks=sheet.max_row*2
pyautogui.moveTo(842,299)

for i in range(2,sheet.max_row+1):
    pyautogui.moveTo(842,(299+18*(i-2)))
    print(i)
    pyautogui.click(clicks=2, interval=0.25)
    pyautogui.typewrite(str(sheet.cell(i,5).value)) #sheet.cell(i,4).value
    pyautogui.typewrite(['enter'], '0.25')



#time.sleep(3)
#browser.find_element_by_xpath('//*[@id="cpbm_1"]').click()
#time.sleep(3)
#browser.find_element_by_xpath('//*[@id="cpbm_t"]').send_keys('100004')

#pyautogui.typewrite(['enter'], '0.25')

#print(browser.find_element_by_xpath('//*[@id="cpbm_1"]'))
