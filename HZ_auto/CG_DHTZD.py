import pyautogui
import time
import openpyxl
from selenium import webdriver
#读取Excel表格数据

iedriver = R"C:\Users\liuhui\AppData\Local\Programs\Python\Python37-32\Scripts\IEDriverServer.exe"
browser = webdriver.Ie(iedriver)
browser.get('http://192.168.81.249/friends/login.jsp')
# browser.find_element_by_xpath('/html/body/div[1]/div/div[4]/span/a[1]').click()
# time.sleep(1)

# 输入账号密码 点击登录
browser.find_element_by_xpath('//*[@name="user"]').send_keys('sys_oper')
browser.find_element_by_xpath('//*[@name="password"]').send_keys('414871250@')
browser.find_element_by_xpath('//*[@name="imageField"]').click()
time.sleep(5)
for handle in browser.window_handles:
    browser.switch_to_window(handle)



wb=openpyxl.load_workbook(r'D:\cs.xlsx')
sheet=wb.get_sheet_by_name('报缴用表')

#
caigouguanlix=294
caigouguanliy=112
cggl_rcywx=340
cggl_rcywy=149
cggl_dhtzdx=481
cggl_dhtzdy=182


tianjia_tzdx=1317
tianjia_tzdy=147

cangku_x=1344
cangku_y=207

#防错设定

pyautogui.PAUSE=1
pyautogui.FAILSAFE=True
#time.sleep(5)
# pyautogui.moveTo(caigouguanlix,caigouguanliy,duration=0.0025)
# pyautogui.click()
# pyautogui.moveTo(cggl_rcywx,cggl_rcywy,duration=0.0025)
# time.sleep(1)
# pyautogui.moveTo(cggl_dhtzdx,cggl_dhtzdy,duration=0.0025)
# pyautogui.click()
# #time.sleep(2)
# pyautogui.moveTo(tianjia_tzdx,tianjia_tzdy,duration=0.0025)
# pyautogui.click()
#time.sleep(2)
#browser.get('http://192.168.81.249/friends/buy/buy_in_add.vw')
# for handle in browser.window_handles:#方法二，始终获得当前最后的窗口
#     browser.switch_to_window(handle)



browser.get('http://192.168.81.249/friends/buy/buy_in_add.vw')
sreach_window=browser.current_window_handle
#browser.find_element_by_xpath('//*[@onkeyup=SelectKeyUp(this.value)]').send_keys('上海准成品仓')
browser.find_element_by_xpath('//*[@msg="备注"]').send_keys('上海准成品仓1111')
browser.find_elements_by_css_selector()


#循环读数_明细表单

# for i in range(2,sheet.max_row+1):
#     pyautogui.moveTo(cangku_x, cangku_y, duration=0.25)
#     pyautogui.click()

















