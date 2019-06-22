from selenium import webdriver
import time
import json
import requests

# 获取的cookie保存到文件
iedriver = R"C:\Users\liuhui\AppData\Local\Programs\Python\Python37-32\Scripts\IEDriverServer.exe"
driver = webdriver.Ie(iedriver)
driver.get(
    "http://192.168.81.249/friends/login.jsp")

# driver.find_element_by_xpath('//*[@placeholder="用户名"]').send_keys('100681')
# driver.find_element_by_xpath('//*[@placeholder="密码"]').send_keys('414871250')
# driver.find_element_by_xpath('//*[@id="fs-login-btn"]').click()

driver.find_element_by_xpath('//*[@name="user"]').send_keys('sys_oper')
driver.find_element_by_xpath('//*[@name="password"]').send_keys('414871250@')
driver.find_element_by_xpath('//*[@name="imageField"]').click()

for handle in driver.window_handles:
    driver.switch_to_window(handle)
time.sleep(2)

# print(driver.current_url)
# text=driver.page_source
cookie = driver.get_cookies()
print(cookie)
jsonCookies = json.dumps(cookie)
with open('qqhomepage.json', 'w') as f:
    f.write(jsonCookies)

# 整理cookie，将至整理为需要的格式
str = ''
with open('qqhomepage.json', 'r', encoding='utf-8') as f:
    listCookies = json.loads(f.read())
cookie = [item["name"] + "=" + item["value"] for item in listCookies]
cookiestr = '; '.join(item for item in cookie)
print(cookiestr)

# 验证获取的cookie是否能用
url = 'http://192.168.81.249/friends/buy/buy_in_add.vw'

headers = {
    'cookie': cookiestr,
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'
}

html = requests.get(url=url, headers=headers)

print(html.text)
