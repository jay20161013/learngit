import pyautogui
sc=pyautogui.screenshot()
#pic_2 = pypyautoguigui.screenshot('my_screenshot.png')
number7_location = pyautogui.locateOnScreen('tjyh.png')#传入按钮的图片
print(number7_location)# 返回屏幕所在位置
x,y = pyautogui.center(number7_location )
print(x,y)
pyautogui.click(x,y)