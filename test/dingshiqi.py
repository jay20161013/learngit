# import openpyxl as ol
# from openpyxl import workbook
# from openpyxl.utils import get_column_letter
# from random import choice
# from time import time
# import datetime
# wb=ol.Workbook()
#
#
# ws=wb.create_sheet('cs123')
# ws.append(['TIME', 'TITLE', 'A-Z'])
#
# # 输入内容（500行数据）
# for i in range(500):
#     TIME = datetime.datetime.now().strftime("%H:%M:%S")
#     TITLE = str(time())
#     A_Z = get_column_letter(choice(range(1, 50)))
#     ws.append([TIME, TITLE, A_Z])
# print(ws.max_row)
# ws.cell(2,5).value=11
# print(ws)
# print(wb)
# wb.save('\\cs.xlsx')


import pyautogui as auto
sc=auto.screenshot()
#pic_2 = pyautogui.screenshot('my_screenshot.png')
number7_location = auto.locateOnScreen('tjyh.png')#传入按钮的图片
print(number7_location)# 返回屏幕所在位置
x,y = auto.center(number7_location )
print(x,y)
auto.click(x,y)
# x,y = auto.center(number7_location) # 转化为 x,y坐标
# print(number7_location) #按键7的坐标是1441,582
#
# auto.click(number7_location)

