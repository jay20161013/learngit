#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime
import json
import pymssql
import time
import os
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler

global my_url
my_url = "https://oapi.dingtalk.com/robot/send?access_token=a97975dec40b2af79eeb6f01e5ba4db0d5a3b1643f49bed720373ed0b0b31a84"
header = {"Content-Type": "application/json", "Charset": "UTF-8"}

DBjson = []


def main():
    print('Main! The time is: %s' % datetime.now())
    get_mssqldatas()
    global my_url


def get_mssqldatas():
    # 一个传入sql导出数据的函数，实例为MySQL需要先安装pymysql库，cmd窗口命令：pip install pymysql
    # 跟数据库建立连接
    sql = "SELECT 对方户名,借方发生额,贷方发生额,时间戳 FROM  yy_ghsk WHERE 日期=convert(varchar(100),getdate(),23)"
    conn = pymssql.connect(server='192.168.81.14', port=1433, user='lh', password='414871250', database='baobiao')
    # 使用 cursor() 方法创建一个游标对象
    cur = conn.cursor()
    # 使用 execute() 方法执行 SQL
    cur.execute(sql)

    # 获取所需要的数据
    datas = cur.fetchall()
    finalstr = ''
    for row in datas:
        finalstr = finalstr + "对方户名:{0} 借方发生额:{1} 贷方发生额:{2} 时间戳:{3}\n\n".format(row[0], row[1], row[2], str(row[3]))
    conn.close()
    # 关闭连接
    cur.close()
    # 返回所需的数据
    # print(finalstr)
    message(str(finalstr+"@18505583716"))



def message(data):  # 定义信息函数
    text_info = {
        "msgtype": "text",
        "text": {
            "content": data
        },
        "at": {
            "atMobiles": "18505583716",
            "isAtAll": False   #是否@全部人员，如果是True,atMobiles不生效
        }
    }
    Mytitle = "#### XXX报表\r\n单位\t数量\t\n\r %s"
    my_Mytitle = Mytitle.join('\t\n') % data

    text_info["markdown"]["title"] = "XXXX 通报"
    text_info["markdown"]["text"] = my_Mytitle

    print(data)
    String_textMsg = json.dumps(text_info)
    res = requests.post(my_url, data=String_textMsg)


# if __name__ == "__main__":
#    get_mssqldatas()


if __name__ == "__main__":
    get_mssqldatas()
    scheduler = BackgroundScheduler()

    # 每隔60秒执行一次
    scheduler.add_job(main, 'interval', seconds=30)
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        # 其他任务是独立的线程执行
        while True:
            pass
            # time.sleep(60)
            # print('进程正在执行!')
    except (KeyboardInterrupt, SystemExit):
        # 终止任务
        scheduler.shutdown()
        print('Exit The Job!')

