#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from apscheduler.schedulers.blocking import BlockingScheduler


def p():
    print("a")

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(p, "cron",second='1/5')

    try:
        scheduler.start()
    except Exception as e:
        pass