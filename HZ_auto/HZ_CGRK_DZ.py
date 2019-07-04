import re
import cx_Oracle
#import openpyxl

conn = cx_Oracle.connect('nanfang/nanfangerp!@#@192.168.81.249:1521/NANFANG.ACT')
cursor = conn.cursor()
sql="SELECT t.sfdjdh,t.bz,t.sfrq FROM kc_sfdj t where t.djxz=1 and t.zt in (1,2) and t.bz is not null and t.bz like '%折%' and t.sfrq between to_date('2017-06-01', 'yyyy-mm-dd') and to_date('2019-06-25', 'yyyy-mm-dd')  "
cursor.execute(sql)

#print(row[1])
# print(row)
i=0
while True:
    row = cursor.fetchone()
    print(row)
    i=i+1
    #re.search('\d{2}折', row[1])
    #print(re.search('\d{2}折', row[1]),row[0],row[1],i)

    if row == None:
        break
    else:
        re.search('\d{2}折', row[1])
    print(i)

cursor.close()
conn.close()