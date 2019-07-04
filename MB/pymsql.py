import pymssql
conn=pymssql.connect(server='192.168.81.12', port=6401, user='sa', password='@#southbedding001', database='south_hr_data')
cur=conn.cursor()
gh=input('工号:')
sql="select * from Hr_Employee Where Emp_ID IN(%s)" % str(gh)
jg=cur.execute(sql)
datas = cur.fetchall()
print(datas)
cur.close()
