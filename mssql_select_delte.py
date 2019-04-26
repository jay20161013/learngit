import pymssql
db=pymssql.connect(server='192.168.81.14', port=1433, user='lh', password='414871250', database='baobiao')
cur=db.cursor()
cur.execute('insert into nf_rtx (id,name) values (100682,\'liude2hua\')')
db.commit()
r = cur.execute('select * from nf_rtx ')
r= cur.fetchall()
print(r)
r1 = cur.execute('delete from nf_rtx where id=\'100681\'')
db.commit()
cur.close()
db.close()
#往数据库里增加数据
#通过一个交互的界面往数据库里增加、删除、修改、查询数据
#显示出来的时候，尽可能是一行一行显示