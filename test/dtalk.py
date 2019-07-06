#https://dingtalk-sdk.readthedocs.io/zh_CN/latest/client/index.html
import dingtalk
from dingtalk import  AppKeyClient

corp_id='dingf14485c1fcff68a3'
app_key='dingnfwjn4hwrvhjatx5'
app_secret='PrHR2J9mWioD8tC9E2-7SooZEMiECmZ_fMtCzsatvDuq6HGTg49Gu8J4tflfKvJT'
#client = SecretClient('corp_id', 'secret')  # 旧 access_token 获取方式
client = AppKeyClient(corp_id, app_key, app_secret)  # 新 access_token 获取方式

user = client.user.get('100681')
departments = client.department.list()
b=dingtalk.client.api.User(client)
bb=b.get_dept_member(54268940)

ccc=

print(bb)
#print(user)
a=dingtalk.client.api.Department(client).get(54286667)
a=dingtalk.client.api.Department(client).list()




# a=
#
#
#
#
#
#
#
#
#
#
#
#
#
# print(a)