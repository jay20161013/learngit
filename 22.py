import  DingtalkChatbot
book="https://oapi.dingtalk.com/robot/send?access_token=d6cfec57209b27d7756cec6eacb48a9310f37cd564694b6943363a73e9b42efb"

xd=DingtalkChatbot(book)
xd.send_text(msg='我就是小丁，小丁就是我！', is_at_all=True)

