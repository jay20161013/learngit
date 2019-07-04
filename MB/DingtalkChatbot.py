from dingtalkchatbot.chatbot import DingtalkChatbot
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=a97975dec40b2af79eeb6f01e5ba4db0d5a3b1643f49bed720373ed0b0b31a84'
xiaoding = DingtalkChatbot(webhook )

xiaoding.send_text(msg='我就是小丁，小丁就是我！', is_at_all=True)