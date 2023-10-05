import json
import smtplib
from typing import List


class Send_email:
    '''
    Класс для отправки писем на почту
    '''
     
    def __init__(self, login_password_path):

        self.login_password_path = login_password_path # Путь до словарика с логином и паролем от почты

    def send_email(self, sender_email, receiver_email_list: List, flag = None):

        with open(self.login_password_path) as login_password_file:
            login_password_data = json.loads(login_password_file.read())

            _login = login_password_data.get('login')
            _password = login_password_data.get('password')
        

        if flag == None:
            pass
        else:
            server = smtplib.SMTP('smtp.yandex.ru')
            server.ehlo()
            server.login(_login, _password)
            server.sendmail(sender_email, receiver_email_list, flag)
            server.close()