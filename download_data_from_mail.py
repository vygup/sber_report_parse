from imap_tools import MailBox
import datetime
import json
import os, sys

# Шаблон лога
def print_logs(text_logs):
    print('\nPROC - PARS_INVEST_REPORT_MAIL:\n' + '\t' + str(datetime.datetime.now()) + ' - ' + text_logs)

abspath = os.path.abspath(__file__)
basename = os.path.basename(__file__)
curdir = abspath.replace(basename, '')

# Базовый конфиг
with open(curdir + '/config.json') as file:
    base_conf = json.load(file)

# Путь с конфигом для коннекта
path_conf = base_conf.get('path') 
# Путь для сохранения данных
path_data = base_conf.get('path_data') 
# Почта брокера
mail_broker = base_conf.get('ya_mail_broker')
# host imap
imap_host = base_conf.get('ya_imap_host')
# port imap
imap_port = base_conf.get('ya_imap_port')
# Папка с отчетами
try:
    mail_folder = base_conf.get('ya_mail_folder')
except:
    mail_folder = None


# Конфигурационный файл
with open(path_conf) as file:
    config_json = json.load(file)

# Ключ к почте
key_mail = config_json.get('ya_key_invest') 
# Почта
login_mail = config_json.get('ya_mail_invest') 

# Создаем структуру папок 
try:
    os.mkdir(path_data + '/day/')
    os.mkdir(path_data + '/period/')
    print('Структура папок создана')
except:
    print('Структура папок корректна')

# Проверка последнего обработанного письма
try:
    with open(f'{path_data}/load_data.json', 'r') as file:
        update_rep = json.load(file)
        print(f'Инфо о последнем обработанном письме Id: {list(update_rep.items())[0][0]} | Тема: {list(update_rep.items())[0][1]}')
except:
    with open(f'{path_data}/load_data.json', 'w') as file:
        update_rep = {"-1":"0"}
        json.dump(update_rep, file)
        print('Создан шаблон load_data.json')

# Процесс выгрузки данных
print_logs('START')

try:
    # Коннект
    print_logs('Подключение к почте')
    mailbox = MailBox(host = imap_host, port = imap_port)#, ssl=True)
    mailbox.login(login_mail, key_mail)
    if mail_folder:
        print_logs(f'Читаем папку {mail_folder}')
        mailbox.folder.set(mail_folder)
    else:
        print_logs(f'Читаем папку Входящие')
    
    # id последнего обработанного письма
    print_logs('Загрузка uid_old')
    uid_old = int(list(update_rep.keys())[0])

    print_logs('Читаем почту')
    cnt_report = 0
    # генератор - imap_tools.MailMessage
    for message in mailbox.fetch():
        uid_max = int(message.uid)
        #Только новые письма  
        if uid_max > uid_old:
            sub_max = message.subject
            # только отчеты от сбер брокера
            if message.from_ == mail_broker:
                # только отчеты
                if str(message.subject).find('Отчет брокера') > 0:
                    # Проверка типа отчета и перенаправление загрузки
                    if str(message.subject).find('за период') > 0:
                        path_corr = '/period/'
                    else:
                        path_corr = '/day/'
                    # забираем данные из сообщения
                    mail = message.obj
                    # забираем только письма со списком подобъектов
                    if mail.is_multipart():
                        # забираем части и подчасти дерева объектов сообщений в порядке обхода в глубину
                        for part in mail.walk():
                            # забираем тип содержимого сообщения, только с файлом
                            if part.get_content_type() == 'application/data':
                                # забираем название файла
                                filename = part.get_filename()
                                # продолжаем, если файл точно есть
                                if filename:
                                    print_logs('Скачивание отчета: ' + filename)
                                    with open(str(path_data + path_corr + filename), 'wb') as new_file:
                                        new_file.write(part.get_payload(decode=True))
                                    cnt_report += 1

    # отчет по работе
    if uid_max == uid_old:
        print_logs('Новых писем нет')
    else:
        print_logs('Скачано ' + str(cnt_report) + ' отчетов')

        print_logs('Записываем uid_max')
        # Записываем последнее обработанное письмо
        line_d = json.dumps({uid_max: sub_max}, ensure_ascii=False)
        with open(f'{path_data}/load_data.json', 'w') as outf:
            outf.write(line_d)

    print_logs('Закрываем соединение')
    mailbox.logout()

except Exception as er:
    print_logs(str(er))

finally:
    print_logs('Успешно')