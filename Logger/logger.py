import os,sys

from datetime import datetime
class Logger:
    ROOT_DIR = os.path.abspath(os.curdir)
    @staticmethod
    def save_log_to_file(content,save_file_name):
        with open(f"{sys.path[1]}/logs/{datetime.today().strftime('%Y%m%d')}_save_file_name", 'a', encoding='utf-8') as log:
            print(content+"\n", file=log)

    @staticmethod
    def save_error_log_to_file(content,save_file_name):
        with open(f"{sys.path[1]}/logs/{datetime.today().strftime('%Y%m%d')}_error_file_name", 'a', encoding='utf-8') as log:
            print(content+"\n", file=log)
