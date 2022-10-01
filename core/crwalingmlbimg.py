from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import urllib.request
import requests
import random
from Logger.logger import Logger
import os
from DB.DML import DML

class CrawlingMlbImg:
    MLB_URL = "https://www.mlb.com/player/"
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-Logger"])
    driver = webdriver.Chrome(options=options)
    # crawler_path = "C:/Users/SSAFY/github_repository/ssafy/crawler/chromedriver.exe"  # 자신의 chomedriver의 경로 위치

    @classmethod
    def file_save(cls, img_url, save_path):
        r = requests.get(img_url)
        with open(save_path,"wb") as output:
            output.write(r.content)

    @classmethod
    def file_save2(cls, img_url, save_path):
        urllib.request.urlretrieve(img_url, save_path)

    @classmethod
    def get_image_url(cls, image_xpath):
        img_object = cls.driver.find_element("xpath", image_xpath)
        img_url = img_object.get_attribute("src")
        return img_url

    @classmethod
    def save_local(cls, player_names):
        """
        :desc 지점 위치를 크롤링해서 리턴하는 함수
        :return {지점:주소} 형태로 리턴한다.
        """
        log_file_name: str = "player_save.txt"

        for i in range(len(player_names)):

            names = player_names[i][1].split(" ")
            preprocessed_name = ""

            for j in range(len(names)):
                preprocessed_name += names[j] + "-"
            preprocessed_name += str(player_names[i][0])
            preprocessed_name = preprocessed_name.lower()
            cls.driver.get(cls.MLB_URL + preprocessed_name)
            sec = int(random.random() * 5)
            print(f"1. random waiting sec:{sec}")
            time.sleep(sec)
            # print(preprocessed_name)
            image_name = f"{player_names[i][0]}_{player_names[i][1]}"
            save_path = f"{os.getcwd()}/result/{image_name}.jpg"
            # print(save_path)
            try:
                image_xpath = "/html/body/main/section/header/div/img"
                img_url = cls.get_image_url(image_xpath)
            except:
                Logger.save_error_log_to_file(f"{image_name}", log_file_name)
                print(f"{i} except")

            cls.file_save(img_url, save_path)
            print(f"2. random waiting sec:{sec}")
            time.sleep(sec)

            Logger.save_log_to_file(f"success_{i} {player_names[i]}", log_file_name)
            print(f"{i} success")

    @classmethod
    def save_db_by_player_names(self, table_name, player_names, field_name):
        """
        :desc 지점 위치를 크롤링해서 리턴하는 함수
        :return {지점:주소} 형태로 리턴한다.
        """
        dml_instance = DML()
        log_file_name: str = "player_save.txt"

        for i in range(len(player_names)):

            names = player_names[i][1].split(" ")
            preprocessed_name = ""

            for j in range(len(names)):
                preprocessed_name += names[j] + "-"
            preprocessed_name += str(player_names[i][0])
            preprocessed_name = preprocessed_name.lower()
            preprocessed_name
            self.driver.get(self.MLB_URL + preprocessed_name)
            sec = 1+int(random.random() * 2)
            print(f"1. random waiting sec:{sec}")
            time.sleep(sec)
            image_name = f"{player_names[i][0]}_{player_names[i][1]}"
            save_path = f"{os.getcwd()}/result/{image_name}.jpg"
            try:
                image_xpath = "/html/body/main/section/header/div/img"
                img_url = self.get_image_url(image_xpath)
                condition = f"uid = {player_names[i][0]}"
                dml_instance.update_from_where(table_name=table_name, field_name=field_name, data=img_url,
                                               condition=condition)
                Logger.save_log_to_file(f"success_{i} {player_names[i]}", log_file_name)
                print(f"{i} success")
            except:
                Logger.save_error_log_to_file(f"{image_name}", log_file_name)
                print(f"{i} except")

            print(f"2. random waiting sec:{sec}")
            time.sleep(sec)

        dml_instance.close()

    @classmethod
    def save_db_by_name_slug(self, table_name, name_slugs, field_name):
        """
        :desc 지점 위치를 크롤링해서 리턴하는 함수
        :return {지점:주소} 형태로 리턴한다.
        """
        dml_instance = DML()
        log_file_name: str = "player_save.txt"

        for i in range(len(name_slugs)):
            preprocessed_name = name_slugs[i][0]
            self.driver.get(self.MLB_URL + preprocessed_name)
            sec = int(random.random() * 2)
            # print(f"1. random waiting sec:{sec}")
            time.sleep(sec)
            try:
                image_xpath = "/html/body/main/section/header/div/img"
                img_url = self.get_image_url(image_xpath)
                uid = preprocessed_name.split("-")[-1]
                condition = f"uid = {uid}"
                dml_instance.update_from_where(table_name=table_name, field_name=field_name, data=img_url,
                                               condition=condition)
                Logger.save_log_to_file(f"success_{i} {name_slugs[i]}", log_file_name)
                print(f"{i} success")
            except Exception as e:
                Logger.save_error_log_to_file(f"{preprocessed_name} {e}", log_file_name)
                print(f"{i} {e}")

            # print(f"2. random waiting sec:{sec}")
            time.sleep(sec)

        dml_instance.close()