from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import urllib.request
import random
from Logger.logger import Logger
import os


class CrawlingGoogleImg:
    GOOGLE_URL = "https://www.google.com/search?q=dfs&sxsrf=ALiCzsaDpm52AEll91Q8a3sPzO_8TuFK9Q:1662188925176&source=lnms&tbm=isch&sa=X&ved=2ahUKEwin5byjiPj5AhWFBd4KHUOqDRwQ_AUoAXoECAEQAw&biw=1920&bih=902&dpr=1"
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-Logger"])
    driver = webdriver.Chrome(options=options)
    crawler_path = "C:/Users/SSAFY/github_repository/ssafy/crawler/chromedriver.exe"  # 자신의 chomedriver의 경로 위치

    @classmethod
    def file_save(cls, img_url, save_path):
        urllib.request.urlretrieve(img_url, save_path)

    @classmethod
    def get_image_url(cls, image_xpath):
        # image_xpath = "/html/body/div[7]/div/div[11]/div[2]/div/div/div[2]/div/div[1]/div/div/div/div/div/div/div/div/div/div[2]/div/div[1]/a/div/div/img"
        img_object = cls.driver.find_element("xpath", image_xpath)
        img_url = img_object.get_attribute("src")
        return img_url

    @classmethod
    def main(cls, player_names):
        """
        :desc 지점 위치를 크롤링해서 리턴하는 함수
        :return {지점:주소} 형태로 리턴한다.
        """
        log_file_name: str = "player_save.txt"
        cls.driver.get(cls.GOOGLE_URL)
        for i in range(len(player_names)):
            sec = int(random.random()*5)
            print(f"1. random waiting sec:{sec}")
            search_object = cls.driver.find_element("xpath",
                                                    '/html/body/c-wiz/c-wiz/div/div[3]/div[2]/div/div[1]/form/div[1]/div[2]/div/div[2]/input')
            # 이름 보내기
            search_object.clear()
            time.sleep(sec)
            search_object.send_keys(str(player_names[i][0])+" "+ player_names[i][1])
            # 엔터치기
            search_object.send_keys(Keys.ENTER)

            image_name = f"{player_names[i][0]} {player_names[i][1]}"
            save_path = f"{os.getcwd()}/result/{image_name}.jpg"
            print(save_path)
            # save_path = f"C:/Users/SSAFY/Desktop/result/{image_name}.jpg"
            try:
                image_xpath = "/html/body/div[2]/c-wiz/div[3]/div[1]/div/div/div/div[1]/div[1]/span/div[1]/div[1]/div[1]/a[1]/div[1]/img"
                img_url = cls.get_image_url(image_xpath)
            except:
                Logger.save_error_log_to_file(f"{image_name}", log_file_name)
                print(f"{i} except")

            cls.file_save(img_url, save_path)
            print(f"2. random waiting sec:{sec}")
            time.sleep(sec)

            Logger.save_log_to_file(f"success_{i} {player_names[i]}",log_file_name)
            print(f"{i} success")

