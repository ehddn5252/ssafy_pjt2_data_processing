from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import urllib.request

from Logger.logger import Logger


class CrawlingGoogleMain:
    # player_names = ["박찬호", "류현진", "강정호", "강민호", "이대호"]
    GOOGLE_URL = "https://www.google.com/search?q=%EA%B0%95%EB%AF%BC%ED%98%B8&hl=ko&source=hp&ei=Xt0SY66aMov50AT9wpaoBw&iflsig=AJiK0e8AAAAAYxLrbpMccpvncGuzk74MZX9ZPU7xejOe&ved=0ahUKEwiuweDS6ff5AhWLPJQKHX2hBXUQ4dUDCAk&uact=5&oq=%EA%B0%95%EB%AF%BC%ED%98%B8&gs_lcp=Cgdnd3Mtd2l6EAMyCAguEIAEELEDMgsILhCABBCxAxCDATIECAAQAzILCC4QgAQQsQMQgwEyCwguEIAEELEDEIMBMgsIABCABBCxAxCDATIICC4QgAQQsQMyBQguEIAEMggILhCABBDUAjIICC4QgAQQsQM6EQguEIAEELEDEIMBEMcBENEDOgUIABCABDoOCC4QgAQQsQMQgwEQ1AI6FwgAEOoCELQCEIoDELcDENQDEOUCEIsDOhcILhDqAhC0AhCKAxC3AxDUAxDlAhCLAzoUCAAQ6gIQtAIQigMQtwMQ1AMQ5QI6FAgAEOoCELQCEIoDELcDEOUCEIsDOgQILhADOhQILhCABBCxAxCDARDHARDRAxDUAjoICAAQgAQQsQM6CwguEIAEEMcBEK8BUABY3xJgrhNoBHAAeAGAAWWIAfAFkgEDNy4xmAEAoAEBsAEKuAEC&sclient=gws-wiz"
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

            search_object = cls.driver.find_element("xpath",
                                                    '/html/body/div[4]/div[2]/form/div[1]/div[1]/div[2]/div/div[2]/input')
            # 이름 보내기
            search_object.clear()
            time.sleep(2)
            search_object.send_keys(player_names[i])
            # 엔터치기
            search_object.send_keys(Keys.ENTER)

            image_name = f"{player_names[i][0]}"
            save_path = f"C:/Users/SSAFY/Desktop/tmp/{image_name}.jpg"
            try:
                image_xpath = "/html/body/div[7]/div/div[11]/div[2]/div/div/div[2]/div/div[1]/div/div/div/div/div/div/div/div/div/div[2]/div/div[1]/a/div/div/img"
                img_url = cls.get_image_url(image_xpath)
            except:
                try:
                    image_xpath = "/html/body/div[7]/div/div[7]/div[1]/div[2]/div/div[1]/div/div[1]/g-scrolling-carousel/div[1]/div/a[1]/div/div/div[2]/g-img/img"
                    img_url = cls.get_image_url(image_xpath)
                except:
                    try:
                        image_xpath = "/html/body/div[7]/div/div[11]/div[1]/div[2]/div[2]/div/div/div[2]/div/div/div[2]/g-section-with-header/div[2]/div[2]/div/div/div[1]/div[1]/div/div/img"
                        img_url = cls.get_image_url(image_xpath)
                    except:
                        try:
                            image_xpath = "/html/body/div[7]/div/div[11]/div[2]/div/div/div[2]/div/div[1]/div/div[2]/div/div/a/div/div/g-img/img"
                            img_url = cls.get_image_url(image_xpath)
                        except:
                            try:
                                image_xpath = "/html/body/div[7]/div/div[11]/div[1]/div[2]/div[2]/div/div/div[1]/div/div/div[2]/g-section-with-header/div[2]/div[2]/div/div/div[1]/div[1]/div/div/img"
                                img_url = cls.get_image_url(image_xpath)
                            except:
                                Logger.save_error_log_to_file(f"{image_name}", log_file_name)
                                print(f"{i} except")

            print("img_url")
            print(img_url)
            cls.file_save(img_url, save_path)
            time.sleep(1)
            Logger.save_log_to_file(f"success_{i} {image_name}",log_file_name)
            print(f"{i} success")
