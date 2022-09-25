from bs4 import BeautifulSoup
from selenium import webdriver
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


class Crawler:
    store_names: list = ["계양점", "광명점", "구로점", "금정점", "마산점", "명일점", "목동점", "미아점", "반야월점", "보라점", \
                         "분당점", "사천점", "산본점", "상봉점", "성남점", "성수점", "세종점", "수색점", "수지점", "신도림점", \
                         "신월점", "신촌점", "양산점", "에코시티점", "영등포점", "용산점", "월계점", "월배점", "은평점", "죽전점", \
                         "중동점", "창동점", "창원점", "천안터미널점"]

    mean_rates: list = {"계양점": [3.045, 44], "광명점": [3.050, 20], "구로점": [3.568, 88], "금정점": [1.625, 8],
                        "마산점": [2.844, 45], "명일점": [3.831, 65], "목동점": [4.188, 229], "미아점": [3.057, 70],
                        "반야월점": [3.000, 6], "보라점": [3.476, 21], "분당점": [3.152, 99], "사천점": [5.000, 1],
                        "산본점": [3.074, 94], "상봉점": [3.439, 41], "성남점": [3.634, 82],"성수점": [4.302, 388],
                        "세종점": [2.944, 18], "수색점": [3.500, 30], "수지점": [3.533, 75], "신도림점": [3.733, 97],
                        "신월점": [3.375, 16], "신촌점": [3.957, 46], "양산점": [5.000, 1], "에코시티점": [3.281, 32],
                        "영등포점": [4.222, 171], "용산점": [3.455, 55], "월계점": [3.509, 106], "월배점": [2.526, 19],
                        "은평점": [2.981, 156], "죽전점": [3.036, 28],"중동점": [4.067, 104], "창동점": [3.484, 62],
                        "창원점": [3.167, 6], "천안터미널점": [5.000, 2]}

    @classmethod
    def store_location_crawler(cls):
        """
        :desc 지점 위치를 크롤링해서 리턴하는 함수
        :return {지점:주소} 형태로 리턴한다.
        """

        path = "C:/Users/SSAFY/PycharmProjects/emart/chromedriver.exe"  # 자신의 chomedriver의 경로 위치
        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["enable-Logger"])
        driver = webdriver.Chrome(options=options)

        driver.get("https://store.emart.com/branch/list.do?trcknCode=header_store")
        address_dicts: dict = {}
        # user_id = driver.find_element_by_xpath("/html/body/div/section[2]/form/div[1]/div/input")
        user_id = driver.find_element("id", 'searchBar')
        for i in range(len(cls.store_names)):
        # for i in range(3):
        # 이름 보내기
            user_id.send_keys(cls.store_names[i])
            time.sleep(0.4)
            # 검색
            driver.find_element("xpath", "/html/body/div/section[2]/form/div[1]/div/button").send_keys(Keys.ENTER)
            time.sleep(0.4)
            # 검색 후의 Xpath 는 다 똑같음 (/html/body/div/section[2]/form/div[2]/div/div[2]/div[2]/div/ul/li/a)
            driver.find_element("xpath",
                                "/html/body/div/section[2]/form/div[2]/div/div[2]/div[2]/div/ul/li/a").send_keys(
                Keys.ENTER)
            time.sleep(0.2)
            data = driver.find_element("xpath",
                                       "/html/body/div/section[3]/div/div[2]/div[2]/div[1]/ul/li[2]/dl/dd[2]").text
            time.sleep(0.2)
            user_id.clear()
            address_dicts[cls.store_names[i]] = data
        return address_dicts

    @staticmethod
    def avg_grade_crawler():
        """
        :desc 지점별 만족도와 평점을 리턴한다. 매일 요약되는 배치라 crwaler에 넣었으나  일회성 데이터라 모아서 전송
        :return {지점:{평점,고객수}}} 형태로 리턴한다.
        """
        return Crawler.mean_rates
