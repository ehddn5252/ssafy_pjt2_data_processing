from pyspark.sql import SparkSession, DataFrame, SQLContext
from pprint import pprint
# PySpark
from datetime import datetime
import time
import pyspark
import json
sql_url = "j7e202.p.ssafy.io"
database = "mlbti"
table = "new_schedules"
user = "ssafy"
password = "!321yfass"
import requests

if __name__=="__main__":

    conf = pyspark.SparkConf().setAppName("spark-sql").set("spark.driver.extraClassPath", "./data/mysql-connector-java-8.0.30.jar")
    sc = pyspark.SparkContext(conf=conf)
    sqlCtx = SQLContext(sc)
    # PySpark 세션 불러오기
    # spark = SparkSession.builder.master("local").appName("mlbti").getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    # .createOrReplaceTempView("mytable")
    jdbc = spark.read.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("url","jdbc:mysql://{}:3306/{}?serverTimezone=Asia/Seoul ".format(sql_url, database)).option("user", user).option("password", password).option("dbtable", table).load()

    
    # 스파크 필드수는 25개로 한정되어 있는데, 이를 설정해줌
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    spark.read.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("url", "jdbc:mysql://{}:3306/{}?serverTimezone=Asia/Seoul ".format(sql_url, database)).option("user", user).option("password", password).option("dbtable", "game_raw_datas").load().createOrReplaceTempView("game_raw_datas")

    sql = "select distinct(game_id) from schedules where game_id not in (select distinct(game_uid) from game_raw_datas) and game_id > 191492 and game_id <= 225000"
    sql = "select distinct(game_id) from schedules where game_id > 191492 and game_id <= 225000"
    game_ids = spark.sql(sql)

    print(game_ids.collect())
    t = game_ids.toPandas()
    # print(t["game_id"])
    row, col=t.shape
    for i in range(row):
        print(i)
        game_uid = t["game_id"].iloc[i]
        url = 'https://statsapi.mlb.com/api/v1.1/game/' + str(game_uid) + '/feed/live'
        response = requests.get(url)
        contents = response.text
        game_raw_data = json.dumps(json.loads(contents))
        sql = f"insert into new_table(uid, game_uid, game_raw_data) values({i+2},{game_uid}, 'tmp')"
        # timestamp = time.mktime(datetime.today().timetuple())
        # sql = f"insert into new_table(uid, game_uid, game_raw_data, creater,updater,create_time,update_time) values({i},{game_uid}, 'tmp')"
        spark.sql(sql)

    # SQL을 저장하는 부분은 시간이 오래걸린다.
    # print(game_ids.toPandas().loc["game_id"])

    # pandas dataframe 만드는 부분
    # sparkDF = spark.createDataFrame(pandasDF)
    print("============end=============")
    # for game_id in game_ids:


    # df.select("uid", "event_type").write.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("url", "jdbc:mysql://{}:3306/{}?serverTimezone=Asia/Seoul ".format(sql_url, database)).option("user", user).option("password", password).option("dbtable", table).save()
