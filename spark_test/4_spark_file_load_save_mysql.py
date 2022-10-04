from pyspark.sql import SparkSession, DataFrame, SQLContext
from pprint import pprint
# PySpark
import pyspark

conf = pyspark.SparkConf().setAppName("spark-sql").set("spark.driver.extraClassPath", "./data/mysql-connector-java-8.0.30.jar")
sc = pyspark.SparkContext(conf=conf)
sqlCtx = SQLContext(sc)
# PySpark 세션 불러오기
# spark = SparkSession.builder.master("local").appName("mlbti").getOrCreate()
spark = SparkSession.builder.getOrCreate()



# df.select("uid", "event_type").write.format("jdbc").option("url", "jdbc:mysql://j7e202.p.ssafy.io:3306/mlbti").ption("driver", "com.mysql.jdbc.cj.Driver").option("dbtable", "sample_table").option("user", "ssafy").option("password", "!321yfass").save()
sql_url = "j7e202.p.ssafy.io"
database = "mlbti"
table = "sample_table"
user = "ssafy"
password = "!321yfass"
# jdbc = spark.read.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("url", "jdbc:mysql://{}:3306/{}?serverTimezone=Asia/Seoul ".format(sql_url, database)).option("user", user).option("password", password).option("dbtable", table).load()
df = spark.read.options(header=False, inferSchema=True).csv('./data/events_sample.csv')  # load csv
new_columns = ['uid', 'player_type', 'player_uid', 'date', 'game_uid', 'season', 'weather', 'opponent_uid',
               'event_index', 'event_type', 'player_main_position', 'opponent_hand', 'rbi', 'strikes', 'balls', 'outs',
               'inning', 'is_top_inning', 'type', 'creater', 'updater', 'create_time', 'update_time']
for i in range(len(df.columns)):
    df = df.withColumnRenamed(df.columns[i], new_columns[i])
# jdbc.show(10)
df.select("uid", "event_type").write.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("url", "jdbc:mysql://{}:3306/{}?serverTimezone=Asia/Seoul ".format(sql_url, database)).option("user", user).option("password", password).option("dbtable", table).save()
