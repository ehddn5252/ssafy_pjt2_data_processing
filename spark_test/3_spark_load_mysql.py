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
table = "users"
user = "ssafy"
password = "!321yfass"
jdbc = spark.read.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("url", "jdbc:mysql://{}:3306/{}?serverTimezone=Asia/Seoul ".format(sql_url, database)).option("user", user).option("password", password).option("dbtable", table).load()
print(type(jdbc))
jdbc.show(10)
# df.toDF('uid', 'player_type', 'player_uid', 'date', 'game_uid', 'season', 'weather', 'opponent_uid', 'event_index', 'event_type', 'player_main_position', 'opponent_hand', 'rbi', 'strikes', 'balls', 'outs', 'inning', 'is_top_inning', 'type', 'creater', 'updater','create_time','update_time') # column name 변경
# df.withColumnRenamed('old', 'new')
# df.columns # show columns
# df.show(10)
# df.dtypes
# 'uid', 'player_type', 'player_uid', 'date', 'game_uid', 'season', 'weather', 'opponent_uid', 'event_index', 'event_type', 'player_main_position', 'opponent_hand', 'rbi', 'strikes', 'balls', 'outs', 'inning', 'is_top_inning','at_bat'

# df.drop('mpg')
# df[df.mpg > 20] # filtering
# df[(df.mpg > 20) & (df.cyl == 6)] # operation

# import pyspark.sql.functions as F
# df.withColumn('logdisp', F.log(df.log(df.disp)) # transformation
# numpy 쓸 수 있지만 되도록 built-in function 사용할 것

# df.sample(False, 0.1).toPandas().hist() # histogram