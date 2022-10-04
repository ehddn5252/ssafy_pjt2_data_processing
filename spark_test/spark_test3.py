from pyspark.sql import SparkSession, DataFrame
from pprint import pprint
from pyspark.sql.functions import count
# PySpark 세션 불러오기
spark = SparkSession.builder.master("local").appName("mlbti").getOrCreate()
# 일을 csv 파일 dataframe으로 가져오기 pyspark.sql.dataframe.DataFrame form
df = spark.read.options(header=False, inferSchema=True).csv('./data/events_sample.csv')  # load csv
new_columns = ['uid', 'player_type', 'player_uid', 'date', 'game_uid', 'season', 'weather', 'opponent_uid',
               'event_index', 'event_type', 'player_main_position', 'opponent_hand', 'rbi', 'strikes', 'balls', 'outs',
               'inning', 'is_top_inning', 'type', 'creater', 'updater', 'create_time', 'update_time']
for i in range(len(df.columns)):
    df = df.withColumnRenamed(df.columns[i], new_columns[i])

# df.select("uid", "event_type", "player_uid", count("opponent_uid")).show(10)

# df.select(count("opponent_uid").alias("count")).show()


df.select("uid", "event_type").write.format("jdbc").option("url", "jdbc:mysql://j7e202.p.ssafy.io:3306/mlbti").option("driver", "com.mysql.jdbc.cj.Driver").option("dbtable", "sample_table").option("user", "ssafy").option("password", "!321yfass").save()

# df.select("uid","event_type","player_uid","opponent_uid").write.format("jdbc").option("url", "jdbc:mysql://127.0.0.1:3306/dumps&useUnicode=true&characterEncoding=UTF-8&useSSL=false") \
# 	.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "batter_test").option("user", "root").option("password", "sunbi6311!").save()
