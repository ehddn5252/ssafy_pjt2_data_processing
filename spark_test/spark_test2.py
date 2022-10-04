from pyspark.sql import SparkSession, DataFrame
from pprint import pprint
# PySpark

# PySpark 세션 불러오기
spark = SparkSession.builder.master("local").appName("mlbti").getOrCreate()
# 일을 csv 파일 dataframe으로 가져오기 pyspark.sql.dataframe.DataFrame form
df = spark.read.options(header=False, inferSchema=True).csv('./data/events_sample.csv') # load csv
#
# df.show() # show df
print("type is")
print(type(df))
exit(-1)
df.show(10)
df.toDF('uid', 'player_type', 'player_uid', 'date', 'game_uid', 'season', 'weather', 'opponent_uid', 'event_index', 'event_type', 'player_main_position', 'opponent_hand', 'rbi', 'strikes', 'balls', 'outs', 'inning', 'is_top_inning', 'type', 'creater', 'updater','create_time','update_time') # column name 변경
df.withColumnRenamed('old', 'new')
df.columns # show columns
df.show(10)
df.dtypes
# 'uid', 'player_type', 'player_uid', 'date', 'game_uid', 'season', 'weather', 'opponent_uid', 'event_index', 'event_type', 'player_main_position', 'opponent_hand', 'rbi', 'strikes', 'balls', 'outs', 'inning', 'is_top_inning','at_bat'

# df.drop('mpg')
# df[df.mpg > 20] # filtering
# df[(df.mpg > 20) & (df.cyl == 6)] # operation

# import pyspark.sql.functions as F
# df.withColumn('logdisp', F.log(df.log(df.disp)) # transformation
# numpy 쓸 수 있지만 되도록 built-in function 사용할 것

# df.sample(False, 0.1).toPandas().hist() # histogram