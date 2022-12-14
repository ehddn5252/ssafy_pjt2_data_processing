import pymysql
import requests
import json

conn = pymysql.connect(host='j7e202.p.ssafy.io', user='ssafy', password='!321yfass', db='mlbti', charset='utf8')

cur = conn.cursor()
cur.execute("select distinct(game_id) from schedules where game_id not in (select distinct(game_uid) from game_raw_datas) and game_id > 191492 and game_id <= 225000")
results = cur.fetchall()


try:
    for result in results:
        game_uid = result[0]
        url = 'https://statsapi.mlb.com/api/v1.1/game/'+str(game_uid)+'/feed/live'
        response = requests.get(url)
        contents = response.text
        game_raw_data = json.dumps(json.loads(contents))
        sql = "insert into game_raw_datas (game_uid, game_raw_data, creater) values(%s, %s, %s)"
        vals = (game_uid, game_raw_data, "")
        cur.execute(sql, vals)
        conn.commit()
except Exception as e:
    print(e)