import pymysql
import statsapi
from DB.DML import DML
from typing import Tuple

if __name__ == "__main__":
    dml_instance = DML()
    raw_data: Tuple = None

    sql = f"select distinct(game_id) from schedules;"
    schedule_game_ids = dml_instance.execute_fetch_sql(sql, [])
    game_id_list = []
    for i in schedule_game_ids:
        game_id_list.append(i[0])

    val_list = []
    for year in range(2022, 2000, -1):
        print('year: ' + str(year))
        games = statsapi.schedule(start_date='01/01/' + str(year), end_date='12/31/' + str(year))
        print(len(games))
        for i in games:
            if i.get("game_id") not in game_id_list:
                print("it is in schedule")
                continue
            game_id = i.get("game_id")
            game_datetime = i.get("game_datetime")
            game_date = i.get("game_date")
            game_type = i.get("game_type")
            status = i.get("status")
            away_name = i.get("away_name")
            home_name = i.get("home_name")
            away_id = i.get("away_id")
            home_id = i.get("home_id")
            doubleheader = i.get("doubleheader")
            game_num = i.get("game_num")
            home_probable_pitcher = i.get("home_probable_pitcher")
            away_probable_pitcher = i.get("away_probable_pitcher")
            home_pitcher_note = i.get("home_pitcher_note")
            away_pitcher_note = i.get("away_pitcher_note")
            away_score = i.get("away_score")
            home_score = i.get("home_score")
            current_inning = i.get("current_inning")
            inning_state = i.get("inning_state")
            venue_id = i.get("venue_id")
            venue_name = i.get("venue_name")
            winning_team = i.get("winning_team")
            losing_team = i.get("losing_team")
            winning_pitcher = i.get("winning_pitcher")
            losing_pitcher = i.get("losing_pitcher")
            save_pitcher = i.get("save_pitcher")
            summary = i.get("summary")

            sql = "insert into new_schedules values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            vals = (
            game_id, game_datetime, game_date, game_type, status, away_name, home_name, away_id, home_id, doubleheader,
            game_num, home_probable_pitcher, away_probable_pitcher
            , home_pitcher_note, away_pitcher_note, away_score, home_score, current_inning, inning_state, venue_id,
            venue_name, winning_team, losing_team, winning_pitcher, losing_pitcher
            , save_pitcher, summary)
            val_list.append(vals)
        try:
            dml_instance.execute_insert_many_sql(sql, val_list)
        except pymysql.err.IntegrityError as e:
            print(e)
