




# 1분에 한번씩 업데이트하기
def update_schedule():
    import pymysql
    import statsapi
    from DB.DML import DML
    from datetime import date
    dml_instance = DML()
    date.today()
    today = str(date.today())
    today_year = today[0:4]
    today_month = today[5:7]
    today_day = today[8:10]

    date = today_month + "/" + today_day + "/" + today_year
    if int(today_month) != 1:
        last_month = "0" + str((int(today_month) - 1))
        start_date = last_month + "/01/" + today_year
    else:
        start_date = "01/01/" + today_year

    for year in range(int(today_year), int(today_year)-1, -1):
        games = statsapi.schedule(start_date=start_date, end_date=date)
        for i in games:
            print("===========================")
            print("start update schedules")
            print("===========================")
            game_id = i.get("game_id")
            status = i.get("status")
            home_probable_pitcher = i.get("home_probable_pitcher")
            away_probable_pitcher = i.get("away_probable_pitcher")
            home_pitcher_note = i.get("home_pitcher_note")
            away_pitcher_note = i.get("away_pitcher_note")
            away_score = i.get("away_score")
            home_score = i.get("home_score")
            current_inning = i.get("current_inning")
            inning_state = i.get("inning_state")
            winning_team = i.get("winning_team")
            losing_team = i.get("losing_team")
            winning_pitcher = i.get("winning_pitcher")
            losing_pitcher = i.get("losing_pitcher")
            save_pitcher = i.get("save_pitcher")
            summary = i.get("summary")
            sql = f"update schedules set status = %s, home_probable_pitcher = %s, away_probable_pitcher = %s, home_pitcher_note = %s, away_pitcher_note = %s, away_score = %s, home_score = %s, current_inning = %s,inning_state = %s, winning_team = %s, losing_team = %s, winning_pitcher = %s, losing_pitcher = %s, save_pitcher = %s, summary = %s "
            # sql = f"update schedules set status = '{status}', home_probable_pitcher = '{home_probable_pitcher}', away_probable_pitcher = '{away_probable_pitcher}', home_pitcher_note = '{home_pitcher_note}', away_pitcher_note = '{away_pitcher_note}', away_score = '{away_score}', home_score = '{home_score}', current_inning = '{current_inning}',inning_state = '{inning_state}', winning_team = '{winning_team}', losing_team = '{losing_team}', winning_pitcher = '{winning_pitcher}', losing_pitcher = '{losing_pitcher}', save_pitcher = '{save_pitcher}', summary = '{summary}' "
            sql += f"where game_id = {game_id}"
            vals = (
                status, home_probable_pitcher, away_probable_pitcher, home_pitcher_note, away_pitcher_note, away_score,
                home_score, current_inning, inning_state, winning_team, losing_team, winning_pitcher, losing_pitcher,
                save_pitcher, summary)
            try:
                # dml_instance.execute_sql(sql)
                dml_instance.execute_update_sql(sql, vals)
            except pymysql.err.IntegrityError as e:
                print(e)


if __name__ == "__main__":
    # 업데이트할 것: status, home_probable_pitcher, away_probable_pitcher, home_pitcher_note, away_pitcher_note, away_score, home_score, current_inning, inning_state, winning_team, losing_team, winning_pitcher, losing_pitcher, summary
    # 매일 업데이터할 것
    dml_instance = DML()
    update_schedule(dml_instance)
