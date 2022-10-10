import pymysql
from DB.DML import DML
# conn = pymysql.connect(host='j7e202.p.ssafy.io', user='ssafy', password='!321yfass', db='mlbti', charset='utf8')
#cursor = conn.cursor()
EVENT_BATTERS_TABLE = "new_new_new_event_batters"
BATTERS_TABLE = "new_batters"
EVENT_BATTER_COUNTS_TABLE = "new_new_new_event_batter_counts"
YEAR = 2022

def stack_batters():
    dml_instance = DML()

    sql = "SELECT DISTINCT player_uid from new_new_new_event_batters"
    dml_instance.execute(sql)
    res = dml_instance.fetch_all()
    i = 0
    for r in res:
        print(i)
        i += 1
        player_uid = r[0]
        sql = f"select distinct team from {EVENT_BATTERS_TABLE} where player_uid = %s"
        dml_instance.execute(sql, player_uid)
        resTeam = dml_instance.fetch_all()
        for t in resTeam:
            team = t[0]
            sql = f"select distinct season from {EVENT_BATTERS_TABLE} where player_uid = %s and team = %s"
            dml_instance.execute(sql, (player_uid, team))
            res2 = dml_instance.fetch_all()
            val_list = []
            for s in res2:
                season = s[0]
                sql = "select full_name, primary_position_abbreviation from baseball_players where uid = %s"
                dml_instance.execute(sql, (player_uid))
                res3 = dml_instance.fetch_all()
                name = res3[0][0]
                position = res3[0][1]

                sql = "SELECT opponent_hand, event, sum(count) as count " \
                      f" FROM {EVENT_BATTERS_TABLE} " \
                      "where player_uid = %s " \
                      "and season = %s " \
                      "and team = %s " \
                      "and is_hit = true " \
                      "group by opponent_hand, event"
                dml_instance.execute(sql, (player_uid, season, team))
                res4 = dml_instance.fetch_all()
                left_twob_hit_num = 0
                left_threeb_hit_num = 0
                left_hr_num = 0
                left_hit_num = 0
                right_twob_hit_num = 0
                right_threeb_hit_num = 0
                right_hr_num = 0
                right_hit_num = 0
                for temp in res4:
                    if temp[0] == 'L':
                        if temp[1] == 'Single':
                            left_hit_num += temp[2]
                        elif temp[1] == 'Double':
                            left_hit_num += temp[2]
                            left_twob_hit_num = temp[2]
                        elif temp[1] == 'Triple':
                            left_hit_num += temp[2]
                            left_threeb_hit_num = temp[2]
                        elif temp[1] == 'Home Run':
                            left_hit_num += temp[2]
                            left_hr_num = temp[2]
                    else:
                        if temp[1] == 'Single':
                            right_hit_num += temp[2]
                        elif temp[1] == 'Double':
                            right_hit_num += temp[2]
                            right_twob_hit_num = temp[2]
                        elif temp[1] == 'Triple':
                            right_hit_num += temp[2]
                            right_threeb_hit_num = temp[2]
                        elif temp[1] == 'Home Run':
                            right_hit_num += temp[2]
                            right_hr_num = temp[2]
                sql = "SELECT opponent_hand, event, sum(count) as count " \
                      f" FROM {EVENT_BATTERS_TABLE} " \
                      "where player_uid = %s " \
                      "and season = %s " \
                      "and team = %s " \
                      "and is_hit = false " \
                      "group by opponent_hand, event"
                dml_instance.execute(sql, (player_uid, season, team))
                res5 = dml_instance.fetch_all()
                left_bb_num = 0
                right_bb_num = 0
                left_ao_num = 0
                right_ao_num = 0
                left_go_num = 0
                right_go_num = 0
                left_so_num = 0
                right_so_num = 0
                left_sh_num = 0
                right_sh_num = 0
                left_sf_num = 0
                right_sf_num = 0
                left_ibb_num = 0
                right_ibb_num = 0
                for temp in res5:
                    if temp[0] == 'L':
                        if temp[1] == 'Walk':
                            left_bb_num += temp[2]
                        elif temp[1] == 'Flyout':
                            left_ao_num = temp[2]
                        elif temp[1] == 'Groundout':
                            left_go_num = temp[2]
                        elif temp[1] == 'Strikeout':
                            left_so_num = temp[2]
                        elif temp[1] == 'Sac Bunt':
                            left_sh_num = temp[2]
                        elif temp[1] == 'Sac Fly':
                            left_sf_num = temp[2]
                        elif temp[1] == 'Intent Walk Run':
                            left_ibb_num = temp[2]
                    else:
                        if temp[1] == 'Walk':
                            right_bb_num += temp[2]
                        elif temp[1] == 'Flyout':
                            right_ao_num = temp[2]
                        elif temp[1] == 'Groundout':
                            right_go_num = temp[2]
                        elif temp[1] == 'Strikeout':
                            right_so_num = temp[2]
                        elif temp[1] == 'Sac Bunt':
                            right_sh_num = temp[2]
                        elif temp[1] == 'Sac Fly':
                            right_sf_num = temp[2]
                        elif temp[1] == 'Intent Walk Run':
                            right_ibb_num = temp[2]
                try:
                    sql = "SELECT *" \
                          f"FROM {EVENT_BATTER_COUNTS_TABLE} " \
                          "where player_uid = %s " \
                          "and season = %s " \
                          "and opponent_hand = %s"
                    dml_instance.execute(sql, (player_uid, season, "L"))
                    res6 = dml_instance.fetch_all()
                    left_rbi = res6[0][4]
                    left_strike_num = res6[0][5]
                    left_ball_num = res6[0][6]
                    left_game_num = res6[0][7]
                except:
                    pass
                try:
                    sql = "SELECT *" \
                          f"FROM {EVENT_BATTER_COUNTS_TABLE} " \
                          "where player_uid = %s " \
                          "and season = %s " \
                          "and opponent_hand = %s"
                    dml_instance.execute(sql, (player_uid, season, "R"))
                    res7 = dml_instance.fetch_all()
                    right_rbi = res7[0][4]
                    right_strike_num = res7[0][5]
                    right_ball_num = res7[0][6]
                    right_game_num = res7[0][7]
                except:
                    pass
                sql = "select count(*) " \
                      f"from {EVENT_BATTERS_TABLE} " \
                      "where at_bat= %s and player_uid = %s and season = %s and team = %s and opponent_hand = %s"
                dml_instance.execute(sql, (True, player_uid, season, team, "L"))
                left_at_bat_num = dml_instance.fetch_all()[0][0]
                sql = "select count(*) " \
                      f"from {EVENT_BATTERS_TABLE} " \
                      "where at_bat= %s and player_uid = %s and season = %s and team = %s and opponent_hand = %s"
                dml_instance.execute(sql, (True, player_uid, season, team, "R"))
                right_at_bat_num = dml_instance.fetch_all()[0][0]
                sql = "select count(*) " \
                      f"from {EVENT_BATTERS_TABLE} " \
                      "where pa= %s and player_uid = %s and season = %s and team = %s and opponent_hand = %s"
                dml_instance.execute(sql, (True, player_uid, season, team, "L"))
                left_pa_num = dml_instance.fetch_all()[0][0]
                sql = "select count(*) " \
                      f"from {EVENT_BATTERS_TABLE} " \
                      "where pa= %s and player_uid = %s and season = %s and team = %s and opponent_hand = %s"
                dml_instance.execute(sql, (True, player_uid, season, team, "R"))
                right_pa_num = dml_instance.fetch_all()[0][0]
                sql = f"insert into {BATTERS_TABLE} (player_uid, season, name, position, team_name, left_hit_num, " \
                      "right_hit_num, left_twob_hit_num, right_twob_hit_num, left_threeb_hit_num, right_threeb_hit_num, " \
                      "left_hr_num, right_hr_num, left_pa_num, right_pa_num, left_at_bat_num, " \
                      "right_at_bat_num, left_rbi, right_rbi, right_game_num, left_game_num, " \
                      "left_ops, right_ops, left_bb_num, right_bb_num, left_ao_num, " \
                      "right_ao_num, left_go_num, right_go_num, left_so_num, right_so_num, " \
                      "left_dp_num, right_dp_num, left_sh_num, right_sh_num, left_sf_num, " \
                      "right_sf_num, left_ibb_num, right_ibb_num, left_strike_num, right_strike_num, " \
                      "left_ball_num, right_ball_num) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                      "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                      "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                      "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                      "%s, %s, %s)"
                val_list.append((player_uid, season, name, position, team, left_hit_num, right_hit_num,
                                 left_twob_hit_num, right_twob_hit_num, left_threeb_hit_num,
                                 right_threeb_hit_num, left_hr_num, right_hr_num, left_pa_num, right_pa_num,
                                 left_at_bat_num, right_at_bat_num, left_rbi, right_rbi, right_game_num,
                                 left_game_num, 0, 0, left_bb_num, right_bb_num, left_ao_num, right_ao_num, left_go_num,
                                 right_go_num, left_so_num,
                                 right_so_num, 0, 0, left_sh_num, right_sh_num, left_sf_num, right_sf_num, left_ibb_num,
                                 right_ibb_num, left_strike_num,
                                 right_strike_num, left_ball_num, right_ball_num))
            dml_instance.execute_insert_many_sql(sql, val_list)
            dml_instance.commit()

    dml_instance.close()

def remove_season_batters():
    dml_instance = DML()
    delete_sql = f"delete from {BATTERS_TABLE} where season ={YEAR}"
    dml_instance.execute(delete_sql)
    dml_instance.commit()

if __name__=="__main__":
    #remove_season_batters()
    stack_batters()