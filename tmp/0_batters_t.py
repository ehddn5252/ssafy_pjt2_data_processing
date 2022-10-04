
from DB.DML import DML

sql = "SELECT DISTINCT player_uid from new_event_batters"
dml_instance = DML()
res = dml_instance.execute_fetch_sql(sql,[])
i =0
for r in res:
    print(i)
    i += 1
    player_uid = r[0]
    sql = "select distinct season from new_event_batters where player_uid = %s"
    res2 = dml_instance.execute_fetch_sql(sql, player_uid)
    for s in res2:
        try:
            season = s[0]
            sql = "select full_name, primary_position_abbreviation from baseball_players where uid = %s"
            res3 = dml_instance.execute_fetch_sql(sql, player_uid)
            name = res3[0][0]
            position = res3[0][1]

            sql = "SELECT opponent_hand, event, sum(count) as count " \
                  " FROM new_event_batters " \
                  "where player_uid = %s " \
                  "and season = %s " \
                  "and is_hit = true " \
                  "group by opponent_hand, event"
            res4 = dml_instance.execute_fetch_sql(sql, player_uid)
            left_twob_hit_num =0
            left_threeb_hit_num =0
            left_hr_num =0
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
                  " FROM new_event_batters " \
                  "where player_uid = %s " \
                  "and season = %s " \
                  "and is_hit = false " \
                  "group by opponent_hand, event"
            res5 = dml_instance.execute_fetch_sql(sql, (player_uid, season))
            left_bb_num = 0
            right_bb_num =0
            left_ao_num =0
            right_ao_num= 0
            left_go_num=0
            right_go_num =0
            left_so_num =0
            right_so_num=0
            left_sh_num=0
            right_sh_num=0
            left_sf_num=0
            right_sf_num=0
            left_ibb_num=0
            right_ibb_num =0
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

            sql = "SELECT *" \
                  "FROM new_event_batter_counts " \
                  "where player_uid = %s " \
                  "and season = %s " \
                  "and opponent_hand = %s"
            res6 = dml_instance.execute_fetch_sql(sql, (player_uid, season, "L"))
            left_rbi = res6[0][4]
            left_strike_num = res6[0][5]
            left_ball_num = res6[0][6]
            left_game_num = res6[0][7]

            sql = "SELECT *" \
                  "FROM new_event_batter_counts " \
                  "where player_uid = %s " \
                  "and season = %s " \
                  "and opponent_hand = %s"
            res7 = dml_instance.execute_fetch_sql(sql, (player_uid, season, "R"))
            right_rbi = res7[0][4]
            right_strike_num = res7[0][5]
            right_ball_num = res7[0][6]
            right_game_num = res7[0][7]
            sql = "select count(*) " \
                  "from new_event_batters " \
                  "where at_bat= %s and player_uid = %s and season = %s and opponent_hand = %s"

            left_at_bat_num = dml_instance.execute_fetch_sql(sql, (True, player_uid, season, "L"))[0][0]
            sql = "select count(*) " \
                  "from new_event_batters " \
                  "where at_bat= %s and player_uid = %s and season = %s and opponent_hand = %s"

            right_at_bat_num = dml_instance.execute_fetch_sql(sql, (True, player_uid, season, "R"))[0][0]
            sql = "select count(*) " \
                  "from new_event_batters " \
                  "where pa= %s and player_uid = %s and season = %s and opponent_hand = %s"
            left_pa_num = dml_instance.execute_fetch_sql(sql, (True, player_uid, season, "L"))[0][0]
            sql = "select count(*) " \
                  "from new_event_batters " \
                  "where pa= %s and player_uid = %s and season = %s and opponent_hand = %s"
            right_pa_num = dml_instance.execute_fetch_sql(sql, (True, player_uid, season, "R"))[0][0]
            sql = "insert into batters(player_uid, season, name, position, left_hit_num, " \
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
                  "%s, %s)"
            dml_instance.execute(sql, (player_uid, season, name, position, left_hit_num, right_hit_num, left_twob_hit_num, right_twob_hit_num, left_threeb_hit_num, right_threeb_hit_num, left_hr_num, right_hr_num, left_pa_num, right_pa_num, left_at_bat_num, right_at_bat_num, left_rbi, right_rbi, right_game_num, left_game_num, 0, 0, left_bb_num, right_bb_num, left_ao_num, right_ao_num, left_go_num, right_go_num, left_so_num, right_so_num, 0, 0, left_sh_num, right_sh_num, left_sf_num, right_sf_num, left_ibb_num, right_ibb_num, left_strike_num, right_strike_num, left_ball_num, right_ball_num))
            dml_instance.commit()
        except:
            pass
# conn.commit()
# for event in new_event_to_pitchers_dict:
#     sql = "UPDATE new_event_batters SET is_hit = %s, at_bat = %s, pa = %s WHERE event = %s"
#     cursor.execute(sql, [new_event_to_pitchers_dict[event][0], new_event_to_pitchers_dict[event][1], new_event_to_pitchers_dict[event][2], event])
#     conn.commit()

dml_instance.commit()
dml_instance.close()