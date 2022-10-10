# 20221010
import pymysql
import requests
import json
from typing import List, Tuple
from tqdm import tqdm
from Logger.logger import Logger
from DB.DML import DML
from DB.DDL import DDL
from info.process_info import left_event_pitchers_to_pitchers_convert_dict, \
    right_event_pitchers_to_pitchers_convert_dict
import time
from info.process_info import new_event_to_pitchers_dict, pitcher_primary_position_abbreviation
import statsapi

log_file_name = "4_insert_into_pitcher_log.txt"
GAME_RAWDATAS_TABLE = "new_new_new_game_raw_datas"
SCHEDULES_TABLE = "new_new_new_schedules"
EVENTS_TABLE = "new_new_new_events"
EVENT_PITCHERS_TABLE = "new_new_new_event_pitchers"
EVENT_BATTERS_TABLE = "new_new_new_event_batters"
PITCHERS_TABLE = "new_new_new_pitchers"
BATTERS_TABLE = "new_batters"
EVENT_BATTER_COUNTS_TABLE = "new_new_new_event_batter_counts"
BASEBALL_PLAYERS_TABLE = "baseball_players"
YEAR = 2022


def stack_batters(dml_instance):

    sql = f"SELECT DISTINCT player_uid from {EVENT_BATTERS_TABLE}"
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
                sql = f"select full_name, primary_position_abbreviation from {BASEBALL_PLAYERS_TABLE} where uid = %s"
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


def remove_season_batters(dml_instance, year):
    delete_sql = f"delete from {BATTERS_TABLE} where season ={year}"
    dml_instance.execute(delete_sql)
    dml_instance.commit()


def stack_schedules(dml_instance, year):
    val_list = []
    for year in range(year, year - 1, -1):
        print('year: ' + str(year))
        games = statsapi.schedule(start_date='01/01/' + str(year), end_date='12/31/' + str(year))
        for i in games:
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
            sql = f"insert into {SCHEDULES_TABLE}(game_id, game_datetime, game_date, game_type, status, away_name, home_name, away_id, home_id, doubleheader, game_num, home_probable_pitcher, away_probable_pitcher, home_pitcher_note, away_pitcher_note, away_score, home_score, current_inning, inning_state, venue_id, venue_name, winning_team, losing_team, winning_pitcher, losing_pitcher, save_pitcher, summary) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

            vals = (
                game_id, game_datetime, game_date, game_type, status, away_name, home_name, away_id, home_id,
                doubleheader,
                game_num, home_probable_pitcher, away_probable_pitcher, home_pitcher_note, away_pitcher_note,
                away_score,
                home_score, current_inning, inning_state, venue_id, venue_name, winning_team, losing_team,
                winning_pitcher,
                losing_pitcher, save_pitcher, summary)
            val_list.append(vals)
        try:
            dml_instance.execute_insert_many_sql(sql, val_list)
        except pymysql.err.IntegrityError as e:
            print(e)


def insert_into_pitchers(dml_instance, _player_uid, _season):
    # 왼속 먼저하고 오른속 그 다음에 하기
    sql = f"select event, is_hit, at_bat, pa, count,rbi, name, team_id, team_name  from {EVENT_PITCHERS_TABLE} where player_uid = {_player_uid} and season = '{_season}' and opponent_hand='L'"

    new_events: Tuple = dml_instance.execute_fetch_sql(sql, [])
    _name = ""
    _team_id = 0
    _team_name = ""
    # event: [is_hit, at_bat, pa]
    # 공통
    # count player_uid, season
    case_dict = {"name": "", "team_id": 0, "team_name": "", "left_count_num": 0, "right_count_num": 0,
                 "left_hit_num": 0, "right_hit_num": 0,
                 "left_twob_hit_num": 0, "right_twob_hit_num": 0, "left_threeb_hit_num": 0,
                 "right_threeb_hit_num": 0,
                 "left_hr_num": 0, "right_hr_num": 0, "left_pa_num": 0, "right_pa_num": 0, "left_er": 0,
                 "right_er": 0, "left_not_my_er": 0, "right_not_my_er": 0, "left_game_num": 0, "right_game_num": 0,
                 "left_bb_num": 0, "right_bb_num": 0, "left_ao_num": 0, "right_ao_num": 0, "left_dp_num": 0,
                 "right_dp_num": 0, "left_ibb_num": 0, "right_ibb_num": 0, "left_count_num": 0,
                 "right_count_num": 0,
                 "win_num": 0, "lose_num": 0, "save_num": 0, "hold_num": 0, "left_out_num": 0, "right_out_num": 0,
                 "pickoff_num": 0, "pickoff_catch_num": 0, "left_go_num": 0, "right_go_num": 0, "left_k_num": 0,
                 "right_k_num": 0, "get_stolen_num": 0, "left_wild_pitch_num": 0, "right_wild_pitch_num": 0,
                 "balk_num": 0, "left_ball_num": 0, "right_ball_num": 0, "left_strike_num": 0, "right_strike_num": 0,
                 "left_rbi": 0, "right_rbi": 0}

    for new_event in new_events:
        # case_dict["left_count_num"] += new_event[4]
        _event = new_event[0]
        case_dict["name"] = new_event[6]
        case_dict["team_id"] = new_event[7]
        case_dict["team_name"] = new_event[8]
        case_dict["left_rbi"] += new_event[5]
        for element in left_event_pitchers_to_pitchers_convert_dict[_event]:
            case_dict[element] += new_event[4]

    # 오른손 상대
    sql = f"select event, is_hit, at_bat, pa, count,rbi, name, team_id, team_name from {EVENT_PITCHERS_TABLE} where player_uid = {_player_uid} and season = '{_season}' and opponent_hand='R'"
    new_events: Tuple = dml_instance.execute_fetch_sql(sql, [])
    try:
        for new_event in new_events:
            # case_dict["right_count_num"] += new_event[4]
            _event = new_event[0]
            case_dict["right_rbi"] += new_event[5]
            for element in right_event_pitchers_to_pitchers_convert_dict[_event]:
                case_dict[element] += new_event[4]
        sql = f"insert into {PITCHERS_TABLE}(season, player_uid, name,team_uid, team_name,left_hit_num,right_hit_num,left_twob_hit_num,right_twob_hit_num,left_threeb_hit_num,right_threeb_hit_num,left_hr_num, right_hr_num, left_pa_num, right_pa_num, left_er, right_er, left_not_my_er, right_not_my_er, left_game_num, right_game_num, left_bb_num, right_bb_num, left_ao_num, right_ao_num, left_dp_num, right_dp_num, left_ibb_num, right_ibb_num, left_count_num,right_count_num,win_num,lose_num,save_num,hold_num, left_out_num, right_out_num, pickoff_num, pickoff_catch_num, left_go_num,right_go_num, left_k_num, right_k_num, get_stolen_num,left_wild_pitch_num,right_wild_pitch_num,balk_num,left_ball_num,right_ball_num, left_strike_num, right_strike_num, left_rbi, right_rbi) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        vars = (int(_season), _player_uid, case_dict["name"], case_dict["team_id"], case_dict["team_name"],
                case_dict["left_hit_num"], case_dict["right_hit_num"], case_dict["left_twob_hit_num"],
                case_dict["right_twob_hit_num"], case_dict["left_threeb_hit_num"],
                case_dict["right_threeb_hit_num"], case_dict["left_hr_num"], case_dict["right_hr_num"],
                case_dict["left_pa_num"], case_dict["right_pa_num"], case_dict["left_er"], case_dict["right_er"],
                case_dict["left_not_my_er"],
                case_dict["right_not_my_er"], case_dict["left_game_num"], case_dict["right_game_num"],
                case_dict["left_bb_num"],
                case_dict["right_bb_num"], case_dict["left_ao_num"], case_dict["right_ao_num"],
                case_dict["left_dp_num"],
                case_dict["right_dp_num"], case_dict["left_ibb_num"], case_dict["right_ibb_num"],
                case_dict["left_count_num"], case_dict["right_count_num"], case_dict["win_num"], case_dict["lose_num"],
                case_dict["save_num"],
                case_dict["hold_num"],
                case_dict["left_out_num"], case_dict["right_out_num"], case_dict["pickoff_num"],
                case_dict["pickoff_catch_num"],
                case_dict["left_go_num"], case_dict["right_go_num"], case_dict["left_k_num"], case_dict["right_k_num"],
                case_dict["get_stolen_num"], case_dict["left_wild_pitch_num"], case_dict["right_wild_pitch_num"],
                case_dict["balk_num"], case_dict["left_ball_num"], case_dict["right_ball_num"],
                case_dict["left_strike_num"], case_dict["right_strike_num"], case_dict["left_rbi"],
                case_dict["right_rbi"])
        dml_instance.execute_insert_sql(sql, vars)
    except Exception as e:

        Logger.save_error_log_to_file(f"{e}", log_file_name)
        print(f"{e} except")


def stack_raw_data(dml_instance):
    # schdeuls 로 부터 데이터 쌓기
    sql = f"select distinct(game_id) from {SCHEDULES_TABLE} where game_id not in (select distinct(game_uid) from {GAME_RAWDATAS_TABLE}) and game_date like '%2022%'"
    print(sql)
    dml_instance.execute(sql)
    results = dml_instance.fetch_all()
    try:
        for result in tqdm(results):
            game_uid = result[0]
            url = 'https://statsapi.mlb.com/api/v1.1/game/' + str(game_uid) + '/feed/live'
            response = requests.get(url)
            contents = response.text
            game_raw_data = json.dumps(json.loads(contents))
            sql = f"insert into {GAME_RAWDATAS_TABLE}(game_uid, game_raw_data, creater) values(%s, %s, %s)"
            vars = (game_uid, game_raw_data, "kdw")
            dml_instance.execute_insert_sql(sql, vars)
    except Exception as e:
        print(e)


def stack_event_table_from_raw_data(dml_instance):
    raw_data: Tuple = None
    condition = f" game_uid not in (select distinct(game_uid) from {EVENTS_TABLE})"
    game_uids: Tuple = dml_instance.get_select_from_where(column_names=["game_uid"], table_name=GAME_RAWDATAS_TABLE,
                                                          condition=condition, print_sql=True)
    num_sql = f"SELECT count(game_uid) from {GAME_RAWDATAS_TABLE}"
    for count, game_uid in tqdm(enumerate(game_uids)):
        try:
            condition = f"game_uid={game_uid[0]}"
            raw_data: Tuple = dml_instance.get_select_from_where(column_names=["game_raw_data"],
                                                                 table_name=f"{GAME_RAWDATAS_TABLE}",
                                                                 condition=condition)
            raw_data: dict = json.loads(raw_data[0][0])
        except:
            print("condition: " + condition)
        try:
            all_plays_len = len(raw_data['liveData']['plays']['allPlays'])
            date = raw_data['gameData']['datetime']['officialDate']
            game_uid = raw_data['gameData']['game']['pk']
            season = raw_data['gameData']['game']['season']
            try:
                weather = raw_data['gameData']['weather']['condition']
            except:
                weather = "Unknown"
            batter_val_list: List = []
            pitcher_val_list: List = []
            all_plays = raw_data['liveData']['plays']['allPlays']
            for index in range(all_plays_len):
                batter_id = all_plays[index]['matchup']['batter']['id']
                batter_name = all_plays[index]['matchup']['batter']['fullName']
                pitcher_id = all_plays[index]['matchup']['pitcher']['id']
                pitcher_name = all_plays[index]['matchup']['pitcher']['fullName']
                event_index = index
                try:
                    event_type = all_plays[index]['result']['eventType']
                except:
                    event_type = "etc"

                try:
                    event = all_plays[index]['result']['event']
                except:
                    event = "etc"
                player_main_position = ""
                try:
                    batter_hand = all_plays[index]['matchup']['batSide']['code']
                except:
                    batter_hand = "etc"
                try:
                    pitcher_hand = all_plays[index]['matchup']['pitchHand']['code']
                except:
                    pitcher_hand = "etc"
                try:
                    rbi = all_plays[index]['result']['rbi']
                except:
                    #
                    all_plays[index]['result'].keys()
                    print(all_plays[index]['result'].keys())
                    rbi = 0

                strikes = all_plays[index]['count']['strikes']
                balls = all_plays[index]['count']['balls']
                outs = all_plays[index]['count']['outs']
                inning = all_plays[index]['about']['inning']
                is_top_inning = all_plays[index]['about']['isTopInning']
                if is_top_inning:
                    batter_team_id = raw_data['gameData']['teams']['away']['id']
                    batter_team_name = raw_data['gameData']['teams']['away']['name']
                    pitcher_team_id = raw_data['gameData']['teams']['home']['id']
                    pitcher_team_name = raw_data['gameData']['teams']['home']['name']
                else:
                    batter_team_id = raw_data['gameData']['teams']['home']['id']
                    batter_team_name = raw_data['gameData']['teams']['home']['name']
                    pitcher_team_id = raw_data['gameData']['teams']['away']['id']
                    pitcher_team_name = raw_data['gameData']['teams']['away']['name']

                batter_val_list.append(
                    (batter_name, batter_team_id, batter_team_name, 'batters', batter_id, date, game_uid, season,
                     weather, pitcher_id, event_index, event,
                     event_type, player_main_position, pitcher_hand, rbi, strikes, balls, outs, inning,
                     is_top_inning))

                pitcher_val_list.append(
                    (pitcher_name, pitcher_team_id, pitcher_team_name, 'pitchers', pitcher_id, date, game_uid, season,
                     weather, batter_id, event_index, event,
                     event_type, player_main_position, batter_hand, rbi, strikes, balls, outs, inning,
                     is_top_inning))
                batter_player_type = "batters"
                pitcher_player_type = "pitchers"
                # 개별
                # batter_sql2 = f"insert into events(player_type, player_uid, date, game_uid, season, weather, opponent_uid, event_index, event_type, player_main_position, opponent_hand, rbi, strikes, balls, outs, inning, is_top_inning) select %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s from dual where not exists ( select * from events where player_type ='{batter_player_type}' and game_uid='{game_uid}' and season='{season}' and event_index='{event_index}' and event_type='{event_type}')"
                # pitcher_sql2 = f"insert into events(player_type, player_uid, date, game_uid, season, weather, opponent_uid, event_index, event_type, player_main_position, opponent_hand, rbi, strikes, balls, outs, inning, is_top_inning) select %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s from dual where not exists ( select * from events where player_type ='{pitcher_player_type}' and game_uid='{game_uid}' and season='{season}' and event_index='{event_index}' and event_type='{event_type}')"
            batter_sql = f"insert into {EVENTS_TABLE}(name, team_id, team_name, player_type, player_uid, date, game_uid, season, weather, opponent_uid, event_index, event, event_type, player_main_position, opponent_hand, rbi, strikes, balls, outs, inning, is_top_inning) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            pitcher_sql = f"insert into {EVENTS_TABLE}(name, team_id, team_name, player_type, player_uid, date, game_uid, season, weather, opponent_uid, event_index, event, event_type, player_main_position, opponent_hand, rbi, strikes, balls, outs, inning, is_top_inning) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            dml_instance.execute_insert_many_sql(batter_sql, batter_val_list)
            dml_instance.execute_insert_many_sql(pitcher_sql, pitcher_val_list)
        except Exception as e:
            print("error is " + str(e))
            print("game_uid: " + str(game_uid))


def stack_event_players_from_events(dml_instance):
    s = f'''insert into {EVENT_PITCHERS_TABLE}(player_uid, season, opponent_hand, event, count, strikes, balls, outs, rbi, name, team_id, team_name)
            select player_uid,season, opponent_hand,event, count(uid), sum(strikes), sum(balls), sum(outs), sum(rbi), name, team_id, team_name
            from {EVENTS_TABLE}
            group by player_uid, player_type,season, opponent_hand,name,team_id,team_name, event
            having player_type="pitchers"
            '''
    dml_instance.execute_sql(s)


def update_event_pitchers_execute(dml_instance, table_name, where_condition: List, set_value: str):
    where = ""
    for i, event in enumerate(where_condition):
        if i != 0:
            where += f" or event='{event}'"
        else:
            where += f" where event='{event}'"
    sql = f"update {table_name} set {set_value} = 1"
    sql += where
    # sql = f"update event_pitchers set {set_value} = 0" # reset
    dml_instance.execute_sql(sql)


def update_event_pitchers(dml_instance):
    is_hit_list = []
    at_bat_list = []
    pa_list = []
    for key, value in new_event_to_pitchers_dict.items():
        if value[0] == 1:
            is_hit_list.append(key)
        if value[1] == 1:
            at_bat_list.append(key)
        if value[2] == 1:
            pa_list.append(key)
    table_name = EVENT_PITCHERS_TABLE
    update_event_pitchers_execute(dml_instance, table_name, is_hit_list, "is_hit")
    update_event_pitchers_execute(dml_instance, table_name, at_bat_list, "at_bat")
    update_event_pitchers_execute(dml_instance, table_name, pa_list, "pa")


def stack_pitchers_from_event_pitchers(dml_instance: DML):
    sql = f"select distinct(player_uid) from {EVENT_PITCHERS_TABLE}"
    print(time.time())
    player_uids: Tuple = dml_instance.execute_fetch_sql(sql, [])
    print(player_uids)
    for _, player_uid in enumerate(tqdm(player_uids)):
        sql = f"select distinct(season) from {EVENT_PITCHERS_TABLE} where player_uid = {player_uid[0]}"
        seasons: Tuple = dml_instance.execute_fetch_sql(sql, [])
        for season in seasons:
            insert_into_pitchers(dml_instance, player_uid[0], season[0])


def remove_season_pitchers(dml_instance, year):
    delete_sql = f"delete from {PITCHERS_TABLE} where season ={YEAR}"
    dml_instance.execute(delete_sql)
    dml_instance.commit()


def update_pitcher_position(dml_instance):
    '''
    https://github.com/toddrob99/MLB-StatsAPI/wiki
    standing data
    '''
    sql = f"update {PITCHERS_TABLE} set position = 'P' where primary_position_abbreviation is null"
    dml_instance.execute(sql)
    dml_instance.commit()


def insert_into_event_batters_count(dml_instance):
    sql = f'''
        insert into {EVENT_BATTER_COUNTS_TABLE}( player_uid, season, team, opponent_hand, rbi, strike, ball, count)
        select player_uid, season, team_name, opponent_hand, sum(rbi), sum(strikes), sum(balls), count(*)
        from {EVENTS_TABLE}
        where player_type="batters"
        group by season, player_uid, opponent_hand,team_name;
        '''
    dml_instance.execute(sql)
    dml_instance.commit()

if __name__ == "__main__":
    dml_instance = DML()
    ddl_instance = DDL()

    # 맨 처음 테이블 생성

    # 새로 쌓는 db TRUNCATE TABLE
    # ddl_instance.truncate_table(table_name=SCHEDULES_TABLE)
    # ddl_instance.truncate_table(table_name=EVENT_PITCHERS_TABLE)
    # ddl_instance.truncate_table(table_name=EVENT_BATTERS_TABLE)
    YEAR = 2022

    # 0. 시즌 정보 테이블 삭제

    # 1. schedule 맨 처음 비워줌
    ddl_instance.truncate_table(table_name=SCHEDULES_TABLE)

    # 2. 새 schedules 쌓아주는 로직
    stack_schedules(dml_instance, YEAR)
    print("================1. stack_schedules clear")

    # 3. schedule 로부터 game_raw_data 데이터 쌓기
    # truncate 할 필요 없음
    stack_raw_data(dml_instance)
    print("================2. stack_raw_data clear")

    # 한 번 비워줘야 함
    ddl_instance.truncate_table(table_name=EVENTS_TABLE)

    # 4. raw_data 에서 events 데이터 쌓기
    stack_event_table_from_raw_data(dml_instance)
    print("================3. stack_event_table_from_raw_data clear")

    # 5. event_pitchers table 만드는 sql
    ddl_instance.truncate_table(table_name=EVENT_PITCHERS_TABLE)
    stack_event_players_from_events(dml_instance)
    print("================4. stack_event_players_from_events clear")

    # 6. event_pitchers 를 update 하기 위한 로직
    update_event_pitchers(dml_instance)
    print("================5. update_event_pitchers clear")

    # 7. 이번년도 시즌의 투수 정보 삭제
    remove_season_pitchers(dml_instance, YEAR)

    # 8. pitchers 테이블 만드는 로직
    stack_pitchers_from_event_pitchers(dml_instance)
    update_pitcher_position(dml_instance)

    # event_batters_count 도 비워주고 다시 생성
    ddl_instance.truncate_table(table_name=EVENT_BATTER_COUNTS_TABLE)
    insert_into_event_batters_count(dml_instance)

    # 9. 이번년도 시즌의 타자 정보 삭제
    remove_season_batters(dml_instance, YEAR)

    # 10. batters 테이블 만드는 로직
    stack_batters(dml_instance)
    print("================6. stack_pitchers_from_event_pitchers")
    dml_instance.close()
