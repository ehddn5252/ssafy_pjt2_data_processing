from DB.DML import DML
from typing import List
# 작성일 2022.09.30
from Logger.logger import Logger

from data.info_data import left_event_pitchers_to_pitchers_convert_dict, right_event_pitchers_to_pitchers_convert_dict
import time
from typing import Tuple
from tqdm import tqdm


log_file_name = "4_insert_into_pitcher_log.txt"

def insert_into_pitchers(dml_instance, _player_uid, _season):
    # 왼속 먼저하고 오른속 그 다음에 하기
    sql = f"select event, is_hit, at_bat, pa, count  from event_pitchers where player_uid = {_player_uid} and season = '{_season}' and opponent_hand='L'"
    new_events: Tuple = dml_instance.execute_fetch_sql(sql, [])
    # event: [is_hit, at_bat, pa]
    # 공통
    # count player_uid, season
    case_dict = {"left_count_num": 0, "right_count_num": 0, "left_hit_num": 0, "right_hit_num": 0,
                 "left_twob_hit_num": 0, "right_twob_hit_num": 0, "left_threeb_hit_num": 0,
                 "right_threeb_hit_num": 0,
                 "left_hr_num": 0, "right_hr_num": 0, "left_pa_num": 0, "right_pa_num": 0, "left_er": 0,
                 "right_er": 0, "left_not_my_er": 0, "right_not_my_er": 0, "left_game_num": 0, "right_game_num": 0,
                 "left_bb_num": 0, "right_bb_num": 0, "left_ao_num": 0, "right_ao_num": 0, "left_dp_num": 0,
                 "right_dp_num": 0, "left_ibb_num": 0, "right_ibb_num": 0, "left_count_num": 0,
                 "right_count_num": 0,
                 "win_num": 0, "lose_num": 0, "save_num": 0, "hold_num": 0, "left_out_num": 0, "right_out_num": 0,
                 "pickoff_num": 0, "pickoff_catch_num": 0, "left_go_num": 0, "right_go_num": 0, "left_k_num": 0,
                 "right_k_num": 0, "get_stolen_num": 0, "left_wild_pitch_num": 0, "right_wild_pitch_num": 0,"balk_num":0}

    for new_event in new_events:
        case_dict["left_count_num"] += new_event[4]
        _event = new_event[0]
        for element in left_event_pitchers_to_pitchers_convert_dict[_event]:
            case_dict[element] += 1
    # 오른손 상대
    sql = f"select event, is_hit, at_bat, pa, count  from event_pitchers where player_uid = {_player_uid} and season = '{_season}' and opponent_hand='R'"
    new_events: Tuple = dml_instance.execute_fetch_sql(sql, [])
    try:
        for new_event in new_events:
            case_dict["right_count_num"] += new_event[4]
            _event = new_event[0]
            for element in right_event_pitchers_to_pitchers_convert_dict[_event]:
                case_dict[element] += 1
        sql = "insert into pitchers(team_uid,player_uid,season,left_hit_num,right_hit_num,left_twob_hit_num,right_twob_hit_num,left_threeb_hit_num,right_threeb_hit_num,left_hr_num, right_hr_num, left_pa_num, right_pa_num, left_er, right_er, left_not_my_er, right_not_my_er, left_game_num, right_game_num, left_bb_num, right_bb_num, left_ao_num, right_ao_num, left_dp_num, right_dp_num, left_ibb_num, right_ibb_num, left_count_num,right_count_num,win_num,lose_num,save_num,hold_num, left_out_num, right_out_num, pickoff_num, pickoff_catch_num, left_go_num,right_go_num, left_k_num, right_k_num, get_stolen_num,left_wild_pitch_num,right_wild_pitch_num,balk_num) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        vars = (100,
                _player_uid, int(_season), case_dict["left_hit_num"], case_dict["right_hit_num"],
                case_dict["left_twob_hit_num"],
                case_dict["right_twob_hit_num"],
                case_dict["left_threeb_hit_num"],
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
                case_dict["balk_num"])
        dml_instance.execute_insert_sql(sql, vars)
    except Exception as e:

        Logger.save_error_log_to_file(f"{e}", log_file_name)
        print(f"{e} except")


if __name__ == "__main__":
    dml_instance = DML()
    sql = f"select distinct(player_uid) from event_pitchers"
    print(time.time())
    player_uids: Tuple = dml_instance.execute_fetch_sql(sql, [])
    print(player_uids)
    for _,player_uid in enumerate(tqdm(player_uids)):
        sql = f"select distinct(season) from event_pitchers where player_uid = {player_uid[0]}"
        seasons: Tuple = dml_instance.execute_fetch_sql(sql, [])
        for season in seasons:
            insert_into_pitchers(dml_instance, player_uid[0], season[0])
