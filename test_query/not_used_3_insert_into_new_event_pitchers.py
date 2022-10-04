from DB.DML import DML
from typing import Tuple
from tqdm import tqdm

import time
from info.process_info import new_event_to_pitchers_dict, pitcher_primary_position_abbreviation

# [20220930]시간 엄청걸림

# raw_data는 튜플형태


def insert_into_event_pitcher(dml_instance, _player_uid, _season, _opponent_hand):
    _count = 0
    _is_hit = 0
    _at_bat = 0
    _pa = 0
    _event = ""
    sql = f"select distinct(event),count from new_event_player where player_uid = {_player_uid} and season = '{_season}' and opponent_hand='{_opponent_hand}'"
    new_events: Tuple = dml_instance.execute_fetch_sql(sql, [])
    # event: [is_hit, at_bat, pa]

    vars = []
    for new_event in new_events:
        _event = new_event[0]
        _is_hit = new_event_to_pitchers_dict[_event][0]
        _at_Bat = new_event_to_pitchers_dict[_event][1]
        _pa = new_event_to_pitchers_dict[_event][2]
        _count = new_event[1]
        vars.append((_player_uid, _season, _opponent_hand, _count, _is_hit, _at_bat, _pa, _event))

    sql = "insert into event_pitchers(player_uid,season,opponent_hand,count,is_hit,at_bat,pa,event) values(%s, %s, %s, %s, %s, %s, %s, %s)";
    dml_instance.execute_insert_many_sql(sql, vars)


if __name__ == "__main__":

    dml_instance = DML()
    # event_pitcher: Tuple = dml_instance.get_select_from_where(['*'], "new_events")
    sql = f"select distinct(uid) from baseball_players where primary_position_abbreviation in {pitcher_primary_position_abbreviation}"
    print(time.time())
    player_uids: Tuple = dml_instance.execute_fetch_sql(sql, [])
    print(player_uids)
    for player_uid in tqdm(player_uids):
        sql = f"select distinct(season) from new_event_player where player_uid = {player_uid[0]}"
        seasons: Tuple = dml_instance.execute_fetch_sql(sql, [])
        for season in seasons:
            for opponent_hand in ["L", "R"]:
                insert_into_event_pitcher(dml_instance, player_uid[0], season[0], opponent_hand)
