from DB.DML import DML
from typing import List
# 작성일 2022.09.30


from info.process_info import new_event_to_pitchers_dict, pitcher_primary_position_abbreviation



if __name__ == "__main__":

    dml_instance = DML()
    # dml_instance.execute_sql()
    s ='''insert into tmp_event_pitchers(player_uid, season, opponent_hand, event, count, strikes, balls, outs, rbi, name, team_id, team_name)
    select player_uid,season, opponent_hand,event, count(uid), sum(strikes), sum(balls), sum(outs), sum(rbi), name, team_id, team_name
    from new_new_events
    group by player_uid, player_type,season, opponent_hand,name,team_id,team_name, event
    having player_type="pitchers"
    '''
    print(s)
    dml_instance.execute_sql(s)