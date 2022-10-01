from DB.DML import DML
from typing import Tuple, Dict, List
import json
from tqdm import tqdm
import time, datetime
from datetime import datetime
from pprint import pprint

# raw_data는 튜플형태
'''
- player_type(batter or pitcher): 이건 맨처음 insert into할 때 투수하고 타자 나눠서 저장해야 함
- player_uid(플레이어 uid): player_uid
- date(날짜 정보): gameData.datetime.officalDate
- game_uid(게임 uid): gameData.game.pk
- season(시즌): gameData.game.season
- weather(날씨정보): gameData.weather.condition
- opponent_uid(상대 플레이어 uid): liveData.plays.allPlays.[index].matchup.pitcher.id, liveData.plays.allPlays.[index].matchup.batter.id
- event_index(이벤트 인덱스): liveData.plays.allPlays.[index] 여기서의 index값
- event(이벤트 타입의 하위 개념): liveData.plays.allPlays.[index].result.event
- event_type(이벤트 타입): liveData.plays.allPlays.[index].result.eventType
- player_main플레이어 주 포지션(이건 baseball_players 에 있어서 나중에 join 때려야 할 듯): liveData.plays.allPlays.[index]
- hand(상대 선수의 주손): liveData.plays.allPlays.[index].matchup.batSide.code and liveData.plays.allPlays.[index].matchup.pitchHand.code
- rbi(점수낸 정보): liveData.plays.allPlays.[index].result.rbi
- strikes(현재 스트라이크 카운트): liveData.plays.allPlays.[index].count.strikes
- balls(현재 볼 카운트): liveData.plays.allPlays.[index].count.balls
- outs(현재 아웃카운트): liveData.plays.allPlays.[index].count.outs
- inning(현재 이닝): liveData.plays.allPlays.[index].about.inning
- is_top_inning(현재 초공격인지 말공격인지): liveData.plays.allPlays.[index].about.isTopInning
- at타수 정보인지(at bat): liveData.plays.allPlays.[index].result.type (모든 경우가 atBat 이라 하지 않기)
'''
if __name__ == "__main__":
    dml_instance = DML()
    raw_data: Tuple = None
    condition = f" game_uid not in (select distinct(game_uid) from new_new_events)"
    game_uids: Tuple = dml_instance.get_select_from_where(column_names=["game_uid"], table_name="game_raw_datas",
                                                          condition=condition, print_sql=True)
    # 1초에 10 iter

    print(len(game_uids))
    num_sql = "SELECT count(game_uid) from game_raw_datas"
    all_count = dml_instance.execute_fetch_sql(num_sql, [])
    print(all_count[0][0])
    all_count = all_count[0][0]
    # 50892 60536
    for count, game_uid in tqdm(enumerate(reversed(game_uids))):
        # if count<all_count-len(game_uids) - 50890:
        #     continue
        # if count<50890:
        #     continue
        condition = f"game_uid={game_uid[0]}"
        raw_data: Tuple = dml_instance.get_select_from_where(column_names=["game_raw_data"],
                                                             table_name="game_raw_datas",
                                                             condition=condition)
        raw_data: dict = json.loads(raw_data[0][0])
        try:
            all_plays_len = len(raw_data['liveData']['plays']['allPlays'])
            date = raw_data['gameData']['datetime']['officialDate']
            game_uid = raw_data['gameData']['game']['pk']
            season = raw_data['gameData']['game']['season']
            try:
                weather = raw_data['gameData']['weather']['condition']
            except:
                weather = "Unknown"
            batter_sql = "insert into new_new_events(name, team_id, team_name, player_type, player_uid, date, game_uid, season, weather, opponent_uid, event_index, event, event_type, player_main_position, opponent_hand, rbi, strikes, balls, outs, inning, is_top_inning) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            pitcher_sql = "insert into new_new_events(name, team_id, team_name, player_type, player_uid, date, game_uid, season, weather, opponent_uid, event_index, event, event_type, player_main_position, opponent_hand, rbi, strikes, balls, outs, inning, is_top_inning) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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

                batter_val_list.append((batter_team_id, batter_team_name, 'batters', batter_id, date, game_uid, season,
                                        weather, pitcher_id, event_index, event,
                                        event_type, player_main_position, pitcher_hand, rbi, strikes, balls, outs, inning,
                                        is_top_inning))

                pitcher_val_list.append((pitcher_team_id, pitcher_team_name, 'pitchers', pitcher_id, date, game_uid, season,
                                         weather, batter_id, event_index, event,
                                         event_type, player_main_position, batter_hand, rbi, strikes, balls, outs, inning,
                                         is_top_inning))
                batter_player_type = "batters"
                pitcher_player_type = "pitchers"
                # 개별
                # batter_sql2 = f"insert into events(player_type, player_uid, date, game_uid, season, weather, opponent_uid, event_index, event_type, player_main_position, opponent_hand, rbi, strikes, balls, outs, inning, is_top_inning) select %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s from dual where not exists ( select * from events where player_type ='{batter_player_type}' and game_uid='{game_uid}' and season='{season}' and event_index='{event_index}' and event_type='{event_type}')"
                # pitcher_sql2 = f"insert into events(player_type, player_uid, date, game_uid, season, weather, opponent_uid, event_index, event_type, player_main_position, opponent_hand, rbi, strikes, balls, outs, inning, is_top_inning) select %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s from dual where not exists ( select * from events where player_type ='{pitcher_player_type}' and game_uid='{game_uid}' and season='{season}' and event_index='{event_index}' and event_type='{event_type}')"
            dml_instance.execute_insert_many_sql(batter_sql, batter_val_list)
            dml_instance.execute_insert_many_sql(pitcher_sql, pitcher_val_list)
        except Exception as e:
            print("error is "+str(e))
            print("game_uid: " + str(game_uid))
